/*
 * Copyright 2025, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package robsyme.plugin

import java.nio.file.Path
import java.time.OffsetDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import groovy.json.JsonOutput
import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskRun
import nextflow.script.params.FileOutParam
import nextflow.script.params.OutParam
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.FilePublishEvent
import nextflow.trace.event.TaskEvent

/**
 * Observer that sends HTTP notifications when files are published.
 *
 * Uses TraceObserverV2 to queue publish events until task completion,
 * allowing access to output metadata.
 *
 * Supports filtering by process name using Nextflow-style selectors
 * (e.g., 'ALIGNMENT', '.*BAM.*', '!REPORT').
 */
@Slf4j
@CompileStatic
class TownCrierObserver implements TraceObserverV2 {

    /** HTTP connection timeouts */
    private static final int HTTP_CONNECT_TIMEOUT_MS = 5000
    private static final int HTTP_READ_TIMEOUT_MS = 10000
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 60

    private Session session
    private String endpoint
    private List<String> processSelectors

    /** Cache for compiled regex patterns to avoid recompilation */
    private Map<String, Pattern> patternCache = new ConcurrentHashMap<>()

    /** Map from task hash to task info (process name + metadata) */
    private Map<String, TaskInfo> taskHashToInfo = new ConcurrentHashMap<>()

    /** Map from task hash to pending publish events (queued until task completes) */
    private Map<String, List<PendingPublish>> pendingPublishes = new ConcurrentHashMap<>()

    /** Lock for synchronizing publish queue operations */
    private final Object publishLock = new Object()

    /** Holds task information including output metadata */
    @Canonical
    private static class TaskInfo {
        String processName
        Map<String, Object> outputs = [:]
        boolean complete = false
    }

    /** Holds a pending file publish event */
    @Canonical
    private static class PendingPublish {
        Path source
        Path target
        List<String> labels
        OffsetDateTime timestamp = OffsetDateTime.now()
    }

    /** Single-threaded executor for async HTTP notifications */
    private ExecutorService executor = Executors.newSingleThreadExecutor()

    TownCrierObserver(Session session, String endpoint, List<String> processSelectors) {
        this.session = session
        this.endpoint = endpoint
        this.processSelectors = processSelectors ?: ['.*']
        log.debug "TownCrier initialized with endpoint=$endpoint, selectors=$processSelectors"
    }

    @Override
    void onFlowCreate(Session session) {
        log.debug "TownCrier: Workflow starting"
    }

    @Override
    void onFlowBegin() {
        log.debug "TownCrier: Workflow DAG ignited"
    }

    /**
     * Track task submissions to register the task.
     */
    @Override
    void onTaskSubmit(TaskEvent event) {
        def task = event.handler.task
        def hash = task.hash.toString()
        def processName = task.processor.name

        def taskInfo = new TaskInfo(processName: processName)
        taskHashToInfo.put(hash, taskInfo)

        log.trace "TownCrier: Task submitted - hash=$hash, process=$processName"
    }

    /**
     * When task completes, capture output metadata and send any queued notifications.
     */
    @Override
    void onTaskComplete(TaskEvent event) {
        def task = event.handler.task
        def hash = task.hash.toString()

        def taskInfo = taskHashToInfo.get(hash)
        if (taskInfo) {
            taskInfo.outputs = extractMetadata(task.outputs, FileOutParam)
            // Mark complete and flush within synchronized block
            synchronized (publishLock) {
                taskInfo.complete = true
                log.trace "TownCrier: Task complete - hash=$hash, outputs=${taskInfo.outputs}"
                // Send any queued publish events for this task
                flushPendingPublishesLocked(hash, taskInfo)
            }
        }
    }

    /**
     * Handle cached tasks - they won't have onTaskSubmit called first.
     */
    @Override
    void onTaskCached(TaskEvent event) {
        def task = event.handler.task
        def hash = task.hash.toString()
        def processName = task.processor.name

        def taskInfo = new TaskInfo(processName: processName, outputs: extractMetadata(task.outputs, FileOutParam), complete: true)

        synchronized (publishLock) {
            taskHashToInfo.put(hash, taskInfo)
            log.trace "TownCrier: Task cached - hash=$hash, process=$processName"
            // Flush any pending publishes (unlikely for cached, but handle it)
            flushPendingPublishesLocked(hash, taskInfo)
        }
    }

    /**
     * Queue file publish events until the task completes.
     * This ensures we have access to output metadata when sending notifications.
     */
    @Override
    void onFilePublish(FilePublishEvent event) {
        def hash = extractTaskHash(event.source, session.workDir)
        if (!hash) {
            log.trace "TownCrier: Could not extract task hash from ${event.source}"
            return
        }

        def taskInfo = taskHashToInfo.get(hash)
        if (!taskInfo) {
            log.trace "TownCrier: No task info for hash=$hash"
            return
        }

        // Check process selector filter
        if (!matchesSelectors(taskInfo.processName)) {
            log.trace "TownCrier: Skipping notification for ${event.target} (process ${taskInfo.processName} not in selectors)"
            return
        }

        def pending = new PendingPublish(event.source, event.target, event.labels)

        // Synchronize to avoid race between queue and flush
        synchronized (publishLock) {
            // If task already complete, send immediately
            if (taskInfo.complete) {
                log.debug "TownCrier: File published (task complete) - ${event.target}"
                executor.submit { sendNotification(pending, taskInfo) }
            } else {
                // Queue until task completes
                log.debug "TownCrier: File published (queued) - ${event.target}"
                pendingPublishes.computeIfAbsent(hash, { new ArrayList<PendingPublish>() }).add(pending)
            }
        }
    }

    /**
     * Send all queued notifications for a completed task.
     * Must be called while holding publishLock.
     */
    private void flushPendingPublishesLocked(String hash, TaskInfo taskInfo) {
        def pending = pendingPublishes.remove(hash)
        if (!pending) return

        // Check selector filter
        if (!matchesSelectors(taskInfo.processName)) return

        for (PendingPublish p : pending) {
            log.debug "TownCrier: Sending queued notification - ${p.target}"
            final publish = p
            final info = taskInfo
            executor.submit { sendNotification(publish, info) }
        }
    }

    /**
     * Cleanup when workflow completes - wait for pending notifications to finish.
     */
    @Override
    void onFlowComplete() {
        log.debug "TownCrier: Workflow complete, waiting for pending notifications..."

        // Warn about any orphaned pending publishes
        if (!pendingPublishes.isEmpty()) {
            log.warn "TownCrier: ${pendingPublishes.size()} tasks have unpublished files (task may have failed)"
        }

        executor.shutdown()
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn "TownCrier: Timed out waiting for notifications to complete"
                executor.shutdownNow()
            }
        } catch (InterruptedException e) {
            log.warn "TownCrier: Interrupted while waiting for notifications"
            executor.shutdownNow()
            Thread.currentThread().interrupt()
        }
        log.debug "TownCrier: All notifications completed"
    }

    /**
     * Extract value metadata from task outputs.
     * Skips file parameters - only captures maps, strings, numbers, booleans.
     */
    private Map<String, Object> extractMetadata(Map<?, Object> params, Class<?> fileParamType) {
        Map<String, Object> metadata = [:]

        params.each { param, value ->
            // Skip file parameters
            if (fileParamType.isInstance(param)) return

            String paramName = (param instanceof OutParam) ? ((OutParam) param).getName() : "param"

            // For tuple parameters, value is a list
            if (value instanceof List) {
                value.eachWithIndex { item, idx ->
                    if (isSerializable(item)) {
                        String key = value.size() > 1 ? "${paramName}_${idx}" : paramName
                        metadata[key] = serializableValue(item)
                    }
                }
            } else if (isSerializable(value)) {
                metadata[paramName] = serializableValue(value)
            }
        }

        return metadata
    }

    /**
     * Check if a value can be serialized to JSON.
     */
    private static boolean isSerializable(Object value) {
        return value instanceof Map || value instanceof String ||
               value instanceof Number || value instanceof Boolean
    }

    /**
     * Convert a value to a JSON-serializable form.
     */
    private static Object serializableValue(Object value) {
        if (value == null) return null
        if (value instanceof Map) {
            return value.collectEntries { k, v -> [k.toString(), serializableValue(v)] }
        }
        if (value instanceof List) {
            return value.collect { serializableValue(it) }
        }
        // String, Number, Boolean are already serializable
        return value
    }

    /**
     * Extract task hash from a source path within the work directory.
     */
    private static String extractTaskHash(Path sourcePath, Path workDir) {
        if (!sourcePath || !workDir) return null
        if (!sourcePath.startsWith(workDir)) return null

        def relativePath = workDir.relativize(sourcePath)
        if (relativePath.nameCount < 2) return null

        def bucket = relativePath.getName(0).toString()
        if (bucket.length() != 2) return null

        def hashDir = relativePath.getName(1).toString()
        def fullHash = bucket + hashDir

        if (fullHash.length() != 32 || !fullHash.matches('[0-9a-f]+')) {
            return null
        }

        return fullHash
    }

    /**
     * Check if a process name matches the configured selectors.
     */
    private boolean matchesSelectors(String processName) {
        if (!processName) return false
        return processSelectors.any { selector ->
            matchesSelector(processName, selector)
        }
    }

    /**
     * Check if a process name matches a selector pattern.
     */
    private boolean matchesSelector(String name, String pattern) {
        def isNegated = pattern.startsWith('!')
        if (isNegated) {
            pattern = pattern.substring(1).trim()
        }

        def compiled = patternCache.computeIfAbsent(pattern, { Pattern.compile(it) })
        boolean matches = compiled.matcher(name).matches()
        return isNegated ? !matches : matches
    }

    /**
     * Send HTTP notification for a file publish event.
     */
    private void sendNotification(PendingPublish publish, TaskInfo taskInfo) {
        def payload = [
            event: 'file_published',
            timestamp: publish.timestamp.toString(),
            process: taskInfo.processName,
            source: publish.source?.toUri()?.toString(),
            target: publish.target.toUri().toString(),
            labels: publish.labels ?: [],
            metadata: taskInfo.outputs,
            workflow: [
                runName: session.runName,
                sessionId: session.uniqueId.toString()
            ]
        ]
        httpPost(JsonOutput.toJson(payload))
    }

    /**
     * Send an HTTP POST request with JSON payload.
     */
    private void httpPost(String json) {
        HttpURLConnection conn = null
        try {
            conn = (HttpURLConnection) new URL(endpoint).openConnection()
            conn.requestMethod = 'POST'
            conn.setRequestProperty('Content-Type', 'application/json')
            conn.doOutput = true
            conn.connectTimeout = HTTP_CONNECT_TIMEOUT_MS
            conn.readTimeout = HTTP_READ_TIMEOUT_MS

            conn.outputStream.withWriter('UTF-8') { writer ->
                writer.write(json)
            }

            def responseCode = conn.responseCode
            if (responseCode >= 200 && responseCode < 300) {
                log.debug "TownCrier: POST to $endpoint succeeded (HTTP $responseCode)"
            } else {
                log.warn "TownCrier: POST to $endpoint returned HTTP $responseCode"
            }
        } catch (Exception e) {
            log.warn "TownCrier: Failed to notify API at $endpoint - ${e.message}"
        } finally {
            conn?.disconnect()
        }
    }
}
