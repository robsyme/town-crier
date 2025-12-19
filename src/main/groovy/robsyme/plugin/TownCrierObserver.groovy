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
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord

/**
 * Observer that sends HTTP notifications when files are published.
 *
 * Supports filtering by process name using Nextflow-style selectors
 * (e.g., 'ALIGNMENT', '.*BAM.*', '!REPORT').
 */
@Slf4j
@CompileStatic
class TownCrierObserver implements TraceObserver {

    private Session session
    private String endpoint
    private List<String> processSelectors

    /** Map from task hash to process name for resolving published files */
    private Map<String, String> taskHashToProcessName = new ConcurrentHashMap<>()

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
     * Track task submissions to build a mapping from task hash to process name.
     * This must happen early (before onFilePublish) because file publishing
     * can occur before onProcessComplete is called.
     */
    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        def task = handler.task
        def hash = task.hash.toString()
        def processName = task.processor.name
        taskHashToProcessName.put(hash, processName)
        log.trace "TownCrier: Task submitted - hash=$hash, process=$processName"
    }

    /**
     * Called when a file is published via publishDir directive.
     * Sends an HTTP notification if the source process matches the configured selectors.
     */
    @Override
    void onFilePublish(Path destination, Path source) {
        if (!shouldNotify(source)) {
            log.trace "TownCrier: Skipping notification for ${destination} (no matching selector)"
            return
        }

        log.debug "TownCrier: File published - ${destination}"
        final destPath = destination
        final srcPath = source
        executor.submit { sendFilePublishNotification(destPath, srcPath) }
    }

    /**
     * Cleanup when workflow completes - wait for pending notifications to finish.
     */
    @Override
    void onFlowComplete() {
        log.debug "TownCrier: Workflow complete, waiting for pending notifications..."
        executor.shutdown()
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
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
     * Resolve the process name from a source file path by extracting the task hash.
     */
    private String getProcessName(Path source) {
        if (!source) return null
        def hash = extractTaskHash(source, session.workDir)
        return hash ? taskHashToProcessName.get(hash) : null
    }

    /**
     * Extract the task hash from a source path within the work directory.
     * Work directory structure: work/XX/YYYYYYYY.../file
     * where XXYYYYYYYY... is the hash.
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

        // Validate it looks like a hash (32 hex chars)
        if (fullHash.length() != 32 || !fullHash.matches('[0-9a-f]+')) {
            return null
        }

        return fullHash
    }

    /**
     * Check if a file publish event should trigger a notification based on process selectors.
     * Uses the same matching logic as Nextflow's withName: selectors.
     */
    private boolean shouldNotify(Path source) {
        def processName = getProcessName(source)
        if (!processName) {
            log.trace "TownCrier: Could not resolve process name for source path"
            return false
        }
        return processSelectors.any { selector ->
            matchesSelector(processName, selector)
        }
    }

    /**
     * Check if a process name matches a selector pattern.
     * Supports regex patterns and negation with '!' prefix.
     * This mirrors Nextflow's ProcessConfigBuilder.matchesSelector() logic.
     */
    private static boolean matchesSelector(String name, String pattern) {
        def isNegated = pattern.startsWith('!')
        if (isNegated) {
            pattern = pattern.substring(1).trim()
        }
        return Pattern.compile(pattern).matcher(name).matches() ^ isNegated
    }

    /**
     * Send HTTP notification for a file publish event.
     */
    private void sendFilePublishNotification(Path destination, Path source) {
        def processName = getProcessName(source)
        def payload = [
            event: 'file_published',
            timestamp: OffsetDateTime.now().toString(),
            process: processName,
            source: source?.toUri()?.toString(),
            target: destination.toUri().toString(),
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
            conn.connectTimeout = 5000
            conn.readTimeout = 10000

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
