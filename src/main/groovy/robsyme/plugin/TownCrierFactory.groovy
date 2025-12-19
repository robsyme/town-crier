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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory

/**
 * Factory to create {@link TownCrierObserver} instances.
 *
 * Reads configuration from nextflow.config:
 * <pre>
 * towncrier {
 *     enabled = true
 *     endpoint = 'https://api.example.com/notify'
 *     processes = ['ALIGNMENT', 'MODKIT']  // Optional: filter by process name
 * }
 * </pre>
 */
@Slf4j
@CompileStatic
class TownCrierFactory implements TraceObserverFactory {

    @Override
    Collection<TraceObserver> create(Session session) {
        // Read configuration from session.config.towncrier
        def config = session.config.towncrier as Map ?: [:]

        // Check if explicitly disabled
        if (config.enabled == false) {
            log.debug "TownCrier: Explicitly disabled in configuration"
            return Collections.emptyList()
        }

        // Get endpoint - required for plugin to be active
        def endpoint = config.endpoint as String
        if (!endpoint) {
            log.debug "TownCrier: No endpoint configured, plugin disabled"
            return Collections.emptyList()
        }

        // Get process selectors - defaults to all processes
        def selectors = normalizeSelectors(config.processes)

        log.info "TownCrier: Enabled with endpoint=$endpoint, processes=$selectors"
        return List.<TraceObserver>of(new TownCrierObserver(session, endpoint, selectors))
    }

    /**
     * Normalize the process selectors from configuration.
     * Accepts a single string or a list of strings.
     * Defaults to ['.*'] (all processes) if not specified.
     */
    private List<String> normalizeSelectors(Object processes) {
        if (processes == null) {
            return ['.*']
        }
        if (processes instanceof String) {
            return [processes]
        }
        if (processes instanceof List) {
            return processes.collect { it.toString() }
        }
        log.warn "TownCrier: Invalid 'processes' configuration, expected String or List"
        return ['.*']
    }
}
