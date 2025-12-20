# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Town Crier is a demonstration Nextflow plugin that sends HTTP notifications when files are published via `publishDir`. It showcases the TraceObserverV2 API introduced in Nextflow 25.04.0. This is an educational project, not production-ready.

## Key Architecture

### Plugin Structure

The plugin implements three key extension points:

1. **TownCrierPlugin** (src/main/groovy/robsyme/plugin/TownCrierPlugin.groovy) - Main plugin entry point extending `BasePlugin`

2. **TownCrierFactory** (src/main/groovy/robsyme/plugin/TownCrierFactory.groovy) - Implements `TraceObserverFactoryV2` to create observer instances. Reads configuration from `session.config.towncrier` and validates that an endpoint is configured before activating.

3. **TownCrierObserver** (src/main/groovy/robsyme/plugin/TownCrierObserver.groovy) - Implements `TraceObserverV2` to observe workflow events. This is where the core logic lives.

### Observer Event Flow

The observer uses a **queued notification pattern** to ensure output metadata is available:

1. **onTaskSubmit** - Registers task information (process name, hash)
2. **onFilePublish** - Queues publish events with timestamp but doesn't send yet
3. **onTaskComplete** - Extracts output metadata from task outputs, then flushes queued notifications
4. **onTaskCached** - Handles cached tasks which skip submission
5. **onFlowComplete** - Waits for pending HTTP requests to complete (60s timeout)

This queuing is critical because `FilePublishEvent` occurs before task completion, but we need the task's output values (metadata) to include in notifications.

### Key Implementation Details

- **Task Hash Extraction**: The observer extracts the 32-character task hash from work directory paths (format: `workDir/{bucket}/{hash}/...` where bucket is first 2 chars)
- **Process Filtering**: Supports Nextflow-style selectors ('ALIGNMENT', '.*BAM.*', '!REPORT', 'PROCESS1|PROCESS2')
- **Metadata Extraction**: Only captures value types (Map, String, Number, Boolean) from task outputs, excludes file parameters
- **Thread Safety**: Uses `ConcurrentHashMap` and synchronized blocks to coordinate between event callbacks
- **HTTP Notifications**: Uses a single-threaded executor to send notifications asynchronously

### Configuration Schema

```groovy
towncrier {
    enabled = true                      // Optional: explicit enable/disable
    endpoint = 'https://api.example.com/notify'  // Required
    processes = 'ALIGNMENT|MODKIT'     // Optional: defaults to '.*' (all)
}
```

### Notification Payload

HTTP POST requests contain:
- `event`: Always "file_published"
- `timestamp`: ISO 8601 with offset
- `process`: Process name (e.g., "ALIGNMENT")
- `source`: Work directory file URI
- `target`: Published file URI
- `labels`: Empty array (publishDir labels support)
- `metadata`: Extracted output values from task
- `workflow`: Contains `runName` and `sessionId`

## Development Commands

Build the plugin:
```bash
make assemble
# or: ./gradlew assemble
```

Run tests:
```bash
make test
# or: ./gradlew test
```

Install to local Nextflow plugins directory (~/.nextflow/plugins/):
```bash
make install
# or: ./gradlew install
```

Clean build artifacts and Nextflow work directories:
```bash
make clean
```

## Testing

The plugin uses Spock for testing (src/test/groovy/robsyme/plugin/TownCrierObserverTest.groovy). Tests verify:
- Observer creation based on configuration
- Process selector normalization (string vs list)
- Proper disabling when endpoint missing or explicitly disabled

To run a single test:
```bash
./gradlew test --tests "TownCrierObserverTest.should create observer when endpoint is configured"
```

## Working with Nextflow Source

This repository has `/Users/robsyme/dev/bears/nextflow/v25.10.2` added as an additional working directory for reference to Nextflow internals when developing plugin features.

## Plugin Limitations

- Only tracks `publishDir` files, not the new workflow output syntax
- Metadata extraction limited to value types (excludes Path/File objects)
- Fixed 60-second timeout on workflow completion for pending notifications
- Single HTTP endpoint (no multi-destination support)
