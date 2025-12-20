# Town Crier Plugin

> **Demo Plugin** - This is an example Nextflow plugin for educational purposes, demonstrating how to use the TraceObserver API to send HTTP notifications when files are published. Not intended for production use.

## What it does

Town Crier sends HTTP POST notifications to a configured endpoint whenever files are published via Nextflow's `publishDir` directive. It supports filtering by process name using Nextflow-style selectors.

## Configuration

```groovy
// nextflow.config
plugins {
    id 'town-crier@0.1.0'
}

towncrier {
    enabled = true
    endpoint = 'https://api.example.com/notify'
    processes = 'ALIGNMENT|MODKIT'  // Optional: filter by process name
}
```

### Process Selectors

The `processes` option uses the same selector syntax as Nextflow's `withName:`:

- `'ALIGNMENT'` - exact match
- `'.*'` - all processes (default)
- `'ALIGNMENT|MODKIT'` - multiple specific processes
- `['ALIGNMENT', 'MODKIT']` - multiple specific processes (more verbose)
- `'!REPORT'` - all except REPORT (negation)
- `'.*BAM.*'` - regex matching

## Example Payload

```json
{
    "event": "file_published",
    "timestamp": "2025-12-19T10:30:00Z",
    "process": "ALIGNMENT",
    "source": "file:///work/12/abc123.../sample.bam",
    "target": "file:///results/bam/sample.bam",
    "labels": [],
    "metadata": {
        "meta": {
            "sampleId": "sample1",
            "patient": "P001",
            "aligned": true
        }
    },
    "workflow": {
        "runName": "modest_galileo",
        "sessionId": "abc123-..."
    }
}
```

### Task Metadata

The plugin captures **output** metadata emitted by each task. This allows your API endpoint to receive sample metadata without parsing filenames.

For a process like:

```nextflow
process ALIGNMENT {
    input: val(meta)
    output: tuple val(meta), path("*.bam")
    // ...
}
```

The notification will include the important metadata stored in the `meta` object.

**Note:** Notifications are queued until task completion to ensure output metadata is available. File parameters are excluded (only value types like maps, strings, and numbers are captured).

## Installation

Since this is a demonstration plugin, it has not been published to the [Nextflow Plugin Registry](https://registry.nextflow.io/). To use it, you must clone and install locally:

```bash
git clone https://github.com/robsyme/town-crier.git
cd town-crier
make install
```

This installs the plugin to `~/.nextflow/plugins/`.

## Usage

After installing, run your pipeline with the plugin:

```bash
nextflow run your-pipeline.nf -plugins town-crier@0.1.0
```

Or add it to your `nextflow.config`:

```groovy
plugins {
    id 'town-crier@0.1.0'
}
```

## Requirements

- Nextflow 25.04.0 or later (uses TraceObserverV2 API)

## Limitations

- Only handles `publishDir` files, not the new workflow output syntax. Will not capture publication events using the new [workflow output syntax](https://www.nextflow.io/docs/latest/workflow.html#workflow-outputs).
  
## License

Apache License 2.0
