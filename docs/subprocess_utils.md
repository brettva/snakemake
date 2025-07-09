# Subprocess Utilities with Retry Logic

This document describes the subprocess utilities added to address connection errors in SLURM and other cluster environments.

## Problem Statement

Long-running Snakemake workflows using SLURM executor plugins were failing with intermittent connection errors:

```
sacct: error: _open_persist_conn: failed to open persistent connection to host:slurm:6819: Connection refused
sacct: error: Sending PersistInit msg: Connection refused
sacct: error: Problem talking to the database: Connection refused
```

These errors caused entire workflows to crash, requiring manual restart. The issue was reported in GitHub issue and affects users running workflows for extended periods (e.g., over a day).

## Solution Overview

Added `snakemake.common.subprocess_utils` module providing robust subprocess execution with automatic retry logic for connection-related errors.

### Key Features

- **Automatic retry with exponential backoff** for transient connection issues
- **Pattern matching for known connection error types** (SLURM, network, etc.)
- **Distinction between retryable and permanent errors** 
- **Configurable retry parameters** using existing `reretry` dependency
- **Minimal performance impact** - only retries on actual connection errors

## API Reference

### Functions

#### `check_output_with_retries(cmd, **kwargs)`

Direct replacement for `subprocess.check_output` with automatic retries.

**Parameters:**
- `cmd`: Command to execute (string or list)
- `shell`: Whether to use shell (default: True)
- `text`: Return text output (default: True) 
- `timeout`: Subprocess timeout
- `connection_error_patterns`: Custom error patterns (optional)
- `**kwargs`: Additional subprocess arguments

**Returns:** Command output as string

**Raises:**
- `RetryableSubprocessError`: For connection errors (automatically retried)
- `subprocess.CalledProcessError`: For non-retryable errors
- `subprocess.TimeoutExpired`: For timeout errors

#### `run_with_retries(cmd, **kwargs)`

Graceful variant that returns (output, duration) tuple.

**Parameters:** Same as `check_output_with_retries`

**Returns:** Tuple of `(output: str | None, duration: float | None)`
- Returns `(None, None)` on failure instead of raising exceptions

#### `is_connection_error(error_text, patterns=None)`

Check if error message indicates a retryable connection issue.

**Parameters:**
- `error_text`: Error message to check
- `patterns`: Optional custom regex patterns

**Returns:** Boolean indicating if error is connection-related

### Error Patterns

The utility automatically detects these connection error patterns:

- `failed to open persistent connection.*Connection refused`
- `Connection refused`
- `slurm_persist_conn_open_without_init`
- `Problem talking to the database.*Connection refused`
- `Sending.*msg.*Connection refused`
- `No route to host`
- `Network is unreachable`
- `Connection timed out`
- `Temporary failure in name resolution`

### Retry Configuration

Default retry settings (configurable via `@retry` decorator):
- **tries**: 5 attempts
- **delay**: 3 seconds initial delay
- **backoff**: 2x exponential backoff
- **max_delay**: 60 seconds maximum delay

## Usage Examples

### Basic Usage

```python
from snakemake.common import check_output_with_retries

# Direct replacement for subprocess.check_output
result = check_output_with_retries("sacct -X --parsable2 --clusters all ...")
```

### SLURM Plugin Integration

Replace the existing `job_stati` method pattern:

```python
# OLD - fails immediately on connection errors
try:
    command_res = subprocess.check_output(
        command, text=True, shell=True, stderr=subprocess.PIPE
    )
except subprocess.CalledProcessError as e:
    # Connection errors cause workflow to crash
    return None, None

# NEW - automatic retries for connection errors  
from snakemake.common import run_with_retries

command_res, query_duration = run_with_retries(command)
if command_res is None:
    # Only fails after exhausting all retry attempts
    return None, None
```

### Custom Error Patterns

```python
import re
from snakemake.common import check_output_with_retries

custom_patterns = [
    re.compile(r"database.*unavailable", re.IGNORECASE),
    re.compile(r"service.*timeout", re.IGNORECASE),
]

result = check_output_with_retries(
    command, 
    connection_error_patterns=custom_patterns
)
```

### Graceful Error Handling

```python
from snakemake.common import run_with_retries

result, duration = run_with_retries("potentially_failing_command")
if result is not None:
    print(f"Command succeeded in {duration:.2f} seconds")
    # Process result
else:
    print("Command failed after all retries")
    # Handle failure gracefully
```

## Testing

The module includes comprehensive tests covering:

- Connection error pattern detection
- Retry functionality with exponential backoff
- Proper handling of non-retryable errors
- Performance and timing validation

Run tests:
```bash
python -m unittest tests/test_subprocess_utils.py
```

## Migration Guide

### For Executor Plugin Developers

1. **Import the utility:**
   ```python
   from snakemake.common import run_with_retries
   ```

2. **Replace subprocess calls in status checking:**
   ```python
   # Replace this pattern:
   subprocess.check_output(command, stderr=subprocess.PIPE)
   
   # With this:
   run_with_retries(command)
   ```

3. **Update error handling:**
   ```python
   # OLD
   except subprocess.CalledProcessError as e:
       logger.error("Command failed")
       return None
   
   # NEW - automatic retry handling
   result, duration = run_with_retries(command)
   if result is None:
       logger.error("Command failed after retries")
       return None
   ```

### For SLURM Plugin Specifically

The SLURM executor plugin's `job_stati` method should be updated to use `run_with_retries` for the `sacct` command execution. This will automatically handle the specific connection errors mentioned in the GitHub issue.

## Performance Impact

- **Minimal overhead** for successful commands (single execution)
- **Exponential backoff** prevents overwhelming struggling services
- **Intelligent error detection** avoids retrying non-connection errors
- **Configurable timeouts** prevent indefinite hanging

## Benefits

1. **Improved workflow reliability** - Workflows continue through temporary connectivity issues
2. **Reduced manual intervention** - No need to manually restart workflows
3. **Better resource utilization** - Workflows don't waste compute time due to transient errors
4. **Backwards compatibility** - Drop-in replacement for existing subprocess calls
5. **Extensible design** - Custom error patterns and retry configurations supported

## Related Issues

This utility addresses the SLURM connection issues reported in GitHub issue where users experienced workflow crashes with "Connection refused" errors after running for extended periods.