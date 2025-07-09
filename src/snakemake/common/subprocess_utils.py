__author__ = "Johannes Köster"
__copyright__ = "Copyright 2025, Johannes Köster"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

import subprocess
import time
from typing import List, Optional, Tuple, Union, Pattern
import re
from reretry import retry

try:
    from snakemake_interface_common.exceptions import WorkflowError
except ImportError:
    # Fallback for when snakemake interfaces are not available
    class WorkflowError(Exception):
        pass

try:
    from snakemake.logging import logger
except ImportError:
    # Fallback logger for when snakemake is not available
    import logging
    logger = logging.getLogger(__name__)


# Common error patterns that indicate transient connectivity issues
CONNECTION_ERROR_PATTERNS = [
    re.compile(r"failed to open persistent connection.*Connection refused", re.IGNORECASE),
    re.compile(r"Connection refused", re.IGNORECASE),
    re.compile(r"slurm_persist_conn_open_without_init", re.IGNORECASE),
    re.compile(r"Problem talking to the database.*Connection refused", re.IGNORECASE),
    re.compile(r"Sending.*msg.*Connection refused", re.IGNORECASE),
    re.compile(r"No route to host", re.IGNORECASE),
    re.compile(r"Network is unreachable", re.IGNORECASE),
    re.compile(r"Connection timed out", re.IGNORECASE),
    re.compile(r"Temporary failure in name resolution", re.IGNORECASE),
]


class RetryableSubprocessError(Exception):
    """Exception raised when a subprocess command fails with a retryable error."""
    
    def __init__(self, cmd: str, returncode: int, stderr: str, stdout: str = ""):
        self.cmd = cmd
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = stdout
        super().__init__(f"Command '{cmd}' failed with retryable error: {stderr}")


def is_connection_error(error_text: str, patterns: Optional[List[Pattern]] = None) -> bool:
    """
    Check if an error message indicates a connection-related issue that might be retryable.
    
    Args:
        error_text: The error message to check
        patterns: Optional list of regex patterns to match against. If None, uses default patterns.
        
    Returns:
        True if the error appears to be a connection-related issue
    """
    if patterns is None:
        patterns = CONNECTION_ERROR_PATTERNS
    
    return any(pattern.search(error_text) for pattern in patterns)


@retry(
    exceptions=RetryableSubprocessError,
    tries=5,
    delay=3,
    backoff=2,
    max_delay=60,
    logger=logger
)
def check_output_with_retries(
    cmd: Union[str, List[str]],
    shell: bool = True,
    text: bool = True,
    timeout: Optional[float] = None,
    connection_error_patterns: Optional[List[Pattern]] = None,
    **kwargs
) -> str:
    """
    Execute a subprocess command with automatic retries for connection-related errors.
    
    This function is designed to handle transient connectivity issues that are common
    in cluster environments, particularly with SLURM commands like sacct.
    
    Args:
        cmd: Command to execute (string or list of strings)
        shell: Whether to execute via shell (default: True)
        text: Whether to return text output (default: True)
        timeout: Timeout for the subprocess call
        connection_error_patterns: Custom patterns to match retryable errors
        **kwargs: Additional arguments passed to subprocess.check_output
        
    Returns:
        The stdout of the command as a string
        
    Raises:
        RetryableSubprocessError: For connection-related errors (will be retried)
        subprocess.CalledProcessError: For non-retryable errors
        subprocess.TimeoutExpired: For timeout errors
    """
    try:
        result = subprocess.check_output(
            cmd,
            shell=shell,
            text=text,
            timeout=timeout,
            stderr=subprocess.PIPE,
            **kwargs
        )
        return result
    except subprocess.CalledProcessError as e:
        error_text = e.stderr if e.stderr else str(e)
        
        if is_connection_error(error_text, connection_error_patterns):
            # This is a connection error that we should retry
            logger.warning(
                f"Connection error detected in command '{cmd}': {error_text}. "
                "This appears to be a transient connectivity issue. Retrying..."
            )
            raise RetryableSubprocessError(
                cmd=str(cmd),
                returncode=e.returncode,
                stderr=e.stderr or "",
                stdout=e.output or ""
            )
        else:
            # This is a different kind of error, don't retry
            raise


def run_with_retries(
    cmd: Union[str, List[str]],
    shell: bool = True,
    text: bool = True,
    timeout: Optional[float] = None,
    connection_error_patterns: Optional[List[Pattern]] = None,
    **kwargs
) -> Tuple[Optional[str], Optional[float]]:
    """
    Execute a subprocess command with automatic retries, returning both result and duration.
    
    This is a variant of check_output_with_retries that returns timing information
    and handles errors gracefully by returning None values instead of raising exceptions.
    
    Args:
        cmd: Command to execute (string or list of strings)
        shell: Whether to execute via shell (default: True)
        text: Whether to return text output (default: True)
        timeout: Timeout for the subprocess call
        connection_error_patterns: Custom patterns to match retryable errors
        **kwargs: Additional arguments passed to subprocess.check_output
        
    Returns:
        Tuple of (output, duration_seconds) where both can be None if command failed
    """
    start_time = time.time()
    try:
        result = check_output_with_retries(
            cmd=cmd,
            shell=shell,
            text=text,
            timeout=timeout,
            connection_error_patterns=connection_error_patterns,
            **kwargs
        )
        duration = time.time() - start_time
        return result, duration
    except (subprocess.CalledProcessError, RetryableSubprocessError, subprocess.TimeoutExpired) as e:
        logger.error(f"Command '{cmd}' failed after retries: {e}")
        return None, None