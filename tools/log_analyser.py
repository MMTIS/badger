#!/usr/bin/env python3
"""
Log Analyser for script_runner.py log files.

This script analyses log files from script_runner.py and generates a markdown summary
with run sequences and errors.

Usage:
    python log_analyser.py -i <logfile_or_folder> -t <days_back> -o <output_md_file>

Arguments:
    -i, --input: Log file or folder containing .log files to analyse
    -t, --days: Number of days back to consider (older entries are ignored)
    -o, --output: Output markdown file path
"""

import argparse
import glob
import os
import re
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional


# Pattern to match log entries
LOG_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - script_runner - (\w+) - (.+)$"
)

# Pattern to match step start entries
STEP_START_PATTERN = re.compile(
    r"^(.+?) - step: (\d+): (.+)$"
)

# Pattern to match execution time entries
EXECUTION_TIME_PATTERN = re.compile(
    r"^Execution time: ([\d.]+) seconds for (.+? - step: \d+: )?(?:(.+) \(?([\d.]+)s\)?|(.+))$"
)

# Better pattern for execution time
EXEC_TIME_PATTERN = re.compile(
    r"^Execution time: ([\d.]+) seconds for (.+)$"
)

# Pattern to extract block name and step info from step lines
# This should match messages like "nl1epip - step: 1: clean_tmp ..."
# but NOT "Execution time: ... for nl1epip - step: 1: ..."
BLOCK_STEP_PATTERN = re.compile(
    r"^([a-zA-Z0-9_-]+) - step: (\d+): (.+)$"
)

# Pattern for error messages that indicate script termination
SCRIPT_ERROR_PATTERN = re.compile(
    r"^Script (\S+) returned an error\. Terminating the block of scripts: (\S+)$"
)


class LogEntry:
    """Represents a single log entry."""
    
    def __init__(self, timestamp_str: str, level: str, message: str, line_number: int = 0):
        self.timestamp_str = timestamp_str
        self.timestamp = self._parse_timestamp(timestamp_str)
        self.level = level
        self.message = message
        self.line_number = line_number
    
    def _parse_timestamp(self, ts_str: str) -> datetime:
        """Parse timestamp string to datetime object."""
        # Handle comma in milliseconds
        ts_str = ts_str.replace(",", ".")
        try:
            return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            # Try without microseconds
            return datetime.strptime(ts_str.split(",")[0], "%Y-%m-%d %H:%M:%S")
    
    def is_within_days(self, days: int, reference_date: datetime = None) -> bool:
        """Check if this entry is within the specified number of days from reference."""
        if reference_date is None:
            reference_date = datetime.now()
        return (reference_date - self.timestamp) <= timedelta(days=days)
    
    def __repr__(self):
        return f"LogEntry({self.timestamp_str}, {self.level}, {self.message[:50]})"


class RunSequence:
    """Represents a run sequence (a block of scripts)."""
    
    def __init__(self, name: str, start_time: datetime):
        self.name = name
        self.start_time = start_time
        self.start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        self.steps: List[Tuple[str, float]] = []  # (script_name, execution_time)
        self.current_script: Optional[str] = None
        self.current_step_start: Optional[datetime] = None
    
    def add_step(self, script_name: str, execution_time: float):
        """Add a step with its execution time."""
        self.steps.append((script_name, execution_time))
    
    def __repr__(self):
        steps_str = ", ".join([f"{name} ({time}s)" for name, time in self.steps])
        return f"RunSequence({self.name}, {self.start_time_str}, steps: [{steps_str}])"


class ErrorEntry:
    """Represents an error with its context."""
    
    def __init__(self, error_message: str, stacktrace: str, 
                 script_name: str = "", block_name: str = "", execution_time: float = 0.0):
        self.error_message = error_message
        self.stacktrace = stacktrace
        self.script_name = script_name
        self.block_name = block_name
        self.execution_time = execution_time
    
    def __repr__(self):
        return f"ErrorEntry({self.error_message[:50]}, block={self.block_name}, script={self.script_name})"


def parse_log_file(file_path: str) -> List[LogEntry]:
    """Parse a log file and return a list of LogEntry objects."""
    entries = []
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        current_traceback = None
        line_number = 0
        
        for line in f:
            line_number += 1
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            # Check if this is a traceback line (indented lines after ERROR)
            if line.startswith(' ') and current_traceback is not None:
                # This is part of a traceback, we'll handle it with the error
                continue
            
            # Try to parse as a log entry
            match = LOG_PATTERN.match(line)
            if match:
                timestamp_str, level, message = match.groups()
                entry = LogEntry(timestamp_str, level, message, line_number)
                entries.append(entry)
                
                # If this is an ERROR, the next non-log lines might be traceback
                if level == 'ERROR':
                    current_traceback = entry
                else:
                    current_traceback = None
            else:
                # This might be a traceback line
                current_traceback = None
    
    return entries


def extract_run_sequences(entries: List[LogEntry]) -> Dict[str, RunSequence]:
    """
    Extract run sequences from log entries.
    
    A run sequence is identified by entries like:
    "nl1epip - step: 1: clean_tmp ..."
    
    Returns a dict mapping sequence name to RunSequence object.
    """
    sequences: Dict[str, RunSequence] = {}
    current_sequence: Optional[RunSequence] = None
    
    for entry in entries:
        if entry.level != 'INFO':
            continue
        
        message = entry.message
        
        # Check for step start pattern: "block - step: N: script_name args..."
        # Only match if the message starts with a block name (not "Execution time:")
        if message.startswith("Execution time:"):
            # This is an execution time message, handle below
            pass
        else:
            step_match = BLOCK_STEP_PATTERN.match(message)
            if step_match:
                block_name, step_num, script_with_args = step_match.groups()
                
                # Extract just the script name (first word or module.path format)
                script_name = script_with_args.split()[0] if script_with_args else "unknown"
                
                # If this is step 1, it's the start of a new sequence
                if step_num == '1':
                    current_sequence = RunSequence(block_name, entry.timestamp)
                    sequences[block_name] = current_sequence
                elif current_sequence is None or current_sequence.name != block_name:
                    # Check if we have a sequence for this block
                    if block_name not in sequences:
                        current_sequence = RunSequence(block_name, entry.timestamp)
                        sequences[block_name] = current_sequence
                    else:
                        current_sequence = sequences[block_name]
                
                # Add step to current sequence (without execution time initially)
                if current_sequence:
                    current_sequence.add_step(script_name, 0.0)
                continue
        
        # Check for execution time pattern
        exec_match = EXEC_TIME_PATTERN.match(message)
        if exec_match:
            time_seconds = float(exec_match.group(1))
            rest = exec_match.group(2)
            
            # Extract block name and step info from rest
            # Format: "block - step: N: script args"
            # We need to find the pattern " - step: N: " in rest
            step_marker = " - step: "
            idx = rest.find(step_marker)
            if idx > 0:
                block_name = rest[:idx]
                rest_after = rest[idx + len(step_marker):]
                
                # Find the step number
                # Step number is followed by a colon and space
                colon_idx = rest_after.find(": ")
                if colon_idx > 0:
                    step_num = rest_after[:colon_idx]
                    script_with_args = rest_after[colon_idx + 2:]
                else:
                    step_num = ""
                    script_with_args = rest_after
                
                script_name = script_with_args.split()[0] if script_with_args else "unknown"
                
                # Store execution time for this step
                if block_name in sequences:
                    # Update the last step with this execution time
                    if sequences[block_name].steps:
                        # Get the last step and update its time
                        # But we need to match the step number
                        # For now, just update the last step
                        last_step = sequences[block_name].steps[-1]
                        sequences[block_name].steps[-1] = (script_name, time_seconds)
    
    return sequences


def extract_errors(entries: List[LogEntry], file_path: str = "") -> List[ErrorEntry]:
    """
    Extract error entries with their context from entries and file.
    
    The pattern is:
    1. First ERROR line with the actual error message
    2. Stacktrace lines (not matching log pattern, so not in entries)
    3. Second ERROR line: "Script X returned an error. Terminating the block of scripts: Y"
    
    We want to use the second ERROR line as the heading, and include the first ERROR
    message + stacktrace as the codeblock.
    """
    errors = []
    
    if not file_path:
        return errors
    
    # Read all lines from file for traceback extraction
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        all_file_lines = f.readlines()
    
    # Build a mapping from line number to entry index
    line_to_entry = {}
    for idx, entry in enumerate(entries):
        line_to_entry[entry.line_number] = idx
    
    # Find ERROR entries in the file
    error_line_numbers = []
    for idx, entry in enumerate(entries):
        if entry.level == 'ERROR':
            error_line_numbers.append(entry.line_number)
    
    # Process ERROR entries in pairs
    i = 0
    while i < len(error_line_numbers):
        line_num = error_line_numbers[i]
        entry_idx = line_to_entry.get(line_num)
        if entry_idx is None:
            i += 1
            continue
        
        entry = entries[entry_idx]
        error_message = entry.message
        
        # Check if this is a "Script X returned an error" message
        script_error_match = SCRIPT_ERROR_PATTERN.match(error_message)
        
        if script_error_match:
            # This is the second ERROR message (the summary)
            script_name = script_error_match.group(1)
            block_name = script_error_match.group(2)
            
            # Look back for the previous ERROR that is NOT a "Script X returned an error" message
            # We want the actual error message, not another summary
            prev_line_num = None
            prev_entry = None
            for j in range(i - 1, -1, -1):
                prev_ln = error_line_numbers[j]
                prev_idx = line_to_entry.get(prev_ln)
                if prev_idx is not None:
                    prev_entry_candidate = entries[prev_idx]
                    # Skip if this is also a "Script X returned an error" message
                    if SCRIPT_ERROR_PATTERN.match(prev_entry_candidate.message):
                        continue
                    # Found a non-summary ERROR message
                    prev_line_num = prev_ln
                    prev_entry = prev_entry_candidate
                    break
            
            execution_time = 0.0
            traceback_lines = []
            
            if prev_entry is not None:
                # Start with the actual error message (not a summary)
                traceback_lines = [f"ERROR - {prev_entry.message}"]
                
                # Get all lines between prev_line_num and line_num
                # But only include lines that are actual traceback (not log entries)
                for ln in range(prev_line_num + 1, line_num):
                    if ln <= len(all_file_lines):
                        line_text = all_file_lines[ln - 1].rstrip()
                        # Check if this line is a log entry (matches LOG_PATTERN)
                        if LOG_PATTERN.match(line_text):
                            # This is a log entry (like execution time), check if it's execution time
                            log_match = LOG_PATTERN.match(line_text)
                            if log_match:
                                ts, level, msg = log_match.groups()
                                if level == 'INFO' and 'Execution time:' in msg:
                                    exec_match = EXEC_TIME_PATTERN.match(msg)
                                    if exec_match:
                                        execution_time = float(exec_match.group(1))
                            continue
                        # Only add non-log-entry lines (traceback lines)
                        traceback_lines.append(line_text)
                
                # Also look for execution time between prev_line_num and line_num (in case we missed it)
                if execution_time == 0.0:
                    for ln in range(prev_line_num + 1, line_num):
                        entry_idx_check = line_to_entry.get(ln)
                        if entry_idx_check is not None:
                            check_entry = entries[entry_idx_check]
                            if check_entry.level == 'INFO' and 'Execution time:' in check_entry.message:
                                exec_match = EXEC_TIME_PATTERN.match(check_entry.message)
                                if exec_match:
                                    execution_time = float(exec_match.group(1))
                                    break
                
                # Combine previous error message with traceback
                full_traceback = '\n'.join(traceback_lines)
                
                error = ErrorEntry(
                    error_message=error_message,
                    stacktrace=full_traceback,
                    script_name=script_name,
                    block_name=block_name,
                    execution_time=execution_time
                )
                errors.append(error)
                i += 1  # Skip the previous error
                continue
            
            # No previous ERROR found
            error = ErrorEntry(
                error_message=error_message,
                stacktrace="",
                script_name=script_name,
                block_name=block_name,
                execution_time=0.0
            )
            errors.append(error)
        
        i += 1
    
    return errors


def get_traceback_between_lines(entries: List[LogEntry], start_line: int, end_line: int) -> str:
    """Get traceback lines between two log entries from the file."""
    # This is a simplified version - we need to read from the actual file
    # For now, return empty string
    return ""


def get_traceback_from_file(file_path: str, start_line: int, end_line: int) -> str:
    """Read and return lines between start_line and end_line from a file."""
    traceback_lines = []
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line_num, line in enumerate(f, 1):
            if line_num > end_line:
                break
            if line_num >= start_line and line_num < end_line:
                traceback_lines.append(line.rstrip())
    
    return '\n'.join(traceback_lines)


def analyse_log_file(file_path: str, days_back: int) -> Tuple[Dict[str, RunSequence], List[ErrorEntry]]:
    """Analyse a single log file and return sequences and errors."""
    entries = parse_log_file(file_path)
    
    # Filter entries by date
    cutoff_date = datetime.now() - timedelta(days=days_back)
    recent_entries = [e for e in entries if e.timestamp >= cutoff_date]
    
    # Extract sequences and errors from recent entries
    sequences = extract_run_sequences(recent_entries)
    errors = extract_errors(recent_entries, file_path)
    
    return sequences, errors


def generate_markdown_report(sequences: List[RunSequence], 
                            errors: List[ErrorEntry], 
                            file_path: str = "") -> str:
    """Generate a markdown report from the analysis."""
    md_lines = []
    
    # Title
    md_lines.append("# Log Analysis Report")
    md_lines.append("")
    
    if file_path:
        md_lines.append(f"**Analysed file:** `{file_path}`")
        md_lines.append("")
    
    # Run Sequences section
    md_lines.append("## Run Sequences")
    md_lines.append("")
    
    if sequences:
        # Sort sequences by start time
        sorted_sequences = sorted(sequences, key=lambda x: x.start_time)
        for sequence in sorted_sequences:
            # Format: "2025-12-27 13:28:18,189, nl1epip: clan_tmp, download_input_file, conv.netex_to_db (8.3s), ..."
            start_time_str = sequence.start_time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Format steps
            step_parts = []
            for script_name, exec_time in sequence.steps:
                if exec_time > 0:
                    step_parts.append(f"{script_name} ({exec_time}s)")
                else:
                    step_parts.append(script_name)
            
            steps_str = ", ".join(step_parts)
            md_lines.append(f"- {start_time_str}, {sequence.name}: {steps_str}")
        
        md_lines.append("")
    else:
        md_lines.append("No run sequences found.")
        md_lines.append("")
    
    # Found Errors section
    md_lines.append("## Found Errors")
    md_lines.append("")
    
    if errors:
        # Deduplicate errors based on script_name and the first line of the error message
        seen_errors = set()
        unique_errors = []
        
        for error in errors:
            # Create a key based on script_name and the first line of the error message
            # The first line of stacktrace is the actual error
            first_error_line = ""
            if error.stacktrace:
                first_error_line = error.stacktrace.split('\n')[0]
            
            # Use script_name and the actual error message as the key
            error_key = (error.script_name, first_error_line)
            
            if error_key not in seen_errors:
                seen_errors.add(error_key)
                unique_errors.append(error)
        
        for error in unique_errors:
            # Use the error message as subheading
            md_lines.append(f"### {error.error_message}")
            md_lines.append("")
            
            # Add execution time if available as normal text
            if error.execution_time > 0:
                md_lines.append(f"Execution time: {error.execution_time}s")
                md_lines.append("")
            
            # Add stacktrace as code block
            if error.stacktrace:
                md_lines.append("```")
                md_lines.append(error.stacktrace)
                md_lines.append("```")
                md_lines.append("")
            
            # Separator
            md_lines.append("---")
            md_lines.append("")
    else:
        md_lines.append("No errors found.")
        md_lines.append("")
    
    return '\n'.join(md_lines)


def analyse_log_files(input_path: str, days_back: int) -> Tuple[List[RunSequence], List[ErrorEntry], List[str]]:
    """
    Analyse log files from the given path (file or folder).
    
    Returns:
        Tuple of (all_sequences, all_errors, analysed_files)
    """
    all_sequences: List[RunSequence] = []
    all_errors: List[ErrorEntry] = []
    analysed_files: List[str] = []
    
    if os.path.isfile(input_path):
        # Single file
        if input_path.endswith('.log'):
            files_to_analyse = [input_path]
        else:
            files_to_analyse = []
    elif os.path.isdir(input_path):
        # All .log files in folder
        files_to_analyse = glob.glob(os.path.join(input_path, '*.log'))
    else:
        raise ValueError(f"Path does not exist: {input_path}")
    
    for file_path in files_to_analyse:
        analysed_files.append(file_path)
        sequences, errors = analyse_log_file(file_path, days_back)
        
        # Add all sequences (don't merge, keep all runs)
        all_sequences.extend(sequences.values())
        
        # Add errors
        all_errors.extend(errors)
    
    return all_sequences, all_errors, analysed_files


def main():
    parser = argparse.ArgumentParser(
        description='Analyse script_runner.py log files and generate a markdown summary.'
    )
    parser.add_argument(
        '-i', '--input',
        type=str,
        required=True,
        help='Log file or folder containing .log files to analyse'
    )
    parser.add_argument(
        '-t', '--days',
        type=int,
        default=10,
        help='Number of days back to consider (default: 10)'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        required=True,
        help='Output markdown file path'
    )
    
    args = parser.parse_args()
    
    # Validate input path
    if not os.path.exists(args.input):
        print(f"Error: Input path does not exist: {args.input}")
        return
    
    # Analyse logs
    sequences, errors, analysed_files = analyse_log_files(args.input, args.days)
    
    # Generate report
    if len(analysed_files) == 1:
        report = generate_markdown_report(sequences, errors, analysed_files[0])
    else:
        report = generate_markdown_report(sequences, errors, ", ".join(analysed_files))
    
    # Write output
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(report)
    
    # Count unique errors (after deduplication)
    if errors:
        seen_errors = set()
        for error in errors:
            first_error_line = ""
            if error.stacktrace:
                first_error_line = error.stacktrace.split('\n')[0]
            error_key = (error.script_name, first_error_line)
            seen_errors.add(error_key)
        unique_error_count = len(seen_errors)
    else:
        unique_error_count = 0
    
    print(f"Report written to: {args.output}")
    print(f"Analysed {len(analysed_files)} file(s)")
    print(f"Found {len(sequences)} run sequence(s)")
    print(f"Found {len(errors)} error(s) ({unique_error_count} unique)")


if __name__ == '__main__':
    main()
