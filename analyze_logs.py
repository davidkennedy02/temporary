import re
import os
import json
from collections import Counter
from datetime import datetime

CONFIG_FILE = 'config.json'

def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {}
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def get_log_file(config):
    log_dir = config.get('log_directory', 'logs')
    log_file = os.path.join(log_dir, 'app.log')
    return log_file

def clean_message(message):
    """Generalize the message by masking dynamic IDs and values."""
    
    # Extract the reason for field count errors
    match = re.search(r'Skipping record in batch [^:]+:\d+ record \d+ \(Patient [A-Z0-9]+\) - (.+)$', message)
    if match:
        return match.group(1)
    
    # Extract the reason for patient skip warnings
    match = re.search(r'Skipping patient [A-Z0-9]+ in batch [^:]+:\d+ record \d+ - (.+)$', message)
    if match:
        return match.group(1)
    
    # Failed to create HL7 message
    if 'Failed to create HL7 message for patient' in message:
        return "Failed to create HL7 message"
    
    # Group "Patient X has no date of birth" messages
    if 'has no date of birth, using placeholder' in message:
        return "has no date of birth, using placeholder 1970-01-01"
    
    # Group "Date of death earlier than date of birth" errors
    if 'Date of death' in message and 'is earlir than date of birth' in message:
        return "Date of death is earlier than date of birth"
    
    # Group "Reset invalid date of death" warnings
    if 'Reset invalid date of death to None for patient' in message:
        return "Reset invalid date of death to None"
    
    return message.strip()

def analyze_logs():
    config = load_config()
    log_file = get_log_file(config)
    
    if not os.path.exists(log_file):
        print(f"Log file not found at: {log_file}")
        return

    print(f"Analyzing log file: {log_file}...")
    
    # regex for standard log format
    # 2026-02-05 09:46:49,520 - ERROR - Message...
    log_pattern = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - (WARNING|ERROR|INFO) - (.*)$')
    
    # Pattern to extract saved message counts: "Successfully saved X messages for batch..."
    saved_pattern = re.compile(r'Successfully saved (\d+) messages for batch')
    
    # Pattern to extract batch record counts: "Starting to process batch ... with X records"
    batch_start_pattern = re.compile(r'Starting to process batch .+ with (\d+) records')
    
    # Pattern to extract batch completion for verification
    batch_pattern = re.compile(r'Batch .+ completed: (\d+)/(\d+) records')
    
    warnings = Counter()
    errors = Counter()
    line_count = 0
    total_saved = 0
    total_records = 0
    total_records_from_batches = 0
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            line_count += 1
            if line_count % 100000 == 0:
                print(f"Processed {line_count} lines...", end='\r')
            
            # Count total records from batch starts
            batch_start_match = batch_start_pattern.search(line)
            if batch_start_match:
                total_records += int(batch_start_match.group(1))
            
            # Count saved messages
            saved_match = saved_pattern.search(line)
            if saved_match:
                total_saved += int(saved_match.group(1))
            
            # Count total records from batch completions (for verification)
            batch_match = batch_pattern.search(line)
            if batch_match:
                total_records_from_batches += int(batch_match.group(2))
                
            match = log_pattern.match(line)
            if match:
                level, content = match.groups()
                category = clean_message(content)
                
                if level == 'WARNING':
                    warnings[category] += 1
                elif level == 'ERROR':
                    errors[category] += 1
    
    print("\n" + "="*80)
    print(f"LOG ANALYSIS REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    print(f"\nTotal Lines Processed: {line_count:,}")
    # derived values
    total_errors = sum(errors.values())
    total_initialized = total_records - total_errors
    actually_skipped = total_initialized - total_saved
    
    print(f"\nProcessing Summary:")
    print(f"  Total Records:     {total_records:,}")
    print(f"  Errors (skipped):  {total_errors:,} (invalid format, never initialized)")
    print(f"  Initialized:       {total_initialized:,}")
    print(f"  Skipped:           {actually_skipped:,} (initialized but failed validation)")
    print(f"  Saved to HL7:      {total_saved:,}")
    print(f"\nVerification: {total_errors} errors + {total_initialized} initialized = {total_errors + total_initialized} (should equal {total_records})")
    print(f"              {total_saved} saved + {actually_skipped} skipped = {total_saved + actually_skipped} (should equal {total_initialized})")
    if total_records_from_batches > 0:
        print(f"              Batch completions report {total_records_from_batches} total records (cross-check)")
    
    print(f"\nLog Message Counts:")
    print(f"  Total Warnings: {sum(warnings.values()):,} ({actually_skipped} validation failures)")
    print(f"  Total Errors:   {total_errors:,} ({total_errors} records with invalid format)")
    
    print("\n" + "-"*80)
    print("ERRORS (Count Descending)")
    print("-"*80)
    if not errors:
        print("No errors found.")
    else:
        print(f"{'COUNT':<10} | {'MESSAGE CATEGORY'}")
        print("-" * 80)
        for msg, count in errors.most_common():
            print(f"{count:<10} | {msg}")
            
    print("\n" + "-"*80)
    print("WARNINGS (Count Descending)")
    print("-"*80)
    if not warnings:
        print("No warnings found.")
    else:
        print(f"{'COUNT':<10} | {'MESSAGE CATEGORY'}")
        print("-" * 80)
        for msg, count in warnings.most_common():
            print(f"{count:<10} | {msg}")

if __name__ == "__main__":
    analyze_logs()
