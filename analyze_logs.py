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
    # Mask Patient Internal Numbers (e.g., C023444, 12345)
    msg = re.sub(r'Patient internal number [A-Z0-9]+', 'Patient internal number <ID>', message)
    msg = re.sub(r'Patient [A-Z0-9]+ ', 'Patient <ID> ', msg)
    msg = re.sub(r'patient [A-Z0-9]+', 'patient <ID>', msg)
    msg = re.sub(r'Skipping patient [A-Z0-9]+', 'Skipping patient <ID>', msg)
    
    # Mask specific timestamps or invalid dates
    # "Invalid date TR26 2NU" -> "Invalid date <VALUE>"
    msg = re.sub(r'Invalid date [^ ]+', 'Invalid date <VALUE>', msg)
    
    # Mask variable dates in comparisons
    msg = re.sub(r'earlier than date of birth \d+', 'earlier than date of birth <DOB>', msg)
    
    # Mask other potential IDs if needed
    return msg.strip()

def analyze_logs():
    config = load_config()
    log_file = get_log_file(config)
    
    if not os.path.exists(log_file):
        print(f"Log file not found at: {log_file}")
        return

    print(f"Analyzing log file: {log_file}...")
    
    # regex for standard log format
    # 2026-02-05 09:46:49,520 - ERROR - Message...
    log_pattern = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - (WARNING|ERROR) - (.*)$')
    
    warnings = Counter()
    errors = Counter()
    
    line_count = 0
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            line_count += 1
            if line_count % 100000 == 0:
                print(f"Processed {line_count} lines...", end='\r')
                
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
    
    print(f"\nTotal Lines Processed: {line_count}")
    print(f"Total Warnings: {sum(warnings.values())}")
    print(f"Total Errors:   {sum(errors.values())}")
    
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
