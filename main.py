import csv
from datetime import date, datetime
import os
import traceback
import sys
import chardet
import hl7_utilities
import patientinfo
from logger import AppLogger
from config_manager import config
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

# Load configuration on startup
INPUT_FOLDER = config.get_input_folder()
HL7_OUTPUT_FOLDER = config.get_output_folder()
PAS_RECORD_SEPARATOR = "|"
BATCH_SIZE = config.get_batch_size()
NUM_WORKERS = config.get_max_workers()
MAX_RETRIES = config.get_max_retries()
EVENT_TYPE = config.get_default_event_type()

EXCLUDED_HOSPITAL_NUMBERS = {}

logger = AppLogger()


def calculate_age(birth_date):
    """Calculate the current age from a date of birth.

    Args:
        birth_date (datetime | str): The date of birth used to calculate age.

    Returns:
        int: The current age derived from the date of birth.
    """
    try:
        today = date.today()
        if isinstance(birth_date, str):
            try:
                birth_date = datetime.strptime(birth_date, "%Y%m%d")
            except ValueError:
                logger.log(f"Invalid birth date format: {birth_date}", "WARNING")
                return -1  # Invalid age
                
        age = (
            today.year
            - birth_date.year
            - ((today.month, today.day) < (birth_date.month, birth_date.day))
        )
        return age
    except Exception as e:
        logger.log(f"Error calculating age: {e}", "WARNING")
        return -1  # Return invalid age on error


def detect_encoding(file_path):
    """Detect the encoding of a file with robust error handling."""
    try:
        # Start with a small sample for efficiency
        with open(file_path, 'rb') as file:
            raw_data = file.read(10000)  # Read first 10KB
        
        result = chardet.detect(raw_data)
        
        # If confidence is too low, read more of the file
        if result['confidence'] < 0.7:
            with open(file_path, 'rb') as file:
                raw_data = file.read(100000)  # Try with 100KB
            result = chardet.detect(raw_data)
            
        logger.log(f"Detected encoding for {file_path}: {result['encoding']} (confidence: {result['confidence']:.2f})", "DEBUG")
        
        # Default to utf-8 if detection fails or confidence is still low
        if not result['encoding'] or result['confidence'] < 0.5:
            logger.log(f"Low confidence in encoding detection for {file_path}, defaulting to utf-8", "WARNING")
            return 'utf-8'
            
        return result['encoding']
    except Exception as e:
        logger.log(f"Error detecting encoding for {file_path}: {e}. Defaulting to utf-8", "ERROR")
        return 'utf-8'


def process_record_batch(batch, batch_id):
    """Process a batch of patient records and generate HL7 messages."""
    valid_messages = []  # Will store tuples of (hl7_message, patient_info)
    batch_log = []
    processed_count = 0
    error_count = 0
    skipped_count = 0
    
    start_time = time.time()
    batch_log.append((f"Starting to process batch {batch_id} with {len(batch)} records", "INFO"))
    
    try:
        for record in batch:
            try:
                # Validate record length before accessing indices
                if not isinstance(record, list):
                    batch_log.append((f"Invalid record type in batch {batch_id}: {type(record)}", "WARNING"))
                    skipped_count += 1
                    continue
                
                if len(record) < 25:
                    batch_log.append((f"Skipping record in batch {batch_id} - insufficient data fields (found {len(record)}, need 25)", "WARNING"))
                    skipped_count += 1
                    continue
                    
                # Clean up data - strip whitespace from all fields
                record = [field.strip() if isinstance(field, str) else field for field in record]
                
                # patient_data = {
                #     'internal_patient_number': record[0],
                #     'assigning_authority': record[1],
                #     'hospital_case_number': record[2],
                #     'nhs_number': record[3],
                #     'nhs_verification_status': record[4],
                #     'surname': record[5],
                #     'forename': record[6],
                #     'date_of_birth': record[7],
                #     'sex': record[8],
                #     'patient_title': record[9],
                #     'address_line_1': record[10],
                #     'address_line_2': record[11],
                #     'address_line_3': record[12],
                #     'address_line_4': record[13],
                #     'address_line_5': record[14],
                #     'postcode': record[15],
                #     'death_indicator': record[16],
                #     'date_of_death': record[17],
                #     'registered_gp_code': record[18],
                #     'ethnic_code': record[19],
                #     'home_phone': record[20],
                #     'work_phone': record[21],
                #     'mobile_phone': record[22],
                #     'registered_gp': record[23],
                #     'registered_practice': record[24]
                # }

                patient_data = {
                    'internal_patient_number': record[1],
                    'assigning_authority': record[2],
                    'hospital_case_number': record[3],
                    'nhs_number': record[4],
                    'nhs_verification_status': record[5],
                    'surname': record[6],
                    'forename': record[7],
                    'date_of_birth': record[8],
                    'sex': record[9],
                    'patient_title': record[10],
                    'address_line_1': record[11],
                    'address_line_2': record[12],
                    'address_line_3': record[13],
                    'address_line_4': record[14],
                    # address_line_5 is MISSING in the file
                    # The file has Postcode immediately after Address Line 4
                    'address_line_5': "",  # Placeholder
                    'postcode': record[15],
                    'death_indicator': record[16],
                    'date_of_death': record[17],
                    'registered_gp_code': record[18],
                    'ethnic_code': record[19],
                    'home_phone': record[20],
                    'work_phone': record[21],
                    'mobile_phone': record[22],
                    'registered_gp': record[23],
                    'registered_practice': record[24]
                }
                
                # Unpack into Patient object
                patient_info = patientinfo.Patient(**patient_data)
                
                # Minor exclusion checks
                if not patient_info.surname or patient_info.surname.strip() == "":
                    batch_log.append((f"Skipping patient {patient_info.internal_patient_number} - missing required surname", "WARNING"))
                    skipped_count += 1
                elif patient_info.date_of_birth and (patient_info.date_of_death is None) \
                                            and (calculate_age(patient_info.date_of_birth) > 112):
                    batch_log.append((f"Skipping patient {patient_info.internal_patient_number} - no DOD, and age > 112", "WARNING"))
                    skipped_count += 1
                elif patient_info.death_indicator and patient_info.date_of_death \
                                            and (patient_info.death_indicator == 'Y') \
                                            and (calculate_age(patient_info.date_of_death) > 2):
                    batch_log.append((f"Skipping patient {patient_info.internal_patient_number} - dod > 2 years ago", "WARNING"))
                    skipped_count += 1
                elif patient_info.hospital_case_number in EXCLUDED_HOSPITAL_NUMBERS:
                    batch_log.append((f"Skipping patient {patient_info.internal_patient_number} - hospital case number {patient_info.hospital_case_number} is in exclusion list", "INFO"))
                    skipped_count += 1
                else:
                    # Create the HL7 message and collect in batch with patient info
                    hl7_message = hl7_utilities.create_adt_message(patient_info=patient_info, event_type=EVENT_TYPE)
                    if hl7_message:
                        valid_messages.append((hl7_message, patient_info))
                        processed_count += 1
                    else:
                        batch_log.append((f"Failed to create HL7 message for patient {patient_info.internal_patient_number}", "WARNING"))
                        skipped_count += 1
                
                
            except Exception as e:
                error_trace = traceback.format_exc()
                batch_log.append((f"Error processing record in batch {batch_id}: {str(e)}\n{error_trace}", "ERROR"))
                error_count += 1
                
        if valid_messages:
            try:
                hl7_utilities.save_hl7_messages_batch(
                    hl7_messages=valid_messages, 
                    hl7_folder_path=config.get_output_folder(), 
                    batch_id=batch_id
                )
                batch_log.append((f"Successfully saved {len(valid_messages)} messages for batch {batch_id}", "INFO"))
            except Exception as e:
                error_trace = traceback.format_exc()
                batch_log.append((f"Error saving batch {batch_id}: {str(e)}\n{error_trace}", "ERROR"))
                
        end_time = time.time()
        duration = end_time - start_time
        batch_log.append((
            f"Batch {batch_id} completed: processed {processed_count}, skipped {skipped_count}, errors {error_count} in {duration:.2f} seconds", 
            "INFO"
        ))
    except Exception as batch_error:
        error_trace = traceback.format_exc()
        batch_log.append((f"Critical error processing batch {batch_id}: {str(batch_error)}\n{error_trace}", "CRITICAL"))
    
    return batch_log


def process_file_streaming(input_file, file_type):
    """Process a file using streaming to avoid loading all records into memory.
    Args:
        input_file (str): The path to the input file.
        file_type (str): The type of the file ('csv' or 'pas').
    Returns:
        None
    Notes:
        - Processes the file in batches to limit memory usage.
        - Uses parallel processing to speed up batch processing.
        - Includes robust error handling and logging.
    """
    encoding = 'utf-8'
    
    try:
        # Check if file exists and is readable
        if not os.path.exists(input_file):
            logger.log(f"File not found: {input_file}", "ERROR")
            return
            
        if not os.path.isfile(input_file):
            logger.log(f"Not a file: {input_file}", "ERROR")
            return
            
        if not os.access(input_file, os.R_OK):
            logger.log(f"File is not readable: {input_file}", "ERROR")
            return
        
        # Attempt to detect encoding first to avoid double file reading
        encoding = detect_encoding(input_file)
        logger.log(f"Using encoding for {input_file}: {encoding}", "INFO")
        
        batches = []
        batch_counter = 0
        
        # Handle file according to type with better error handling
        if file_type.lower() == "csv":
            try:
                with open(input_file, newline='', encoding=encoding, errors='replace') as file:
                    reader = csv.reader(file)
                    try:
                        headers = next(reader)  # Skip header row
                        logger.log(f"CSV headers: {headers}", "DEBUG")
                    except StopIteration:
                        logger.log(f"Empty CSV file: {input_file}", "WARNING")
                        return
                    
                    # Process in batches
                    batch = []
                    for i, record in enumerate(reader):
                        batch.append(record)
                        
                        if len(batch) >= BATCH_SIZE:
                            batch_counter += 1
                            batches.append((batch, batch_counter))
                            batch = []
                            
                    if batch:
                        batch_counter += 1
                        batches.append((batch, batch_counter))
            except UnicodeDecodeError as e:
                logger.log(f"Encoding error with {encoding} for {input_file}: {e}. Trying with utf-8 and ignore errors.", "WARNING")
                # Fallback to utf-8 with error ignoring if specified encoding fails
                with open(input_file, newline='', encoding='utf-8', errors='ignore') as file:
                    reader = csv.reader(file)
                    headers = next(reader)  # Skip header row
                    
                    # Process in batches
                    batch = []
                    for i, record in enumerate(reader):
                        batch.append(record)
                        
                        if len(batch) >= BATCH_SIZE:
                            batch_counter += 1
                            batches.append((batch, batch_counter))
                            batch = []
                            
                    if batch:
                        batch_counter += 1
                        batches.append((batch, batch_counter))
        
        elif file_type.lower() == "pas":
            try:
                with open(input_file, newline='', encoding=encoding, errors='replace') as file:
                    content = file.read()
                    records = []
                    
                    # Split by record separator more carefully
                    raw_records = content.splitlines()
                    for raw_record in raw_records:
                        if not raw_record.strip():
                            continue
                            
                        try:
                            record_fields = raw_record.split(PAS_RECORD_SEPARATOR)
                            if len(record_fields) >= 1:  # Ensure we have at least one field
                                records.append(record_fields)
                        except Exception as e:
                            logger.log(f"Error parsing PAS record: {e}", "WARNING")
                    
                    # Process in batches
                    for i in range(0, len(records), BATCH_SIZE):
                        batch = records[i:i+BATCH_SIZE]
                        batch_counter += 1
                        batches.append((batch, batch_counter))
            except UnicodeDecodeError as e:
                logger.log(f"Encoding error with {encoding} for {input_file}: {e}. Trying with utf-8 and ignore errors.", "WARNING")
                # Fallback to utf-8 with error ignoring
                with open(input_file, newline='', encoding='utf-8', errors='ignore') as file:
                    content = file.read()
                    records = [record.split(PAS_RECORD_SEPARATOR) for record in content.splitlines() if record.strip()]
                    
                    # Process in batches
                    for i in range(0, len(records), BATCH_SIZE):
                        batch = records[i:i+BATCH_SIZE]
                        batch_counter += 1
                        batches.append((batch, batch_counter))
        else:
            logger.log(f"Unsupported file type: {file_type}", "ERROR")
            return
            
        if not batches:
            logger.log(f"No records found in {input_file}", "WARNING")
            return
            
        # Process batches in parallel with better error handling
        logger.log(f"Starting parallel processing with {NUM_WORKERS} workers for {len(batches)} batches from {input_file}", "INFO")
        
        # Include filename in context for better logging
        file_basename = os.path.basename(input_file)
        
        # Track failed batches for potential retry
        failed_batches = []
        
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            future_to_batch = {
                executor.submit(process_record_batch, batch, f"{file_basename}:{batch_id}"): (batch, batch_id) 
                for batch, batch_id in batches
            }
            
            # Process logs from completed tasks
            for future in as_completed(future_to_batch):
                try:
                    batch_logs = future.result()
                    for log_message, log_level in batch_logs:
                        logger.log(log_message, log_level)
                except Exception as exc:
                    batch, batch_id = future_to_batch[future]
                    logger.log(f"Batch {file_basename}:{batch_id} generated an exception: {exc}", "ERROR")
                    failed_batches.append((batch, batch_id))
        
        # Retry failed batches
        retry_count = 0
        while failed_batches and retry_count < MAX_RETRIES:
            retry_count += 1
            logger.log(f"Retrying {len(failed_batches)} failed batches from {input_file}, attempt {retry_count}", "WARNING")
            
            still_failed = []
            for batch, batch_id in failed_batches:
                try:
                    batch_logs = process_record_batch(batch, f"{file_basename}:{batch_id}_retry{retry_count}")
                    for log_message, log_level in batch_logs:
                        logger.log(log_message, log_level)
                except Exception as exc:
                    logger.log(f"Batch {file_basename}:{batch_id} retry {retry_count} failed: {exc}", "ERROR")
                    still_failed.append((batch, batch_id))
            
            failed_batches = still_failed
            
        if failed_batches:
            logger.log(f"Unable to process {len(failed_batches)} batches from {input_file} after {MAX_RETRIES} retries", "ERROR")
        
        logger.log(f"Completed processing {input_file}", "INFO")
    
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.log(f"Failed to process {input_file}. Error: {str(e)}\n{error_trace}", "ERROR")


def process_files_in_folder():
    """Processes all files in the target folders using the streaming approach."""
    # Get current folder paths from configuration
    input_folder = config.get_input_folder()
    output_folder = config.get_output_folder()
    
    # Create output directory
    try:
        os.makedirs(output_folder, exist_ok=True)
        logger.log(f"Ensured output directory exists: {output_folder}", "INFO")
    except Exception as e:
        logger.log(f"Error creating output directory {output_folder}: {e}", "ERROR")
        # Try alternative location
        alt_output = os.path.join(os.path.expanduser("~"), "hl7_output")
        try:
            os.makedirs(alt_output, exist_ok=True)
            logger.log(f"Using alternative output directory: {alt_output}", "WARNING")
            output_folder = alt_output
        except Exception as alt_e:
            logger.log(f"Failed to create alternative output directory: {alt_e}", "CRITICAL")
            return

    # Check input directory
    if not os.path.exists(input_folder):
        logger.log(f"Input folder not found: {input_folder}", "CRITICAL")
        try:
            # Try to create it
            os.makedirs(input_folder, exist_ok=True)
            logger.log(f"Created input folder: {input_folder}. Please add files and run again.", "WARNING")
        except Exception as e:
            logger.log(f"Could not create input folder: {e}", "CRITICAL")
        return
    
    if not os.path.isdir(input_folder):
        logger.log(f"Input folder path is not a directory: {input_folder}", "CRITICAL")
        return
        
    # Get the list of files to process
    try:
        files = os.listdir(input_folder)
    except Exception as e:
        logger.log(f"Error reading input directory {input_folder}: {e}", "CRITICAL")
        return
        
    if not files:
        logger.log(f"No files found in input directory: {input_folder}", "WARNING")
        return
        
    csv_count = sum(1 for f in files if f.lower().endswith(".csv"))
    txt_count = sum(1 for f in files if f.lower().endswith(".txt"))
    
    logger.log(f"Found {csv_count} CSV files and {txt_count} TXT files in {input_folder}", "INFO")
    
    # Process each file
    for filename in files:
        input_file = os.path.join(input_folder, filename)
        try:
            if filename.lower().endswith(".csv"):
                logger.log(f"Processing CSV file: {filename}", "INFO")
                process_file_streaming(input_file, "csv")
            elif filename.lower().endswith(".txt"):
                logger.log(f"Processing PAS file: {filename}", "INFO")
                process_file_streaming(input_file, "PAS")
            else:
                logger.log(f"Skipping unsupported file: {filename}", "INFO")
        except Exception as e:
            error_trace = traceback.format_exc()
            logger.log(f"Error processing {filename}: {str(e)}\n{error_trace}", "ERROR")
    
    logger.log(f"Completed processing all files in {input_folder}", "INFO")


if __name__ == "__main__":
    try:
        logger.log("Starting CSVtoHL7 process", "INFO")
        
        # Validate configuration
        config_issues = config.validate_config()
        if config_issues:
            logger.log("Configuration validation issues found:", "ERROR")
            for issue in config_issues:
                logger.log(f"  - {issue}", "ERROR")
            logger.log("Please fix configuration issues before proceeding", "CRITICAL")
            sys.exit(1)
        
        # Log configuration summary
        logger.log(f"Using configuration: Input='{config.get_input_folder()}', Output='{config.get_output_folder()}'", "INFO")
        logger.log(f"HL7 Settings: Sending='{config.get_sending_application()}', Receiving='{config.get_receiving_application()}'", "INFO")
        logger.log(f"Processing: Batch size={config.get_batch_size()}, Workers={config.get_max_workers()}", "INFO")
        
        process_files_in_folder()
        logger.log("CSVtoHL7 process completed successfully", "INFO")
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.log(f"Critical error in main process: {str(e)}\n{error_trace}", "CRITICAL")
        sys.exit(1)
