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
from concurrent.futures import ProcessPoolExecutor, as_completed, wait, FIRST_COMPLETED
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
    """
    Process a batch of raw patient records and convert them into HL7 messages.

    Why:
        Processing records in batches allows us to parallelize the CPU-intensive task 
        of parsing, validating, and generating HL7 messages. It also isolates failures 
        so that one bad record doesn't crash the entire process.

    How:
        1. Validates structure: Checks if record is a list and has enough fields.
        2. Cleans data: Strips whitespace from strings.
        3. Maps fields: Converts raw list indices to named `patient_data` dictionary.
        4. Validates business rules: Checks for required fields (surname), valid dates 
           (age limits), and exclusions (hospital numbers).
        5. Generates HL7: Uses `hl7_utilities` to create the final message string.
        6. Saves: Batches successful messages and saves them to disk.

    Args:
        batch (list): A list of raw records (lists of strings).
        batch_id (str): Identifier for logging and tracking.

    Returns:
        list: A list of log tuples (message, level) generated during processing.
    """
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
                
                # Map record fields to Patient object attributes
                # Note: address_line_5 is missing in the source file structure, so we use a placeholder.

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
                    # Try fast generation first
                    hl7_message = hl7_utilities.create_adt_message_fast(patient_info=patient_info, event_type=EVENT_TYPE)
                    
                    # Fallback to legacy generation if fast method fails
                    if not hl7_message:
                        logger.log(f"Fast HL7 generation failed for patient {patient_info.internal_patient_number}. Falling back to legacy method.", "WARNING")
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


def get_file_reader_generator(file_path, file_type, batch_size, encoding='utf-8'):
    """
    Generator that lazily reads a file and yields batches of records.

    Why:
        Loading large files entirely into memory (e.g., `readlines()`) causes memory spikes 
        and potential crashes. A generator allows us to read only what we need for the 
        next batch, keeping memory usage low and constant (O(1) relative to file size).

    How:
        - For CSV: Uses `csv.reader` as an iterator. It iterates through rows one by one, 
          accumulating them into a `batch` list. When `batch_size` is reached, it yields 
          the batch and clears the list.
        - For PAS (text): Iterates the file object line-by-line. Splits each line by 
          the separator and behaves similarly to the CSV logic.
        - Handles encoding errors by attempting re-reads with fallback encodings if necessary.

    Args:
        file_path (str): Path to the input file.
        file_type (str): 'csv' or 'pas'.
        batch_size (int): Number of records per batch.
        encoding (str): File encoding.

    Yields:
        list: A list of records (each record is a list of strings).
    """
    
    if file_type.lower() == "csv":
        try:
            with open(file_path, newline='', encoding=encoding, errors='replace') as file:
                reader = csv.reader(file)
                try:
                    headers = next(reader)  # Skip header row
                    # logger.log(f"CSV headers: {headers}", "DEBUG") 
                except StopIteration:
                    return

                batch = []
                for record in reader:
                    batch.append(record)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                
                if batch:
                    yield batch
                    
        except UnicodeDecodeError:
            # Fallback to utf-8 ignore if specific encoding fails (re-opening file)
            with open(file_path, newline='', encoding='utf-8', errors='ignore') as file:
                reader = csv.reader(file)
                try:
                    next(reader) 
                except StopIteration:
                    return

                batch = []
                for record in reader:
                    batch.append(record)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                if batch:
                    yield batch

    elif file_type.lower() == "pas":
        # PAS logic: read line by line
        try:
            with open(file_path, newline='', encoding=encoding, errors='replace') as file:
                batch = []
                for line in file:
                    if not line.strip():
                        continue
                    
                    try:
                        record_fields = line.strip().split(PAS_RECORD_SEPARATOR)
                        if len(record_fields) >= 1:
                            batch.append(record_fields)
                    except Exception:
                        continue # Skip malformed lines without crashing generator
                    
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                
                if batch:
                    yield batch
                    
        except UnicodeDecodeError:
             with open(file_path, newline='', encoding='utf-8', errors='ignore') as file:
                batch = []
                for line in file:
                    if not line.strip(): 
                        continue
                    try:
                        record_fields = line.strip().split(PAS_RECORD_SEPARATOR)
                        if len(record_fields) >= 1:
                            batch.append(record_fields)
                    except Exception:
                        continue

                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                if batch:
                    yield batch


def process_file_streaming(input_file, file_type):
    """
    Orchestrates the streaming processing of a single file using bounded parallelism.

    Why:
        We need to process files that may be larger than available RAM. Simply using 
        `ProcessPoolExecutor` normally (submitting all tasks at once) would still queue 
        up all batches in memory, defeating the purpose of the generator. 
        
        We need "backpressure" to stop reading the file when the workers are busy.

    How:
        1. file_reader_generator: Reads chunks of data from disk (Lazy IO).
        2. ProcessPoolExecutor: distributing chunks to multiple worker processes (CPU parallelism).
        3. Backpressure Loop:
            - We check the number of active `futures` (tasks).
            - If active tasks >= `MAX_PENDING_BATCHES` (set to 2 * CPUs), we `wait()` 
              for at least one task to complete before reading the next batch from disk.
            - This ensures we never hold more than ~2xCPUs worth of data in RAM at once.

    Args:
        input_file (str): The path to the input file.
        file_type (str): The type of the file ('csv' or 'pas').
    Returns:
        None
    """
    try:
        if not os.path.exists(input_file):
            logger.log(f"File not found: {input_file}", "ERROR")
            return
            
        # Detect encoding once
        encoding = detect_encoding(input_file)
        logger.log(f"Starting processing of {input_file} (Encoding: {encoding})", "INFO")
        
        file_basename = os.path.basename(input_file)
        
        # Generator yields batches (List[List[str]]) lazily
        batch_generator = get_file_reader_generator(input_file, file_type, BATCH_SIZE, encoding)
        
        futures = set()
        MAX_PENDING_BATCHES = NUM_WORKERS * 2  # Backpressure limit
        
        batch_counter = 0
        failed_batches = [] 

        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            
            for batch in batch_generator:
                batch_counter += 1
                batch_id = f"{batch_counter}"
                
                # Backpressure: wait if too many tasks are pending
                if len(futures) >= MAX_PENDING_BATCHES:
                    done, futures = wait(futures, return_when=FIRST_COMPLETED)
                    
                    for future in done:
                        try:
                            batch_logs = future.result()
                            for log_message, log_level in batch_logs:
                                logger.log(log_message, log_level)
                        except Exception as exc:
                            pass # specific errors handled via batch_data below

                future = executor.submit(process_record_batch, batch, f"{file_basename}:{batch_id}")
                futures.add(future)
                future.batch_data = (batch, batch_id) 

            # Wait for remaining futures
            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        batch_logs = future.result()
                        for log_message, log_level in batch_logs:
                            logger.log(log_message, log_level)
                    except Exception as exc:
                        if hasattr(future, 'batch_data'):
                            batch, batch_id = future.batch_data
                            logger.log(f"Batch {file_basename}:{batch_id} failed: {exc}", "ERROR")
                            failed_batches.append((batch, batch_id))
                        else:
                            logger.log(f"Unknown batch failed: {exc}", "ERROR")

        # Retry logic for failed batches
        if failed_batches:
            logger.log(f"Retrying {len(failed_batches)} failed batches (Max Retries: {MAX_RETRIES})", "WARNING")
            for _ in range(MAX_RETRIES):
                if not failed_batches:
                    break
                    
                retry_failed = []
                for batch, batch_id in failed_batches:
                    try:
                        batch_logs = process_record_batch(batch, f"{file_basename}:{batch_id}_retry")
                        for log_message, log_level in batch_logs:
                            logger.log(log_message, log_level)
                    except Exception as e:
                        logger.log(f"Retry failed for {batch_id}: {e}", "ERROR")
                        retry_failed.append((batch, batch_id))
                
                failed_batches = retry_failed
                
            if failed_batches:
                logger.log(f"Final failure: {len(failed_batches)} batches could not be processed.", "ERROR")

        logger.log(f"Completed streaming processing of {input_file}", "INFO")

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
