"""
File processing module for reading and converting patient records to HL7 messages.

This module handles:
- File encoding detection
- Batch reading from CSV and PAS files using generators
- Parallel batch processing with backpressure control
- Retry logic for failed batches
"""

import csv
import os
import traceback
import time
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED

import chardet
import hl7_utilities
import patientinfo
from logger import AppLogger
from config_manager import config

logger = AppLogger()

# Cache patient mapping indices at module level for fast direct array access
_mapping = config.get_patient_mapping()
_idx = {
    'internal_patient_number': _mapping.get('internal_patient_number'),
    'assigning_authority': _mapping.get('assigning_authority'),
    'hospital_case_number': _mapping.get('hospital_case_number'),
    'nhs_number': _mapping.get('nhs_number'),
    'nhs_verification_status': _mapping.get('nhs_verification_status'),
    'surname': _mapping.get('surname'),
    'forename': _mapping.get('forename'),
    'date_of_birth': _mapping.get('date_of_birth'),
    'sex': _mapping.get('sex'),
    'patient_title': _mapping.get('patient_title'),
    'address_line_1': _mapping.get('address_line_1'),
    'address_line_2': _mapping.get('address_line_2'),
    'address_line_3': _mapping.get('address_line_3'),
    'address_line_4': _mapping.get('address_line_4'),
    'address_line_5': _mapping.get('address_line_5'),
    'postcode': _mapping.get('postcode'),
    'death_indicator': _mapping.get('death_indicator'),
    'date_of_death': _mapping.get('date_of_death'),
    'registered_gp_code': _mapping.get('registered_gp_code'),
    'ethnic_code': _mapping.get('ethnic_code'),
    'home_phone': _mapping.get('home_phone'),
    'work_phone': _mapping.get('work_phone'),
    'mobile_phone': _mapping.get('mobile_phone'),
    'registered_gp': _mapping.get('registered_gp'),
    'registered_practice': _mapping.get('registered_practice')
}
del _mapping  # Don't need this anymore


def _worker_init(queue):
    """
    Initialize logging in worker process.
    Called by ProcessPoolExecutor for each worker.
    
    Args:
        queue: The shared multiprocessing.Queue from main process
    """
    AppLogger.setup_worker(queue)


def calculate_age(birth_date):
    """Calculate the current age from a date of birth.

    Args:
        birth_date (datetime | str): The date of birth used to calculate age.

    Returns:
        int: The current age derived from the date of birth.
    """
    from datetime import date, datetime
    
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


def _map_record_to_patient(record):
    """Maps a raw record list to a Patient object using configured field mapping."""
    return patientinfo.Patient(
        internal_patient_number=record[_idx['internal_patient_number']] if _idx['internal_patient_number'] is not None else "",
        assigning_authority=record[_idx['assigning_authority']] if _idx['assigning_authority'] is not None else "",
        hospital_case_number=record[_idx['hospital_case_number']] if _idx['hospital_case_number'] is not None else "",
        nhs_number=record[_idx['nhs_number']] if _idx['nhs_number'] is not None else "",
        nhs_verification_status=record[_idx['nhs_verification_status']] if _idx['nhs_verification_status'] is not None else "",
        surname=record[_idx['surname']] if _idx['surname'] is not None else "",
        forename=record[_idx['forename']] if _idx['forename'] is not None else "",
        date_of_birth=record[_idx['date_of_birth']] if _idx['date_of_birth'] is not None else "",
        sex=record[_idx['sex']] if _idx['sex'] is not None else "",
        patient_title=record[_idx['patient_title']] if _idx['patient_title'] is not None else "",
        address_line_1=record[_idx['address_line_1']] if _idx['address_line_1'] is not None else "",
        address_line_2=record[_idx['address_line_2']] if _idx['address_line_2'] is not None else "",
        address_line_3=record[_idx['address_line_3']] if _idx['address_line_3'] is not None else "",
        address_line_4=record[_idx['address_line_4']] if _idx['address_line_4'] is not None else "",
        address_line_5=record[_idx['address_line_5']] if _idx['address_line_5'] is not None else "",
        postcode=record[_idx['postcode']] if _idx['postcode'] is not None else "",
        death_indicator=record[_idx['death_indicator']] if _idx['death_indicator'] is not None else "",
        date_of_death=record[_idx['date_of_death']] if _idx['date_of_death'] is not None else "",
        registered_gp_code=record[_idx['registered_gp_code']] if _idx['registered_gp_code'] is not None else "",
        ethnic_code=record[_idx['ethnic_code']] if _idx['ethnic_code'] is not None else "",
        home_phone=record[_idx['home_phone']] if _idx['home_phone'] is not None else "",
        work_phone=record[_idx['work_phone']] if _idx['work_phone'] is not None else "",
        mobile_phone=record[_idx['mobile_phone']] if _idx['mobile_phone'] is not None else "",
        registered_gp=record[_idx['registered_gp']] if _idx['registered_gp'] is not None else "",
        registered_practice=record[_idx['registered_practice']] if _idx['registered_practice'] is not None else ""
    )


def _validate_patient(patient_info, excluded_hospital_numbers):
    """Validates patient data against business rules.
    
    Returns:
        tuple: (is_valid: bool, skip_reason: str or None)
    """
    if not patient_info.surname or not patient_info.surname.strip():
        return False, "missing required surname"
    
    if patient_info.date_of_birth and not patient_info.date_of_death and calculate_age(patient_info.date_of_birth) > 112:
        return False, "no DOD, and age > 112"
    
    if (patient_info.death_indicator == 'Y' and patient_info.date_of_death 
        and calculate_age(patient_info.date_of_death) > 2):
        return False, "dod > 2 years ago"
    
    if patient_info.hospital_case_number in excluded_hospital_numbers:
        return False, f"hospital case number {patient_info.hospital_case_number} is in exclusion list"
    
    return True, None


def process_record_batch(batch, batch_id, excluded_hospital_numbers=None):
    """Process a batch of raw patient records and convert them into HL7 messages.

    Args:
        batch (list): A list of raw records (lists of strings).
        batch_id (str): Identifier for logging and tracking.
        excluded_hospital_numbers (dict): Hospital numbers to exclude from processing.

    Returns:
        list: A list of log tuples (message, level) generated during processing.
    """
    if excluded_hospital_numbers is None:
        excluded_hospital_numbers = {}
    
    valid_messages = []
    batch_log = []
    stats = {'processed': 0, 'skipped': 0, 'errors': 0}
    
    event_type = config.get_default_event_type()
    start_time = time.time()
    batch_log.append((f"Starting to process batch {batch_id} with {len(batch)} records", "INFO"))
    
    try:
        for record_index, record in enumerate(batch, 1):
            patient_id = "UNKNOWN"
            try:
                # Basic validation
                if not isinstance(record, list):
                    batch_log.append((f"Skipping invalid record in batch {batch_id} record {record_index} - not a list", "ERROR"))
                    stats['errors'] += 1
                    continue
                
                # Strict field count validation - must be exactly 26 fields (25 data fields + 1 trailing empty from pipe)
                if len(record) != 26:
                    # Try to get patient ID from field 1 for better logging
                    try:
                        patient_id = record[1] if len(record) > 1 else "UNKNOWN"
                    except:
                        patient_id = "UNKNOWN"
                    batch_log.append((f"Skipping record in batch {batch_id} record {record_index} (Patient {patient_id}) - incorrect number of fields (found {len(record)}, expected 26)", "ERROR"))
                    stats['errors'] += 1
                    continue
                
                # Clean and map record to patient
                record = [field.strip() if isinstance(field, str) else field for field in record]
                patient_id = record[1]  # Store for exception logging
                patient_info = _map_record_to_patient(record)
                
                # Business rule validation
                is_valid, skip_reason = _validate_patient(patient_info, excluded_hospital_numbers)
                if not is_valid:
                    batch_log.append((f"Skipping patient {patient_info.internal_patient_number} in batch {batch_id} record {record_index} - {skip_reason}", "WARNING"))
                    stats['skipped'] += 1
                    continue
                
                # Generate HL7 message (fast method with fallback)
                hl7_message = hl7_utilities.create_adt_message_fast(patient_info=patient_info, event_type=event_type)
                if not hl7_message:
                    logger.log(f"Fast HL7 generation failed for patient {patient_info.internal_patient_number}. Falling back to legacy method.", "WARNING")
                    hl7_message = hl7_utilities.create_adt_message(patient_info=patient_info, event_type=event_type)
                
                if hl7_message:
                    valid_messages.append((hl7_message, patient_info))
                    stats['processed'] += 1
                else:
                    batch_log.append((f"Failed to create HL7 message for patient {patient_info.internal_patient_number} in batch {batch_id} record {record_index}", "ERROR"))
                    stats['errors'] += 1
                
            except Exception as e:
                batch_log.append((f"CRITICAL: Exception processing record in batch {batch_id} record {record_index} (Patient {patient_id}): {str(e)}\n{traceback.format_exc()}", "ERROR"))
                stats['errors'] += 1
        
        # Save valid messages
        if valid_messages:
            try:
                hl7_utilities.save_hl7_messages_batch(valid_messages, config.get_output_folder(), batch_id)
                batch_log.append((f"Successfully saved {len(valid_messages)} messages for batch {batch_id}", "INFO"))
            except Exception as e:
                batch_log.append((f"Error saving batch {batch_id}: {str(e)}\n{traceback.format_exc()}", "ERROR"))
        
        # Summary with accounting verification
        duration = time.time() - start_time
        total_accounted = stats['processed'] + stats['skipped'] + stats['errors']
        expected_count = len(batch)
        
        if total_accounted != expected_count:
            batch_log.append((
                f"⚠️ ACCOUNTING MISMATCH in batch {batch_id}: Expected {expected_count}, Accounted {total_accounted} (processed={stats['processed']}, skipped={stats['skipped']}, errors={stats['errors']}) - MISSING {expected_count - total_accounted}!",
                "CRITICAL"
            ))
        
        batch_log.append((
            f"Batch {batch_id} completed: {total_accounted}/{expected_count} records (processed={stats['processed']}, skipped={stats['skipped']}, errors={stats['errors']}) in {duration:.2f}s", 
            "INFO"
        ))
        
    except Exception as batch_error:
        batch_log.append((f"Critical error processing batch {batch_id}: {str(batch_error)}\n{traceback.format_exc()}", "CRITICAL"))
    
    return batch_log


def _read_file_batches(file_path, file_type, batch_size, encoding, pas_separator):
    """Core generator logic for reading file batches.
    
    Args:
        file_path (str): Path to the input file.
        file_type (str): 'csv' or 'pas'.
        batch_size (int): Number of records per batch.
        encoding (str): File encoding.
        pas_separator (str): Separator for PAS files.

    Yields:
        List of records
    """
    with open(file_path, newline='', encoding=encoding, errors='replace') as file:
        if file_type.lower() == "csv":
            reader = csv.reader(file)
            try:
                next(reader)  # Skip header
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
                
        else:  # PAS
            batch = []
            for line in file:
                if not line.strip():
                    continue
                try:
                    record_fields = line.strip().split(pas_separator)
                    if record_fields:
                        batch.append(record_fields)
                except Exception:
                    continue
                
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch


def get_file_reader_generator(file_path, file_type, batch_size, encoding='utf-8', pas_separator='|'):
    """Generator that lazily reads a file and yields batches of records.

    Args:
        file_path (str): Path to the input file.
        file_type (str): 'csv' or 'pas'.
        batch_size (int): Number of records per batch.
        encoding (str): File encoding.
        pas_separator (str): Separator for PAS files.

    Yields:
        list: A list of records (each record is a list of strings).
    """
    try:
        yield from _read_file_batches(file_path, file_type, batch_size, encoding, pas_separator)
    except UnicodeDecodeError:
        # Fallback to utf-8 with ignore
        yield from _read_file_batches(file_path, file_type, batch_size, 'utf-8', pas_separator)


def _process_completed_futures(done_futures):
    """Process completed futures and log their results."""
    for future in done_futures:
        try:
            batch_logs = future.result()
            for log_message, log_level in batch_logs:
                logger.log(log_message, log_level)
        except Exception:
            pass  # Handled via batch_data


def _handle_failed_batch(future, file_basename, failed_batches):
    """Handle a failed batch future."""
    if hasattr(future, 'batch_data'):
        batch, batch_id = future.batch_data
        logger.log(f"Batch {file_basename}:{batch_id} failed", "ERROR")
        failed_batches.append((batch, batch_id))
    else:
        logger.log("Unknown batch failed", "ERROR")


def _retry_failed_batches(failed_batches, file_basename, max_retries, excluded_hospital_numbers):
    """Retry failed batches with exponential backoff."""
    if not failed_batches:
        return
    
    logger.log(f"Retrying {len(failed_batches)} failed batches (Max Retries: {max_retries})", "WARNING")
    
    for _ in range(max_retries):
        if not failed_batches:
            break
        
        retry_failed = []
        for batch, batch_id in failed_batches:
            try:
                batch_logs = process_record_batch(batch, f"{file_basename}:{batch_id}_retry", excluded_hospital_numbers)
                for log_message, log_level in batch_logs:
                    logger.log(log_message, log_level)
            except Exception as e:
                logger.log(f"Retry failed for {batch_id}: {e}", "ERROR")
                retry_failed.append((batch, batch_id))
        
        failed_batches = retry_failed
    
    if failed_batches:
        logger.log(f"Final failure: {len(failed_batches)} batches could not be processed.", "ERROR")


def process_file_streaming(input_file, file_type, excluded_hospital_numbers=None):
    """Orchestrates the streaming processing of a single file using bounded parallelism.

    Args:
        input_file (str): The path to the input file.
        file_type (str): The type of the file ('csv' or 'pas').
        excluded_hospital_numbers (dict): Hospital numbers to exclude from processing.
        
    Returns:
        None
    """
    if excluded_hospital_numbers is None:
        excluded_hospital_numbers = {}
        
    if not os.path.exists(input_file):
        logger.log(f"File not found: {input_file}", "ERROR")
        return
    
    try:
        # Get config values
        batch_size = config.get_batch_size()
        num_workers = config.get_max_workers()
        max_retries = config.get_max_retries()
        pas_separator = config.get_pas_separator()
        
        # Detect encoding and start processing
        encoding = detect_encoding(input_file)
        logger.log(f"Starting processing of {input_file} (Encoding: {encoding})", "INFO")
        
        file_basename = os.path.basename(input_file)
        
        # Generator yields batches lazily
        batch_generator = get_file_reader_generator(input_file, file_type, batch_size, encoding, pas_separator)
        
        # Get the shared queue to pass to workers
        log_queue = AppLogger.get_queue()
        
        futures = set()
        MAX_PENDING_BATCHES = num_workers * 2
        batch_counter = 0
        failed_batches = []

        with ProcessPoolExecutor(max_workers=num_workers, initializer=_worker_init, initargs=(log_queue,)) as executor:
            # Submit batches with backpressure control
            for batch in batch_generator:
                batch_counter += 1
                batch_id = f"{batch_counter}"
                
                # Wait if too many tasks are pending
                if len(futures) >= MAX_PENDING_BATCHES:
                    done, futures = wait(futures, return_when=FIRST_COMPLETED)
                    _process_completed_futures(done)
                
                future = executor.submit(process_record_batch, batch, f"{file_basename}:{batch_id}", excluded_hospital_numbers)
                futures.add(future)
                future.batch_data = (batch, batch_id)
            
            # Process remaining futures
            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        batch_logs = future.result()
                        for log_message, log_level in batch_logs:
                            logger.log(log_message, log_level)
                    except Exception:
                        _handle_failed_batch(future, file_basename, failed_batches)
        
        # Retry failed batches
        _retry_failed_batches(failed_batches, file_basename, max_retries, excluded_hospital_numbers)
        logger.log(f"Completed streaming processing of {input_file}", "INFO")
        
    except Exception as e:
        logger.log(f"Failed to process {input_file}. Error: {str(e)}\n{traceback.format_exc()}", "ERROR")


def _ensure_directory(path, description, create_fallback=True):
    """Ensure a directory exists, with optional fallback to home directory.
    
    Returns:
        Path or None: The created/validated path, or None on failure
    """
    try:
        os.makedirs(path, exist_ok=True)
        logger.log(f"Ensured {description} exists: {path}", "INFO")
        return path
    except Exception as e:
        logger.log(f"Error creating {description} {path}: {e}", "ERROR")
        
        if create_fallback:
            alt_path = os.path.join(os.path.expanduser("~"), os.path.basename(path) or "hl7_output")
            try:
                os.makedirs(alt_path, exist_ok=True)
                logger.log(f"Using alternative {description}: {alt_path}", "WARNING")
                return alt_path
            except Exception as alt_e:
                logger.log(f"Failed to create alternative {description}: {alt_e}", "CRITICAL")
        
        return None


def process_files_in_folder(input_folder=None, output_folder=None, excluded_hospital_numbers=None):
    """Processes all files in the target folders using the streaming approach.
    
    Args:
        input_folder (str): Path to input folder. If None, uses config value.
        output_folder (str): Path to output folder. If None, uses config value.
        excluded_hospital_numbers (dict): Hospital numbers to exclude from processing.
    """
    if excluded_hospital_numbers is None:
        excluded_hospital_numbers = {}
    
    # Get folder paths from configuration
    input_folder = input_folder or config.get_input_folder()
    output_folder = output_folder or config.get_output_folder()
    
    # Setup output directory
    output_folder = _ensure_directory(output_folder, "output directory")
    if not output_folder:
        return
    
    # Validate input directory
    if not os.path.exists(input_folder):
        logger.log(f"Input folder not found: {input_folder}", "CRITICAL")
        _ensure_directory(input_folder, "input folder", create_fallback=False)
        logger.log(f"Created input folder: {input_folder}. Please add files and run again.", "WARNING")
        return
    
    if not os.path.isdir(input_folder):
        logger.log(f"Input folder path is not a directory: {input_folder}", "CRITICAL")
        return
    
    # Get and validate file list
    try:
        files = os.listdir(input_folder)
    except Exception as e:
        logger.log(f"Error reading input directory {input_folder}: {e}", "CRITICAL")
        return
    
    if not files:
        logger.log(f"No files found in input directory: {input_folder}", "WARNING")
        return
    
    # Log file summary
    csv_count = sum(1 for f in files if f.lower().endswith(".csv"))
    txt_count = sum(1 for f in files if f.lower().endswith(".txt"))
    logger.log(f"Found {csv_count} CSV files and {txt_count} TXT files in {input_folder}", "INFO")
    
    # Process each file
    for filename in files:
        input_file = os.path.join(input_folder, filename)
        try:
            if filename.lower().endswith(".csv"):
                logger.log(f"Processing CSV file: {filename}", "INFO")
                process_file_streaming(input_file, "csv", excluded_hospital_numbers)
            elif filename.lower().endswith(".txt"):
                logger.log(f"Processing PAS file: {filename}", "INFO")
                process_file_streaming(input_file, "PAS", excluded_hospital_numbers)
            else:
                logger.log(f"Skipping unsupported file: {filename}", "INFO")
        except Exception as e:
            logger.log(f"Error processing {filename}: {str(e)}\n{traceback.format_exc()}", "ERROR")
    
    logger.log(f"Completed processing all files in {input_folder}", "INFO")
