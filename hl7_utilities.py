from datetime import date, datetime
import time
from pathlib import Path
from hl7apy import core
from logger import AppLogger
from segments import create_pid, create_msh, create_evn, create_pv1
import patientinfo
import multiprocessing
import traceback
import os
from config_manager import config

# Replace the global variable with a shared counter
sequence_counter = multiprocessing.Value('i', 0)
logger = AppLogger()

def create_control_id():
    """Creates a unique control ID for the HL7 MSH segment.

    Returns:
        str: A unique control ID.
    """
    try:
        current_date_time = datetime.now()
        formatted_date_minutes_milliseconds = current_date_time.strftime("%Y%m%d%H%M%S.%f")
        control_id = formatted_date_minutes_milliseconds.replace(".", "")
        return control_id
    except Exception as e:
        logger.log(f"Error generating control ID: {e}. Using timestamp fallback.", "WARNING")
        # Fallback to timestamp if formatting fails
        return str(int(time.time()))


def create_message_header(messageType):
    """Creates an HL7 MSH segment and returns the HL7 message.

    Args:
        messageType (str): The type of HL7 message to create.

    Returns:
        hl7apy.core.Message: The HL7 message with the MSH segment.
    """
    try:
        current_date = date.today()
        control_id = create_control_id()

        if not messageType:
            default_event = config.get_default_event_type()
            messageType = f"ADT^{default_event}"  # Use configured default
            logger.log(f"Message type not provided, defaulting to ADT^{default_event}", "WARNING")

        hl7_version = config.get_hl7_version()
        try:
            if messageType == "ADT^A01":
                hl7 = core.Message("ADT_A01", version=hl7_version)
            else:
                hl7 = core.Message(version=hl7_version)
        except Exception as msg_err:
            logger.log(f"Failed to create HL7 message object: {msg_err}. Trying generic message.", "WARNING")
            try:
                hl7 = core.Message(version=hl7_version)  # Fallback to generic message
            except Exception as fallback_err:
                logger.log(f"Failed to create generic HL7 message: {fallback_err}", "CRITICAL")
                return None

        hl7 = create_msh.create_msh(messageType, control_id, hl7, current_date)
        return hl7
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.log(f"Error creating message header: {e}\n{error_trace}", "CRITICAL")
        return None
        

def create_adt_message(patient_info:patientinfo.Patient, event_type:str="A28"):
    """Creates an HL7 ADT message with the specified event type.

    Args:
        patient_info (patientinfo.Patient): The patient information.
        event_type (str): The event type for the HL7 message.

    Returns:
        hl7apy.core.Message: The HL7 ADT message.
    """
    try:
        if not patient_info:
            logger.log("Cannot create ADT message: Patient info is None", "ERROR")
            return None
            
        # Default event type if none provided
        if not event_type:
            event_type = config.get_default_event_type()
            logger.log(f"No event type provided, defaulting to {event_type}", "WARNING")
        
        event_type = str(event_type).upper()  # Convert the event type to upper case

        # Construction is halted if return values is None 
        hl7 = create_message_header("ADT^" + event_type)
        if not hl7:
            logger.log("Failed to create message header, cannot proceed with ADT message", "ERROR")
            return None
            
        hl7 = create_evn.create_evn(hl7, event_type=event_type) if hl7 else None
        if not hl7:
            logger.log("Failed to create EVN segment, cannot proceed with ADT message", "ERROR")
            return None
            
        hl7 = create_pid.create_pid(patient_info, hl7) if hl7 else None
        if not hl7:
            logger.log(f"Failed to create PID segment for patient {patient_info.internal_patient_number}", "ERROR")
            return None
            
        if event_type == "A01":
            hl7 = create_pv1.create_pv1(hl7) if hl7 else None
            if not hl7:
                logger.log(f"Failed to create PV1 segment for patient {patient_info.internal_patient_number}", "ERROR")
                return None
                
        return hl7
    except Exception as e:
        error_trace = traceback.format_exc()
        patient_id = getattr(patient_info, 'internal_patient_number', 'Unknown') if patient_info else 'Unknown'
        logger.log(f"Error creating ADT message for patient {patient_id}: {e}\n{error_trace}", "CRITICAL")
        return None
    

def _setup_output_directory(base_path, batch_id):
    """Sets up the output directory with fallback handling.
    
    Args:
        base_path (str): The desired base path for output
        batch_id (int or str): Batch identifier for logging
        
    Returns:
        Path: The created output directory path, or None if all attempts fail
    """
    if not base_path:
        logger.log(f"No folder path provided for batch {batch_id}, using 'hl7_output'", "WARNING")
        base_path = "hl7_output"
        
    try:
        output_path = Path(base_path)
        output_path.mkdir(parents=True, exist_ok=True)
        return output_path
    except Exception as e:
        logger.log(f"Failed to create directory {base_path} for batch {batch_id}: {e}", "ERROR")
        # Try fallback location
        try:
            alt_path = Path(os.path.expanduser("~")) / "hl7_output"
            alt_path.mkdir(parents=True, exist_ok=True)
            logger.log(f"Using alternative path for HL7 batch output: {alt_path}", "WARNING")
            return alt_path
        except Exception as alt_e:
            logger.log(f"Could not create alternative directory: {alt_e}. Cannot save HL7 batch.", "CRITICAL")
            return None


def _extract_year_from_message(message, batch_id) -> str:
    """Extracts the year of birth from an HL7 message.
    
    Args:
        message: HL7 message (string or hl7apy object)
        batch_id (int or str): Batch identifier for logging
        
    Returns:
        str: Four-digit year or 'unknown'
    """
    try:
        if isinstance(message, str):
            # Fast parse from string - find PID segment
            segments = message.split('\r')
            pid_seg = next((s for s in segments if s.startswith('PID|')), None)
            if pid_seg:
                fields = pid_seg.split('|')
                if len(fields) > 7:  # PID.7 is index 7
                    dob_val = fields[7]
                    if dob_val and len(dob_val) >= 4 and dob_val[:4].isdigit():
                        return dob_val[:4]
        else:
            # hl7apy object way
            dob_field = message.pid.pid_7.to_er7()
            if dob_field and len(dob_field.strip()) >= 4 and dob_field[:4].isdigit():
                return dob_field[0:4]
            else:
                logger.log(f"Invalid or empty date of birth field: '{dob_field}', using 'unknown' folder", "WARNING")
    except Exception as e:
        logger.log(f"Could not extract year of birth in batch {batch_id}: {e}", "WARNING")
    
    return "unknown"


def _save_single_hl7_file(message, file_path:Path, patient_identifier:str, batch_id, sequence_num:int, max_retries:int=5) -> bool:
    """Saves a single HL7 message to file with retry logic.
    
    Args:
        message: HL7 message to save (string or hl7apy object)
        file_path (Path): Path to save the file
        patient_identifier (str): Patient identifier for logging
        batch_id (int or str): Batch identifier for logging
        sequence_num (int): Message sequence number
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        bool: True if save was successful, False otherwise
    """
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            with open(file_path, "w", newline="", encoding="utf-8") as hl7_file:
                if isinstance(message, str):
                    hl7_file.write(message)
                else:
                    # hl7apy object - write segment by segment
                    for child in message.children:
                        try:
                            segment_text = child.to_er7()
                            hl7_file.write(segment_text.replace('\r\n', '\r').replace('\n', '\r') + '\r')
                        except Exception as seg_err:
                            logger.log(f"Error writing segment {child.__class__.__name__} in batch {batch_id}: {seg_err}. Skipping.", "ERROR")
            
            logger.log(f"Successfully saved HL7 message ({sequence_num}) from batch {batch_id} for patient {patient_identifier} to {file_path}", "DEBUG")
            return True
            
        except BlockingIOError as e:
            if attempt < max_retries - 1:
                logger.log(f"HL7 message ({sequence_num}), Batch {batch_id}, Patient {patient_identifier}, Attempt {attempt+1}/{max_retries}: Resource temporarily unavailable. Retrying in {retry_delay}s...", "WARNING")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.log(f"HL7 message ({sequence_num}), Batch {batch_id}, Patient {patient_identifier}: Failed after {max_retries} attempts: {e}", "ERROR")
        except Exception as e:
            error_trace = traceback.format_exc()
            logger.log(f"HL7 message ({sequence_num}), Batch {batch_id}, Patient {patient_identifier}: Error saving: {e}\n{error_trace}", "ERROR")
            break
    
    return False


def save_hl7_messages_batch(hl7_messages:list, hl7_folder_path:str, batch_id) -> None:
    """Saves multiple HL7 messages as separate files.

    Args:
        hl7_messages (list): List of tuples (hl7_message, patient_info) to save
        hl7_folder_path (str): The folder path to save the HL7 messages
        batch_id (int or str): Batch identifier (used for logging)
    Returns:
        None
    """
    if not hl7_messages:
        logger.log(f"No HL7 messages to save in batch {batch_id}", "INFO")
        return

    # Setup output directory
    hl7_folder_path = _setup_output_directory(hl7_folder_path, batch_id)
    if not hl7_folder_path:
        return
    
    logger.log(f"Starting to save {len(hl7_messages)} messages from batch {batch_id}", "INFO")
    
    successful_saves = 0
    failed_saves = 0
    created_year_dirs = set()  # Cache to avoid redundant directory creation
    
    for message_tuple in hl7_messages:
        # Validate message tuple
        if not message_tuple or len(message_tuple) != 2:
            logger.log(f"Skipping invalid message tuple in batch {batch_id}", "WARNING")
            failed_saves += 1
            continue
            
        message, patient_info = message_tuple
        if not message:
            logger.log(f"Skipping None message in batch {batch_id}", "WARNING")
            failed_saves += 1
            continue
        
        # Extract patient identifier
        patient_identifier = getattr(patient_info, 'internal_patient_number', 'UNKNOWN') if patient_info else 'UNKNOWN'
        
        # Extract year of birth and setup year subdirectory
        year_of_birth = _extract_year_from_message(message, batch_id)
        year_folder_path = hl7_folder_path / year_of_birth
        
        if year_of_birth not in created_year_dirs:
            try:
                year_folder_path.mkdir(parents=True, exist_ok=True)
                created_year_dirs.add(year_of_birth)
            except Exception as e:
                logger.log(f"Failed to create year directory {year_of_birth} in batch {batch_id}: {e}", "ERROR")
                year_folder_path = hl7_folder_path  # Fallback to base directory
        
        # Generate unique sequence number
        with sequence_counter.get_lock():
            sequence_counter.value += 1
            current_sequence = sequence_counter.value
        
        # Create file path
        try:
            hl7_file_path = year_folder_path / f"{datetime.now().strftime('%Y%m%d%H%M%S')}.{current_sequence:08d}.hl7"
        except Exception as e:
            logger.log(f"Error creating file path in batch {batch_id}: {e}. Using simplified path.", "ERROR")
            hl7_file_path = year_folder_path / f"message_{current_sequence}.hl7"
        
        # Save the file
        save_successful = _save_single_hl7_file(message, hl7_file_path, patient_identifier, batch_id, current_sequence)
        
        if save_successful:
            successful_saves += 1
        else:
            failed_saves += 1
            # Emergency save attempt
            try:
                emergency_path = year_folder_path / f"emergency_{current_sequence}_{int(time.time())}.hl7"
                with open(emergency_path, "w", encoding="utf-8") as emergency_file:
                    emergency_file.write(str(message))
                logger.log(f"Saved emergency version of HL7 message for patient {patient_identifier} to {emergency_path}", "WARNING")
                successful_saves += 1
                failed_saves -= 1
            except Exception as emerg_e:
                logger.log(f"Failed emergency save for patient {patient_identifier} in batch {batch_id}: {emerg_e}", "ERROR")
    

def sanitize_hl7_field(value: str) -> str:
    """Basic sanitization to replace HL7 delimiters with space. 
    Full escaping is expensive and rare for this use case.
    
    Args:
        value (str): The field value to sanitize
    Returns:
        str: Sanitized field value
    """
    if not value:
        return ""
    str_val = str(value)
    # Replace delimiters with space to avoid breaking structure
    for char in ['|', '^', '~', '&', '\r', '\n']:
        if char in str_val:
            str_val = str_val.replace(char, ' ')
    return str_val


def create_adt_message_fast(patient_info: patientinfo.Patient, event_type="A28"):
    """
    Creates an HL7 ADT message using fast f-string formatting.
    Bypasses hl7apy overhead for >10x speed.

    Args:
        patient_info (patientinfo.Patient): The patient information.
        event_type (str): The event type for the HL7 message (e.g., "A01", "A28").
    Returns:
        str: The HL7 ADT message as a string, or None if an error occurs.
    """
    
    def _build_msh(event_type: str, timestamp: str, control_id: str) -> str:
        """Builds the MSH (Message Header) segment."""
        sending_app = config.get_sending_application()
        sending_fac = config.get_sending_facility()
        receiving_app = config.get_receiving_application()
        receiving_fac = config.get_receiving_facility()
        proc_id = config.get_processing_id()
        version = config.get_hl7_version()
        ack_type = config.get_accept_acknowledgment_type()
        app_ack_type = config.get_application_acknowledgment_type()
        
        return (f"MSH|^~\\&|{sending_app}|{sending_fac}|{receiving_app}|{receiving_fac}|"
                f"{timestamp}||ADT^{event_type}|{control_id}|{proc_id}|{version}|||{ack_type}|{app_ack_type}")
    
    def _build_evn(event_type: str, timestamp: str) -> str:
        """Builds the EVN (Event Type) segment."""
        return f"EVN|{event_type}|{timestamp}"
    
    def _build_pid() -> str:
        """Builds the PID (Patient Identification) segment."""
        # 1. Identifiers - PID.3 needs repetition: InternalID^^^HOSP^ASS_AUTH~NHSNumber^Y^^NHSNO^NHS
        hosp_case = sanitize_hl7_field(patient_info.hospital_case_number)
        assign_auth = sanitize_hl7_field(patient_info.assigning_authority)
        identifiers = f"{hosp_case}^^^HOSP^{assign_auth}"
        
        if hasattr(patient_info, 'nhs_number') and patient_info.nhs_number:
            nhs_num = sanitize_hl7_field(patient_info.nhs_number)
            verified = "Y" if patient_info.nhs_verification_status == "01" else "N"
            identifiers += f"~{nhs_num}^{verified}^^NHSNO^NHS"
        
        # 2. Name
        surname = sanitize_hl7_field(patient_info.surname)
        forename = sanitize_hl7_field(patient_info.forename)
        title = sanitize_hl7_field(patient_info.patient_title)
        name = f"{surname}^{forename}^^^{title}"
        
        # 3. Demographics
        dob = sanitize_hl7_field(patient_info.date_of_birth)
        sex = sanitize_hl7_field(patient_info.sex)
        
        # 4. Address - PID.11 Repeating: Addr1^Addr2^City^State^Zip
        postcode = sanitize_hl7_field(patient_info.postcode)
        addr_str = f"^^^^{postcode}"  # Default
        
        if hasattr(patient_info, 'address') and isinstance(patient_info.address, list) and patient_info.address:
            lines = [sanitize_hl7_field(L) for L in patient_info.address]
            count = len(lines)
            a1 = a2 = city = state = ""
            
            # Logic mimics create_pid.py
            if count == 5:
                a1, a2, city, state = lines[0], lines[1], lines[3], lines[4]
            elif count == 4:
                a1, a2, city, state = lines[0], lines[1], lines[2], lines[3]
            elif count == 3:
                a1, a2, city = lines[0], lines[1], lines[2]
            elif count == 2:
                a1, city = lines[0], lines[1]
            else:
                a1 = lines[0] if count > 0 else ""
                a2 = lines[1] if count > 1 else ""
                city = lines[2] if count > 2 else ""
                state = lines[3] if count > 3 else ""
            
            addr_str = f"{a1}^{a2}^{city}^{state}^{postcode}"
        
        # 5. Phone numbers and death info
        home_ph = sanitize_hl7_field(patient_info.home_phone)
        business_ph = sanitize_hl7_field(getattr(patient_info, 'mobile_phone', ''))
        dod = sanitize_hl7_field(patient_info.date_of_death)
        death_ind = sanitize_hl7_field(patient_info.death_indicator)
        
        return (f"PID|1||{identifiers}||{name}||{dob}|{sex}|||{addr_str}||"
                f"{home_ph}|{business_ph}|||||||||||||||{dod}|{death_ind}")
    
    def _build_pv1() -> str:
        """Builds the PV1 (Patient Visit) segment."""
        p_class = config.get_pv1_patient_class()
        visit_inst = config.get_pv1_visit_institution()
        att_doc_id = config.get_pv1_attending_doctor_id()
        att_doc_field = f"^{att_doc_id}" if att_doc_id else ""
        
        return f"\rPV1|1|{p_class}|{visit_inst}||||{att_doc_field}"
    
    try:
        # Prepare common data
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        control_id = create_control_id()
        
        if not event_type:
            event_type = config.get_default_event_type()
        event_type = event_type.upper()
        
        # Build segments
        msh = _build_msh(event_type, timestamp, control_id)
        evn = _build_evn(event_type, timestamp)
        pid = _build_pid()
        pv1 = _build_pv1() if event_type == "A01" else ""
        
        # Combine and return
        return f"{msh}\r{evn}\r{pid}{pv1}\r"
        
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.log(f"Error creating fast ADT message: {e}\n{error_trace}", "ERROR")
        return None
