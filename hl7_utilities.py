from datetime import date, datetime
import time
from pathlib import Path
from hl7apy import core
import hl7apy.core
from hl7apy.core import Field
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
    

def save_hl7_messages_batch(hl7_messages, hl7_folder_path, batch_id):
    """Saves multiple HL7 messages as separate files.

    Args:
        hl7_messages (list): List of tuples (hl7_message, patient_info) to save
        hl7_folder_path (str): The folder path to save the HL7 messages
        batch_id (int or str): Batch identifier (used for logging)
    """
    if not hl7_messages:
        logger.log(f"No HL7 messages to save in batch {batch_id}", "INFO")
        return

    if not hl7_folder_path:
        logger.log(f"No folder path provided for saving HL7 messages batch {batch_id}, using 'hl7_output'", "WARNING")
        hl7_folder_path = "hl7_output"
        
    try:
        hl7_folder_path = Path(hl7_folder_path)
        # Create the base directory
        hl7_folder_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.log(f"Failed to create directory {hl7_folder_path} for batch {batch_id}: {e}", "ERROR")
        # Try a fallback location
        try:
            alt_path = Path(os.path.expanduser("~")) / "hl7_output"
            alt_path.mkdir(parents=True, exist_ok=True)
            hl7_folder_path = alt_path
            logger.log(f"Using alternative path for HL7 batch output: {alt_path}", "WARNING")
        except Exception as alt_e:
            logger.log(f"Could not create alternative directory: {alt_e}. Cannot save HL7 batch.", "CRITICAL")
            return
    
    # Add retry logic for handling temporary resource unavailability
    max_retries = 5
    retry_delay = 0.5  # Start with 0.5 seconds delay
    
    # Log the batch processing start with batch ID for better tracking
    logger.log(f"Starting to save {len(hl7_messages)} messages from batch {batch_id}", "INFO")
    
    successful_saves = 0
    failed_saves = 0
    
    # optimization: cache created directories to avoid redundant syscalls
    created_year_dirs = set()
    
    for message_tuple in hl7_messages:
        if not message_tuple or len(message_tuple) != 2:
            logger.log(f"Skipping invalid message tuple in batch {batch_id}", "WARNING")
            failed_saves += 1
            continue
            
        message, patient_info = message_tuple
        if not message:
            logger.log(f"Skipping None message in batch {batch_id}", "WARNING")
            failed_saves += 1
            continue
            
        # Get patient identifier for logging (prefer internal patient number)
        patient_identifier = getattr(patient_info, 'internal_patient_number', 'UNKNOWN') if patient_info else 'UNKNOWN'
            
        # Extract the year of birth
        try:
            year_of_birth = "unknown"
            
            if isinstance(message, str):
                # Fast parse from string
                # Find PID segment
                segments = message.split('\r')
                pid_seg = next((s for s in segments if s.startswith('PID|')), None)
                if pid_seg:
                    fields = pid_seg.split('|')
                    # PID.7 is index 7
                    if len(fields) > 7:
                        dob_val = fields[7]
                        if dob_val and len(dob_val) >= 4 and dob_val[:4].isdigit():
                            year_of_birth = dob_val[:4]
            else:
                # Old object way
                dob_field = message.pid.pid_7.to_er7()
                # Check if DOB field is empty, too short, or contains invalid characters
                if not dob_field or len(dob_field.strip()) < 4 or not dob_field[:4].isdigit():
                    year_of_birth = "unknown"
                    logger.log(f"Invalid or empty date of birth field: '{dob_field}', using 'unknown' folder", "WARNING")
                else:
                    year_of_birth = dob_field[0:4]
        except Exception as e:
            year_of_birth = "unknown"
            logger.log(f"Could not extract year of birth in batch {batch_id}: {e}", "WARNING")
        
        try:
            # Create year of birth subdirectory only if we haven't checked it yet
            year_folder_path = hl7_folder_path / year_of_birth
            if year_of_birth not in created_year_dirs:
                year_folder_path.mkdir(parents=True, exist_ok=True)
                created_year_dirs.add(year_of_birth)
        except Exception as e:
            logger.log(f"Failed to create year directory {year_of_birth} in batch {batch_id}: {e}", "ERROR")
            year_folder_path = hl7_folder_path  # Fallback to base directory
            
        # Use the shared counter in a thread-safe way for each message
        with sequence_counter.get_lock():
            sequence_counter.value += 1
            current_sequence = sequence_counter.value
            
        try:
            # Format: <YYYYMMDDHHMMSS>.<sequenceNum (8 digits)>.hl7
            hl7_file_path = year_folder_path / f"{datetime.now().strftime('%Y%m%d%H%M%S')}.{current_sequence:08d}.hl7"
        except Exception as e:
            logger.log(f"Error creating file path in batch {batch_id}: {e}. Using simplified path.", "ERROR")
            hl7_file_path = year_folder_path / f"message_{current_sequence}.hl7"
        
        save_successful = False
        for attempt in range(max_retries):
            try:
                with open(hl7_file_path, "w", newline="", encoding="utf-8") as hl7_file:
                    if isinstance(message, str):
                        # Fast path: It's already a string
                        hl7_file.write(message)
                    else:
                        # Slow path: hl7apy object
                        for child in message.children:
                            try:
                                # Write directly with CR line endings
                                segment_text = child.to_er7()
                                hl7_file.write(segment_text.replace('\r\n', '\r').replace('\n', '\r') + '\r')
                            except Exception as seg_err:
                                logger.log(f"Error writing segment {child.__class__.__name__} in batch {batch_id}: {seg_err}. Skipping.", "ERROR")
                
                # If we reach here, the file operation was successful
                logger.log(f"Successfully saved HL7 message ({current_sequence}) from batch {batch_id} for patient with internal ID {patient_identifier} to {hl7_file_path}", "INFO")
                successful_saves += 1
                save_successful = True
                break
                
            except BlockingIOError as e:
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    logger.log(f"HL7 message ({current_sequence}), Batch {batch_id}, Patient {patient_identifier}, Attempt {attempt+1}/{max_retries}: Resource temporarily unavailable for {hl7_file_path}. Retrying in {retry_delay} seconds...", "WARNING")
                    time.sleep(retry_delay)
                    # Exponential backoff - increase delay for next attempt
                    retry_delay *= 2
                else:
                    logger.log(f"HL7 message ({current_sequence}), Batch {batch_id}, Patient {patient_identifier}: Failed to save HL7 message after {max_retries} attempts: {e}", "ERROR")
            except Exception as e:
                error_trace = traceback.format_exc()
                logger.log(f"HL7 message ({current_sequence}), Batch {batch_id}, Patient {patient_identifier}: Error saving HL7 message: {e}\n{error_trace}", "ERROR")
                break
                
        if not save_successful:
            failed_saves += 1
            # Try one last emergency save with simplified approach
            try:
                emergency_path = year_folder_path / f"emergency_{current_sequence}_{int(time.time())}.hl7"
                with open(emergency_path, "w", encoding="utf-8") as emergency_file:
                    emergency_file.write(str(message))
                logger.log(f"Saved emergency version of HL7 message for patient {patient_identifier} to {emergency_path}", "WARNING")
                successful_saves += 1  # Count emergency save as successful
                failed_saves -= 1      # Remove from failed count
            except Exception as emerg_e:
                logger.log(f"Failed emergency save of HL7 message for patient {patient_identifier} in batch {batch_id}: {emerg_e}", "ERROR")
    

def sanitize_hl7_field(value):
    """Basic sanitization to replace HL7 delimiters with space. 
    Full escaping is expensive and rare for this use case."""
    if not value:
        return ""
    str_val = str(value)
    # Replace delimiters with space to avoid breaking structure
    for char in ['|', '^', '~', '&', '\r', '\n']:
        if char in str_val:
            str_val = str_val.replace(char, ' ')
    return str_val

def create_adt_message_fast(patient_info, event_type="A28"):
    """
    Creates an HL7 ADT message using fast f-string formatting.
    Bypasses hl7apy overhead for >10x speed.
    """
    try:
        # --- Prepare Data ---
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        control_id = create_control_id()
        
        sending_app = config.get_sending_application()
        sending_fac = config.get_sending_facility()
        receiving_app = config.get_receiving_application()
        receiving_fac = config.get_receiving_facility()
        proc_id = config.get_processing_id()
        version = config.get_hl7_version()
        ack_type = config.get_accept_acknowledgment_type()
        app_ack_type = config.get_application_acknowledgment_type()
        
        if not event_type:
            event_type = config.get_default_event_type()
        event_type = event_type.upper()

        # --- MSH Segment ---
        msh = (f"MSH|^~\\&|{sending_app}|{sending_fac}|{receiving_app}|{receiving_fac}|"
               f"{timestamp}||ADT^{event_type}|{control_id}|{proc_id}|{version}|||{ack_type}|{app_ack_type}")

        # --- EVN Segment ---
        evn = f"EVN|{event_type}|{timestamp}"

        # --- PID Segment ---
        # 1. Identifiers
        # PID.3 needs repetition: InternalID^^^HOSP~NHSNumber^^^NHS^NHSNO
        pat_id = sanitize_hl7_field(patient_info.internal_patient_number)
        hosp_case = sanitize_hl7_field(patient_info.hospital_case_number)
        assign_auth = sanitize_hl7_field(patient_info.assigning_authority)
        
        identifiers = f"{hosp_case}^^^{assign_auth}^HOSP" # Base identifier
        
        if hasattr(patient_info, 'nhs_number') and patient_info.nhs_number:
            nhs_num = sanitize_hl7_field(patient_info.nhs_number)
            verified = "Y" if patient_info.nhs_verification_status == "01" else "N"
            identifiers += f"~{nhs_num}^{verified}^^NHS^NHSNO"

        # 2. Name
        surname = sanitize_hl7_field(patient_info.surname)
        forename = sanitize_hl7_field(patient_info.forename)
        title = sanitize_hl7_field(patient_info.patient_title)
        # Old format used component 1,2 and 5 (Sur^For^^Title)? No, diff showed Sur^For^^^Title vs Sur^For^^Title
        # Legacy output was: Tester^Test^^^Mr (3 carets). 1=Tester, 2=Test, 3=Empty, 4=Mr.
        name = f"{surname}^{forename}^^^{title}"

        # 3. Demographics
        dob = sanitize_hl7_field(patient_info.date_of_birth)
        sex = sanitize_hl7_field(patient_info.sex)
        
        # 4. Address
        # PID.11 Repeating: Addr1^Addr2^City^State^Zip
        # Logic mimics create_pid.py:
        # 5 lines: 1, 2, 4(City), 5(State) -- 3 omitted
        # 4 lines: 1, 2, 3(City), 4(State)
        # 3 lines: 1, 2, 3(City)
        # 2 lines: 1, 2(City)
        # Else: Map to 1..5
        
        addr_str = ""
        postcode = sanitize_hl7_field(patient_info.postcode)
        
        if hasattr(patient_info, 'address') and isinstance(patient_info.address, list) and patient_info.address:
            lines = [sanitize_hl7_field(L) for L in patient_info.address]
            count = len(lines)
            
            a1 = a2 = city = state = ""
            
            if count == 5:
                a1, a2, city, state = lines[0], lines[1], lines[3], lines[4]
            elif count == 4:
                a1, a2, city, state = lines[0], lines[1], lines[2], lines[3]
            elif count == 3:
                a1, a2, city = lines[0], lines[1], lines[2]
            elif count == 2:
                a1, city = lines[0], lines[1]
            else:
                # Fallback simple mapping
                a1 = lines[0] if count > 0 else ""
                a2 = lines[1] if count > 1 else ""
                city = lines[2] if count > 2 else ""
                state = lines[3] if count > 3 else ""
            
            addr_str = f"{a1}^{a2}^{city}^{state}^{postcode}"
        else:
             addr_str = f"^^^^{postcode}"

        home_ph = sanitize_hl7_field(patient_info.home_phone)
        biz_ph = sanitize_hl7_field(getattr(patient_info, 'mobile_phone', '')) # Mapped to PID.14 in old code
        dod = sanitize_hl7_field(patient_info.date_of_death)
        death_ind = sanitize_hl7_field(patient_info.death_indicator)

        pid = (f"PID|1||{identifiers}||{name}||{dob}|{sex}|||{addr_str}||"
               f"{home_ph}|{biz_ph}|||||||||||||||{dod}|{death_ind}")

        # --- PV1 Segment (Only for A01 usually, but good to have logic ready) ---
        pv1 = ""
        if event_type == "A01":
            p_class = config.get_pv1_patient_class()
            visit_inst = config.get_pv1_visit_institution()
            att_doc_id = config.get_pv1_attending_doctor_id()
            att_doc_field = f"^{att_doc_id}" if att_doc_id else ""
            
            # Simple PV1
            pv1 = f"\rPV1|1|{p_class}|{visit_inst}||||{att_doc_field}"

        # Combine
        return f"{msh}\r{evn}\r{pid}{pv1}\r"

    except Exception as e:
        error_trace = traceback.format_exc()
        logger.log(f"Error creating fast ADT message: {e}\n{error_trace}", "ERROR")
        return None
