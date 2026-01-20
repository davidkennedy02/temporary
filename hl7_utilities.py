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
            # Create year of birth subdirectory
            year_folder_path = hl7_folder_path / year_of_birth
            year_folder_path.mkdir(parents=True, exist_ok=True)
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
    
    logger.log(f"Completed saving batch {batch_id}: {successful_saves} successful, {failed_saves} failed", "INFO")
