# This file contains the code to create the PID segment of the HL7 message
from logger import AppLogger
from patientinfo import Patient
import traceback

logger = AppLogger()

def create_pid(patient_info:Patient, hl7):
    """Creates a PID segment for the HL7 message, 
    requires a patient_info object and the hl7 message"""
    try:
        if not hl7:
            logger.log("Cannot create PID segment: HL7 message object is None", "ERROR")
            return None
            
        if not patient_info:
            logger.log("Cannot create PID segment: Patient info is None", "ERROR")
            return None
            
        # Set each field safely, catching any attribute errors
        hl7.pid.pid_1 = "1"
        
        patient_id = getattr(patient_info, 'internal_patient_number', 'Unknown')
        
        # Set patient identifier fields (PID.3)
        try:
            if patient_info.hospital_case_number:
                hl7.pid.pid_3.pid_3_1 = patient_info.hospital_case_number
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting hospital_case_number for patient {patient_id}: {e}", "WARNING")
        
        try:
            hl7.pid.pid_3.pid_3_4 = "HOSP"
        except Exception as e:
            logger.log(f"Error setting identifier type for patient {patient_id}: {e}", "WARNING")
        
        try:
            if patient_info.assigning_authority:
                hl7.pid.pid_3.pid_3_5 = patient_info.assigning_authority
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting assigning_authority for patient {patient_id}: {e}", "WARNING")
        
        # Set patient name fields (PID.5)
        try:
            if patient_info.surname:
                hl7.pid.pid_5.pid_5_1 = patient_info.surname
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting surname for patient {patient_id}: {e}", "WARNING")
        
        try:
            if patient_info.forename:
                hl7.pid.pid_5.pid_5_2 = patient_info.forename
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting forename for patient {patient_id}: {e}", "WARNING")
        
        try:
            if patient_info.patient_title:
                hl7.pid.pid_5.pid_5_5 = patient_info.patient_title
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting patient_title for patient {patient_id}: {e}", "WARNING")
        
        # Set date of birth (PID.7)
        try:
            if patient_info.date_of_birth:
                hl7.pid.pid_7 = patient_info.date_of_birth
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting date_of_birth for patient {patient_id}: {e}", "WARNING")
        
        # Set sex (PID.8)
        try:
            if patient_info.sex:
                hl7.pid.pid_8 = patient_info.sex
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting sex for patient {patient_id}: {e}", "WARNING")
        
        # Add additional PID.3 instances for other patient identifiers
        try:
            # Add NHS number if available
            if hasattr(patient_info, 'nhs_number') and patient_info.nhs_number:
                # Create a repetition of PID.3 field
                pid3_rep = hl7.pid.add_field("pid_3")
                pid3_rep.pid_3_1 = patient_info.nhs_number
                pid3_rep.pid_3_2 = "Y" if patient_info.nhs_verification_status == "01" else "N"
                pid3_rep.pid_3_4 = "NHSNO"  # NHS identifier type
                pid3_rep.pid_3_5 = "NHS"  # NHS assigning authority
        except Exception as field_error:
            logger.log(f"Error adding NHS number for patient {patient_id}: {field_error}", "WARNING")
        
        # Set address fields (PID.11)
        if hasattr(patient_info, 'address') and isinstance(patient_info.address, list):
            addr_lines = patient_info.address
            addr_count = len(addr_lines)
            
            try:
                if addr_count == 5:
                    # Special case: 5 lines - omit address line 3
                    if addr_lines[0]:
                        hl7.pid.pid_11.pid_11_1 = addr_lines[0]  # Address line 1
                    if addr_lines[1]:
                        hl7.pid.pid_11.pid_11_2 = addr_lines[1]  # Address line 2
                    # addr_lines[2] is omitted
                    if addr_lines[3]:
                        hl7.pid.pid_11.pid_11_3 = addr_lines[3]  # City
                    if addr_lines[4]:
                        hl7.pid.pid_11.pid_11_4 = addr_lines[4]  # State
                        
                elif addr_count == 4:
                    # Map directly to 4 fields
                    if addr_lines[0]:
                        hl7.pid.pid_11.pid_11_1 = addr_lines[0]  # Address line 1
                    if addr_lines[1]:
                        hl7.pid.pid_11.pid_11_2 = addr_lines[1]  # Address line 2
                    if addr_lines[2]:
                        hl7.pid.pid_11.pid_11_3 = addr_lines[2]  # City
                    if addr_lines[3]:
                        hl7.pid.pid_11.pid_11_4 = addr_lines[3]  # State
                        
                elif addr_count == 3:
                    # Map to 3 fields
                    if addr_lines[0]:
                        hl7.pid.pid_11.pid_11_1 = addr_lines[0]  # Address line 1
                    if addr_lines[1]:
                        hl7.pid.pid_11.pid_11_2 = addr_lines[1]  # Address line 2
                    if addr_lines[2]:
                        hl7.pid.pid_11.pid_11_3 = addr_lines[2]  # City
                        
                elif addr_count == 2:
                    # First line to address line 1, second to city
                    if addr_lines[0]:
                        hl7.pid.pid_11.pid_11_1 = addr_lines[0]  # Address line 1
                    if addr_lines[1]:
                        hl7.pid.pid_11.pid_11_3 = addr_lines[1]  # City
                        
                else:
                    # For all other cases, map up to 5 lines sequentially
                    for i in range(min(5, addr_count)):
                        if addr_lines[i]:
                            setattr(hl7.pid.pid_11, f'pid_11_{i+1}', addr_lines[i])
                            
            except Exception as e:
                logger.log(f"Error setting address fields for patient {patient_id}: {e}", "WARNING")
        
        # Set postcode (PID.11.5)
        try:
            if patient_info.postcode:
                hl7.pid.pid_11.pid_11_5 = patient_info.postcode
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting postcode for patient {patient_id}: {e}", "WARNING")
        
        # Set home phone (PID.13)
        try:
            if patient_info.home_phone:
                hl7.pid.pid_13 = patient_info.home_phone
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting home_phone for patient {patient_id}: {e}", "WARNING")
        
        # Set mobile phone (PID.14)
        try:
            if patient_info.mobile_phone:
                hl7.pid.pid_14 = patient_info.mobile_phone
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting mobile_phone for patient {patient_id}: {e}", "WARNING")
        
        # Set date of death (PID.29)
        try:
            if patient_info.date_of_death:
                hl7.pid.pid_29 = patient_info.date_of_death
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting date_of_death for patient {patient_id}: {e}", "WARNING")
        
        # Set death indicator (PID.30)
        try:
            if patient_info.death_indicator:
                hl7.pid.pid_30 = patient_info.death_indicator
        except (AttributeError, Exception) as e:
            logger.log(f"Error setting death_indicator for patient {patient_id}: {e}", "WARNING")
        
    except Exception as e:
        error_trace = traceback.format_exc()
        patient_id = getattr(patient_info, 'internal_patient_number', 'Unknown') if patient_info else 'Unknown'
        logger.log(
            f"An error of type {type(e).__name__} occurred creating PID segment for patient {patient_id}: {str(e)}\n{error_trace}", "CRITICAL")
        return None    
    else:
        return hl7