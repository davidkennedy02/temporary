# This file contains the code to create the PID segment of the HL7 message
from logger import AppLogger
from patientinfo import Patient
import traceback
from hl7apy.core import Field

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
        
        # Dictionary mapping patient attributes to PID fields for cleaner code
        pid_mappings = [
            (lambda p: p.hospital_case_number, lambda h, v: setattr(h.pid.pid_3, 'pid_3_1', v)),
            (lambda p: p.assigning_authority, lambda h, v: setattr(h.pid.pid_3, 'pid_3_4', v)),
            (lambda p: "HOSP", lambda h, v: setattr(h.pid.pid_3, 'pid_3_5', v)),
            (lambda p: p.surname, lambda h, v: setattr(h.pid.pid_5, 'pid_5_1', v)),
            (lambda p: p.forename, lambda h, v: setattr(h.pid.pid_5, 'pid_5_2', v)),
            (lambda p: p.patient_title, lambda h, v: setattr(h.pid.pid_5, 'pid_5_5', v)),
            (lambda p: p.date_of_birth, lambda h, v: setattr(h.pid, 'pid_7', v)),
            (lambda p: p.sex, lambda h, v: setattr(h.pid, 'pid_8', v)),
        ]
        
        # Apply the base mappings for the first PID.3 instance
        for getter, setter in pid_mappings:
            try:
                value = getter(patient_info)
                if value is not None:  # Only set if not None
                    setter(hl7, value)
            except Exception as field_error:
                patient_id = getattr(patient_info, 'internal_patient_number', 'Unknown')
                logger.log(f"Error setting base PID field for patient {patient_id}: {field_error}", "WARNING")
        
        # Add additional PID.3 instances for other patient identifiers
        try:
            # Add NHS number if available
            if hasattr(patient_info, 'nhs_number') and patient_info.nhs_number:
                # Create a repetition of PID.3 field
                pid3_rep = hl7.pid.add_field("pid_3")
                pid3_rep.pid_3_1 = patient_info.nhs_number
                pid3_rep.pid_3_2 = "Y" if patient_info.nhs_verification_status == "01" else "N"
                pid3_rep.pid_3_4 = "NHS"  # NHS assigning authority
                pid3_rep.pid_3_5 = "NHSNO"  # NHS identifier type
                
        except Exception as field_error:
            patient_id = getattr(patient_info, 'internal_patient_number', 'Unknown')
            logger.log(f"Error adding additional identifiers for patient {patient_id}: {field_error}", "WARNING")
        
        # Process address fields if they exist
        addr_mappings = []
        if hasattr(patient_info, 'address') and isinstance(patient_info.address, list):
            addr_lines = patient_info.address
            addr_count = len(addr_lines)
            
            # Special case for exactly 5 address lines - omit address line 3
            if addr_count == 5:
                addr_mappings.extend([
                    # First address line goes to address line 1 (pid_11_1)
                    (lambda p: p.address[0], lambda h, v: setattr(h.pid.pid_11, 'pid_11_1', v)),
                    # Second address line goes to address line 2 (pid_11_2)
                    (lambda p: p.address[1], lambda h, v: setattr(h.pid.pid_11, 'pid_11_2', v)),
                    # Third address line is omitted
                    # Fourth address line goes to city (pid_11_3)
                    (lambda p: p.address[3], lambda h, v: setattr(h.pid.pid_11, 'pid_11_3', v)),
                    # Fifth address line goes to state (pid_11_4)
                    (lambda p: p.address[4], lambda h, v: setattr(h.pid.pid_11, 'pid_11_4', v))
                ])
            # Special case for exactly 4 address lines - map directly to the 4 fields
            elif addr_count == 4:
                addr_mappings.extend([
                    # First address line goes to address line 1 (pid_11_1)
                    (lambda p: p.address[0], lambda h, v: setattr(h.pid.pid_11, 'pid_11_1', v)),
                    # Second address line goes to address line 2 (pid_11_2)
                    (lambda p: p.address[1], lambda h, v: setattr(h.pid.pid_11, 'pid_11_2', v)),
                    # Third address line goes to city (pid_11_3)
                    (lambda p: p.address[2], lambda h, v: setattr(h.pid.pid_11, 'pid_11_3', v)),
                    # Fourth address line goes to state (pid_11_4)
                    (lambda p: p.address[3], lambda h, v: setattr(h.pid.pid_11, 'pid_11_4', v))
                ])
            # Special case for exactly 3 address lines
            elif addr_count == 3:
                addr_mappings.extend([
                    # First address line goes to address line 1 (pid_11_1)
                    (lambda p: p.address[0], lambda h, v: setattr(h.pid.pid_11, 'pid_11_1', v)),
                    # Second address line goes to address line 2 (pid_11_2)
                    (lambda p: p.address[1], lambda h, v: setattr(h.pid.pid_11, 'pid_11_2', v)),
                    # Third address line goes to city (pid_11_3)
                    (lambda p: p.address[2], lambda h, v: setattr(h.pid.pid_11, 'pid_11_3', v))
                ])
            # Special case for exactly 2 address lines
            elif addr_count == 2:
                # First address line goes to address line 1 (pid_11_1)
                addr_mappings.append(
                    (lambda p: p.address[0], lambda h, v: setattr(h.pid.pid_11, 'pid_11_1', v))
                )
                # Second address line goes to city (pid_11_3)
                addr_mappings.append(
                    (lambda p: p.address[1], lambda h, v: setattr(h.pid.pid_11, 'pid_11_3', v))
                )
            # For all other cases, map addresses as before
            else:
                for i in range(min(5, addr_count)):
                    addr_mappings.append(
                        (lambda p, idx=i: p.address[idx], lambda h, v, idx=i: setattr(h.pid.pid_11, f'pid_11_{idx+1}', v))
                    )
                
        # Add postcode and phone numbers
        final_mappings = [
            (lambda p: p.postcode, lambda h, v: setattr(h.pid.pid_11, 'pid_11_5', v)),
            (lambda p: p.home_phone, lambda h, v: setattr(h.pid, 'pid_13', v)),
            (lambda p: p.mobile_phone, lambda h, v: setattr(h.pid, 'pid_14', v)),
            (lambda p: p.date_of_death, lambda h, v: setattr(h.pid, 'pid_29', v)),
            (lambda p: p.death_indicator, lambda h, v: setattr(h.pid, 'pid_30', v)),
        ]
        
        # Combine all mappings
        all_mappings = pid_mappings + addr_mappings + final_mappings
        
        # Apply each mapping safely
        for getter, setter in all_mappings:
            try:
                value = getter(patient_info)
                if value is not None:  # Only set if not None
                    setter(hl7, value)
            except Exception as field_error:
                patient_id = getattr(patient_info, 'internal_patient_number', 'Unknown')
                logger.log(f"Error setting PID field for patient {patient_id}: {field_error}", "WARNING")
                # Continue with other fields
        
    except Exception as e:
        error_trace = traceback.format_exc()
        patient_id = getattr(patient_info, 'internal_patient_number', 'Unknown') if patient_info else 'Unknown'
        logger.log(
            f"An error of type {type(e).__name__} occurred creating PID segment for patient {patient_id}: {str(e)}\n{error_trace}", "CRITICAL")
        return None    
    else:
        return hl7