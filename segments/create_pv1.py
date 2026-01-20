from logger import AppLogger
from config_manager import config
import traceback

logger = AppLogger()

# Creates a PV1 segment for the HL7 message requires a patient_info object and the hl7 message
def create_pv1(hl7):
    try:
        if not hl7:
            logger.log("Cannot create PV1 segment: HL7 message object is None", "ERROR")
            return None
            
        # PV1.1 - Set ID (sequence number)
        hl7.pv1.pv1_1 = "1"
        
        # PV1.2 - Patient Class (I=Inpatient, O=Outpatient, E=Emergency, etc.)
        hl7.pv1.pv1_2 = config.get_pv1_patient_class()
        
        # PV1.3 - Assigned Patient Location (Point of Care^Room^Bed^Facility^Location Status^Person Location Type^Building)
        hl7.pv1.pv1_3 = config.get_pv1_visit_institution()
        
        # PV1.7 - Attending Doctor (ID^LastName^FirstName^MiddleName^Suffix^Prefix^Degree^Source Table^Assigning Authority)
        attending_doctor_id = config.get_pv1_attending_doctor_id()
        hl7.pv1.pv1_7 = f"^{attending_doctor_id}" if attending_doctor_id else ""
        
        # PV1.8 - Referring Doctor (ID^LastName^FirstName^MiddleName^Suffix^Prefix^Degree^Source Table^Assigning Authority)
        attending_doctor_name = config.get_pv1_attending_doctor_name()
        attending_doctor_type = config.get_pv1_attending_doctor_type()
        if attending_doctor_name:
            hl7.pv1.pv1_8 = f"^{attending_doctor_name}^^^^^^{attending_doctor_type}"
        
        # PV1.9 - Consulting Doctor (ID^LastName^FirstName^MiddleName^Suffix^Prefix^Degree^Source Table^Assigning Authority)
        referring_doctor_name = config.get_pv1_referring_doctor_name()
        referring_doctor_id = config.get_pv1_referring_doctor_id()
        if referring_doctor_name and referring_doctor_id:
            hl7.pv1.pv1_9 = f"^{referring_doctor_name}^^^^^^^{referring_doctor_id}"
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.log(
            f"An error of type {type(e).__name__} occurred creating PV1 segment: {str(e)}\n{error_trace}", "CRITICAL")
        return None
    else:
        return hl7