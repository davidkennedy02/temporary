from logger import AppLogger
from config_manager import config
import traceback

logger = AppLogger()

def create_msh(messageType, control_id, hl7, current_date):

    # Add MSH Segment
    try:
        if not hl7:
            logger.log("Cannot create MSH segment: HL7 message object is None", "ERROR")
            return None
            
        if not messageType:
            messageType = "UNKNOWN"
            logger.log("Message type not provided, using 'UNKNOWN'", "WARNING")
            
        # convert the message type to a string replacing the underscore with ^
        messageTypeSegment = str(messageType)
        messageTypeSegment = messageTypeSegment.replace("_", "^")

        hl7.msh.msh_3 = config.get_sending_application()  # Sending Application
        hl7.msh.msh_4 = config.get_sending_facility()  # Sending Facility
        hl7.msh.msh_5 = config.get_receiving_application()  # Receiving Application
        hl7.msh.msh_6 = config.get_receiving_facility()  # Receiving Facility
        
        # Protect against current_date being None
        if current_date:
            try:
                hl7.msh.msh_7 = current_date.strftime("%Y%m%d%H%M")  # Date/Time of Message
            except Exception as date_err:
                logger.log(f"Error formatting date in MSH segment: {date_err}", "WARNING")
                hl7.msh.msh_7 = "000000000000"  # Fallback to default date
        else:
            logger.log("Current date not provided for MSH segment", "WARNING")
            hl7.msh.msh_7 = "000000000000"  # Default date
            
        hl7.msh.msh_9 = messageTypeSegment  # Message Type
        hl7.msh.msh_10 = control_id if control_id else "NOID"  # Message Control ID with fallback
        hl7.msh.msh_11 = config.get_processing_id()  # Processing ID
        hl7.msh.msh_12 = config.get_hl7_version()  # Version ID
        hl7.msh.msh_15 = config.get_accept_acknowledgment_type()  # Accept Acknowledgment Type
        hl7.msh.msh_16 = config.get_application_acknowledgment_type()  # Application Acknowledgment Type
        
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.log(
            f"An error of type {type(e).__name__} occurred creating MSH segment: {str(e)}\n{error_trace}", "CRITICAL")
        return None
    else:
        return hl7