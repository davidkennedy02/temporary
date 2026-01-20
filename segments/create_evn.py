from logger import AppLogger
from segments.segment_utilities import create_obr_time
import traceback

logger = AppLogger()

def create_evn(hl7, event_type:str="A28"):
    '''Creates a EVN segment for the HL7 message - with improved error handling
    '''
    try:
        if not hl7:
            logger.log("Cannot create EVN segment: HL7 message object is None", "ERROR")
            return None

        # Validate event type
        if not event_type or not isinstance(event_type, str):
            logger.log(f"Invalid event_type ({event_type}), using default 'A28'", "WARNING")
            event_type = "A28"

        # Get request date safely
        try:
            request_date = create_obr_time()
        except Exception as date_error:
            logger.log(f"Error getting request date for EVN segment: {date_error}. Using empty value.", "WARNING")
            request_date = ""

        hl7.evn.evn_1 = event_type
        hl7.evn.evn_2 = request_date
        
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.log(
            f"An error of type {type(e).__name__} occurred creating EVN segment: {str(e)}\n{error_trace}", "CRITICAL")
        return None
    else:
        return hl7