import datetime
from typing import List
from logger import AppLogger
from config_manager import config
import traceback

logger = AppLogger()

class Patient:
    """A class used to read in, validate, modify, and store patient information from a CSV file. 
    """
    def __init__(self, internal_patient_number, assigning_authority, hospital_case_number, nhs_number,
                 nhs_verification_status, surname, forename, date_of_birth, sex, patient_title, 
                 address_line_1, address_line_2, address_line_3, address_line_4, address_line_5, 
                 postcode, death_indicator, date_of_death, registered_gp_code, ethnic_code, 
                 home_phone, work_phone, mobile_phone, registered_gp, registered_practice):
        """Initializes a Patient object with the provided information.

        Args:
            internal_patient_number (str): The internal patient number.
            assigning_authority (str): The assigning authority.
            hospital_case_number (str): The hospital case number.
            nhs_number (str): The NHS number.
            nhs_verification_status (str): The NHS verification status.
            surname (str): The patient's surname.
            forename (str): The patient's forename.
            date_of_birth (str): The patient's date of birth in YYYYMMDD format.
            sex (str): The patient's sex.
            patient_title (str): The patient's title.
            address_line_1 (str): The first line of the patient's address.
            address_line_2 (str): The second line of the patient's address.
            address_line_3 (str): The third line of the patient's address.
            address_line_4 (str): The fourth line of the patient's address.
            address_line_5 (str): The fifth line of the patient's address.
            postcode (str): The patient's postcode.
            death_indicator (str): The death indicator ('Y' or 'N').
            date_of_death (str): The patient's date of death in YYYYMMDD format.
            registered_gp_code (str): The registered GP code.
            ethnic_code (str): The ethnic code.
            home_phone (str): The home phone number.
            work_phone (str): The work phone number.
            mobile_phone (str): The mobile phone number.
            registered_gp (str): The registered GP.
            registered_practice (str): The registered practice.
        """
        try:
            # Generate a placeholder for internal_patient_number if not provided
            if not internal_patient_number:
                internal_patient_number = f"UNK{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}"
                logger.log(f"Generated placeholder internal patient number: {internal_patient_number}", "INFO")
                
            logger.log(f"Initializing Patient with internal_patient_number: {internal_patient_number}", "INFO")
            
            self.internal_patient_number = self.validate_length(internal_patient_number, 12)
            self.assigning_authority = config.get_assigning_authority()  # From configuration
            self.hospital_case_number = self.validate_hospital_case_number(hospital_case_number)
            self.nhs_number = self.validate_nhs_number(nhs_number)
            self.nhs_verification_status = self.validate_length(nhs_verification_status, 2)
            self.surname = self.validate_length(surname, 30) if surname else None
            self.forename = self.validate_length(forename, 20)
            self.date_of_birth = self.parse_date(date_of_birth, "Date of birth")
            self.sex = self.map_sex(sex)
            
            # Do not have access to these lookups - for now, have truncated to 8 characters max - NEEDS FIXING
            self.patient_title = self.validate_length(patient_title, 8)
            
            # Safely handle address formatting
            address_list = [address_line_1, address_line_2, address_line_3, address_line_4, address_line_5]
            try:
                self.address = self.format_address(address_list, 50)
            except Exception as addr_err:
                logger.log(f"Error formatting address for patient {self.internal_patient_number}: {addr_err}. Using empty address.", "WARNING")
                self.address = ["", "", "", "", ""]
                
            self.postcode = self.validate_length(postcode, 10).upper() if postcode else None
            self.death_indicator = self.parse_death_indicator(death_indicator)
            self.date_of_death = self.parse_date(date_of_death, "Date of death")
            self.registered_gp_code = self.validate_length(registered_gp_code, 8)
            
            # Do not have access to these lookups - for now, have truncated to 2 characters max - NEEDS FIXING
            self.ethnic_code = self.validate_length(ethnic_code, 2)
            
            # Safely validate phone numbers
            try:
                self.home_phone = self.validate_phone(home_phone)
            except Exception as hp_err:
                logger.log(f"Error validating home phone for patient {self.internal_patient_number}: {hp_err}. Setting to None.", "WARNING")
                self.home_phone = None
                
            # Leave blank
            self.work_phone = ""
            
            try:
                self.mobile_phone = self.validate_phone(mobile_phone)
            except Exception as mp_err:
                logger.log(f"Error validating mobile phone for patient {self.internal_patient_number}: {mp_err}. Setting to None.", "WARNING")
                self.mobile_phone = None
                
            self.registered_gp = registered_gp[:50] if registered_gp else None  # Store description, not code
            
            # Note:
            '''Must exist on the WPE Source table (Source.Source_Code). If it does not exist, 
            load the patient record but do not load this code.
            '''
            # Do not have access to these lookups - for now, have truncated to 10 characters max - NEEDS FIXING
            self.registered_practice = registered_practice[:10] if registered_practice else None  # Must exist in external table

            # Perform additional validation
            if self.date_of_death: 
                self.validate_date_of_death()
                
            # Ensure minimum viable patient data
            self.ensure_minimum_data()
                
            logger.log(f"Patient initialized: {self}", "INFO")
        except Exception as e:
            error_trace = traceback.format_exc()
            logger.log(f"Error initializing patient: {e}\n{error_trace}", "ERROR")
            # Ensure we still have a valid patient object with at least minimal data
            self.internal_patient_number = internal_patient_number or f"ERROR{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.surname = surname  # Keep original value (could be None)
            self.forename = forename or "PATIENT"
            self.date_of_birth = date_of_birth or "19000101"
            self.sex = "U"  # Default to Unknown
            self.address = ["", "", "", "", ""]  # Empty address
            
            # Log that we're creating a minimal patient record
            logger.log(f"Created minimal patient record due to initialization error: {self.internal_patient_number}", "WARNING")

    def ensure_minimum_data(self):
        """Ensures the patient has minimum required data for processing."""
        # Note: surname validation is now handled in main.py processing logic
        # We don't automatically assign placeholders here anymore
            
        # If no date of birth, add placeholder (1970-01-01)
        if not self.date_of_birth:
            self.date_of_birth = "19700101"
            logger.log(f"Patient {self.internal_patient_number} has no date of birth, using placeholder 1970-01-01", "WARNING")
            
        # Ensure sex is valid
        if not self.sex or self.sex not in ['M', 'F', 'U']:
            self.sex = 'U'
            logger.log(f"Patient {self.internal_patient_number} has invalid sex, using 'U'", "WARNING")
            
        # Initialize empty address array if needed
        if not hasattr(self, 'address') or not self.address:
            self.address = ["", "", "", "", ""]
            logger.log(f"Patient {self.internal_patient_number} has no address, using empty address", "WARNING")

    @staticmethod
    def validate_length(value:str, max_length:int):
        """Ensure field does not exceed max_length.

        Args:
            value (str): The value to validate.
            max_length (int): The maximum allowed length.

        Returns:
            str: The validated value.
        """
        if value is None:
            return None
        
        # Handle non-string values safely
        try:
            value_str = str(value).strip()
            return value_str[:max_length] if value_str else None
        except Exception as e:
            logger.log(f"Error validating length of value {type(value)}: {e}. Converting to empty string.", "WARNING")
            return ""

    def validate_hospital_case_number(self, value:str):
        """Ensure hospital case number is less than or equal to 25 characters.
        Added logging to 'flag them up somehow as Data Quality will need to investigate'.

        Args:
            value (str): The hospital case number.

        Returns:
            str: The validated hospital case number.
        """
        if value is None:
            return None
            
        try:
            value_str = str(value).strip()
            if value_str:
                if len(value_str) > 25:
                    logger.log(f"Patient internal number {self.internal_patient_number}: " \
                               f"Hospital number / case note number {value_str} over 25 chars - notify Data Quality team", "ERROR")
                return value_str[:25]
            return None
        except Exception as e:
            logger.log(f"Error validating hospital case number {value}: {e}. Setting to None.", "WARNING")
            return None

    def validate_nhs_number(self, value:str):
        """Ensure NHS number is less than or equal to 10 characters.
        Added logging to 'flag them up somehow as Data Quality will need to investigate'.

        Args:
            value (str): The NHS number.

        Returns:
            str: The validated NHS number.
        """
        if value is None:
            return None
            
        try:
            value_str = str(value).strip()
            
            # Handle "NULL" string
            if value_str == "NULL":
                return None
                
            if value_str:
                if not value_str.isdigit():
                    logger.log(f"Patient internal number {self.internal_patient_number}: " \
                               f"NHS number {value_str} contains non-numeric characters - notify Data Quality team", "ERROR")
                if len(value_str) > 10:
                    logger.log(f"Patient internal number {self.internal_patient_number}: " \
                               f"NHS number {value_str} over 10 chars - notify Data Quality team", "ERROR")
                return value_str[:10]
            return None
        except Exception as e:
            logger.log(f"Error validating NHS number {value}: {e}. Setting to None.", "WARNING")
            return None 

    def parse_date(self, date_str:str, field:str="unknown"):
        """Checks for correct date formatting: YYYYMMDD
        Added logging to 'flag them up somehow as Data Quality will need to investigate'.

        Args:
            date_str (str): The date string to parse.
            field (str): The field name for logging purposes.

        Returns:
            str: The parsed date string or None if invalid.
        """
        try:
            # NULL, so ignore 
            if not date_str or date_str == "NULL":
                return None
            
            # Clean up the date string
            cleaned_date = str(date_str).strip()
            
            # Handle "YYYY-MM-DD HH:MM:SS.mmm" format (ignore time)
            if ' ' in cleaned_date:
                cleaned_date = cleaned_date.split(' ')[0]
            
            # Remove hyphens if present (YYYY-MM-DD -> YYYYMMDD)
            cleaned_date = cleaned_date.replace('-', '')
                
            # Try to parse the date to validate format
            datetime.datetime.strptime(cleaned_date, "%Y%m%d")
            return cleaned_date
            
        except ValueError:
            if date_str:
                logger.log(f"Patient internal number {self.internal_patient_number} in field {field}: " \
                           f"Invalid date {date_str} - notify Data Quality team", "ERROR")
            return None
        except Exception as e:
            logger.log(f"Error parsing date {date_str} for field {field}: {e}. Setting to None.", "WARNING")
            return None
    
    @staticmethod
    def validate_postcode(postcode:str):
        """Ensure postcode is less than or equal to 10 digits, and capitalised.

        Args:
            postcode (str): The postcode to validate.

        Returns:
            str: The validated postcode.
        """
        if postcode is None:
            return None
            
        try:
            postcode_str = str(postcode).strip()
            return postcode_str[:10].upper() if postcode_str else None
        except Exception as e:
            logger.log(f"Error validating postcode {postcode}: {e}. Setting to None.", "WARNING")
            return None
        
    @staticmethod
    def parse_death_indicator(value:any):
        """Convert 'Y'/'N' string to a boolean.
        According to doc: 'Y' if DEATH_DTTM contains a value, otherwise 'N'.

        Args:
            value (any): The death indicator value.

        Returns:
            str: 'Y' if value is truthy, otherwise 'N'.
        """
        try:
            if isinstance(value, str) and value.upper() == 'N':
                return 'N'
            if value and str(value).upper() != 'NULL':
                return 'Y'
            return 'N'
        except Exception as e:
            logger.log(f"Error parsing death indicator {value}: {e}. Defaulting to 'N'.", "WARNING")
            return 'N'

    @staticmethod
    def validate_phone(phone:str):
        """Ensure phone number contains only digits and length <= 20.

        Args:
            phone (str): The phone number to validate.

        Returns:
            str: The validated phone number or None if invalid.
        """
        if not phone:
            return None
            
        try:
            phone_str = str(phone).strip()
            if not phone_str or phone_str == "NULL":
                return None
                
            # Remove common phone formatting characters
            clean_phone = ''.join(c for c in phone_str if c.isdigit())
            
            # If after cleaning, we have no digits, return None
            if not clean_phone:
                return None
                
            return clean_phone[:20]  # Limit to 20 digits
        except Exception as e:
            logger.log(f"Error validating phone number {phone}: {e}. Setting to None.", "WARNING")
            return None

    @staticmethod
    def format_address(address_list:List[str], max_length:int):
        """Format address fields, ensuring valid length.

        Args:
            address_list (list[str]): The list of address lines.
            max_length (int): The maximum allowed length for each address line.

        Returns:
            list[str]: The formatted address list.
        """
        result = []
        for i in range(5):  # Ensure we always have 5 address lines
            try:
                if i < len(address_list) and address_list[i] and address_list[i] != "NULL":
                    line = str(address_list[i])
                    line = " ".join(line.split())  # Remove excess whitespace between words
                    result.append(line[:max_length])  # Truncate line to maximum length
                else:
                    result.append("")  # Empty line if none provided
            except Exception as e:
                logger.log(f"Error formatting address line {i+1}: {e}. Using empty line.", "WARNING")
                result.append("")
        return result
    
    @staticmethod
    def map_sex(value:str):
        """Map '1' to 'M', '2' to 'F', any other value to 'U'.

        Args:
            value (str): The sex value to map.

        Returns:
            str: 'M', 'F', or 'U' based on the input value.
        """
        try:
            if not value:
                return 'U'
                
            value_str = str(value).strip().upper()
            
            if value_str == '1' or value_str == 'M' or value_str == 'MALE':
                return 'M'
            elif value_str == '2' or value_str == 'F' or value_str == 'FEMALE':
                return 'F'
            return 'U'
        except Exception as e:
            logger.log(f"Error mapping sex value {value}: {e}. Defaulting to 'U'.", "WARNING")
            return 'U'
    
    def validate_date_of_death(self):
        """Ensure the date of death does not occur before the date of birth, and if a date of death is recorded, ensure that
        the death indicator is set to 'Y'.
        """
        try:
            if not self.date_of_death or not self.date_of_birth:
                return
                
            dod = datetime.datetime.strptime(self.date_of_death, "%Y%m%d")
            dob = datetime.datetime.strptime(self.date_of_birth, "%Y%m%d")
            
            if dod < dob:
                logger.log(f"Patient internal number {self.internal_patient_number}: " \
                           f"Date of death {self.date_of_death} is earlier than date of birth {self.date_of_birth}", "ERROR")
                # Correct the error by setting date of death to None
                self.date_of_death = None
                logger.log(f"Reset invalid date of death to None for patient {self.internal_patient_number}", "WARNING")
                
        except TypeError as e:
            logger.log(f"Patient internal number {self.internal_patient_number}: "\
                f"Date of death type error: {e}", "ERROR")
        except ValueError as e:
            logger.log(f"Patient internal number {self.internal_patient_number}: "\
                f"Date format error in death validation: {e}", "ERROR")
        except Exception as e:
            logger.log(f"Patient internal number {self.internal_patient_number}: "\
                f"Unexpected error in death validation: {e}", "ERROR")

        # Ensure death indicator is consistent with date of death
        if self.death_indicator == "N" and self.date_of_death:
            logger.log(f"Patient internal number {self.internal_patient_number}: " \
                  f"Death indicator is 'N' but a date of death {self.date_of_death} has been recorded", "WARNING")
            # Auto-correct the inconsistency
            self.death_indicator = "Y"
            logger.log(f"Auto-corrected death indicator to 'Y' for patient {self.internal_patient_number}", "INFO")

    def __str__(self):
        """Safe string representation that handles potential encoding issues."""
        try:
            name_part = f"{self.forename} {self.surname}" if self.forename and self.surname else "Unknown"
            dob_part = f"DOB: {self.date_of_birth}" if self.date_of_birth else "No DOB"
            nhs_part = f"NHS: {self.nhs_number}" if self.nhs_number else "No NHS"
            internal_id = f"ID: {self.internal_patient_number}" if self.internal_patient_number else "Unknown"
            
            return f"Patient({name_part}, {dob_part}, {nhs_part}, {internal_id})"
        except Exception:
            # Provide a fallback representation that won't cause encoding issues
            return f"Patient(ID:{self.internal_patient_number or 'Unknown'})"

