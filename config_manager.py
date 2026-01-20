import json
import os
from typing import Dict, Any, Optional
import multiprocessing

class ConfigManager:
    """Manages configuration settings for the CSV to HL7 converter."""
    
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self.load_config()
    
    def load_config(self, config_path: str = "config.json"):
        """Load configuration from JSON file.
        
        Args:
            config_path (str): Path to the configuration file
        """
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    self._config = json.load(f)
                print(f"Configuration loaded from {config_path}")
            else:
                print(f"Configuration file {config_path} not found, using defaults")
                self._config = self._get_default_config()
                # Create the default config file
                self.save_config(config_path)
                
        except Exception as e:
            print(f"Error loading configuration: {e}. Using defaults.")
            self._config = self._get_default_config()
    
    def save_config(self, config_path: str = "config.json"):
        """Save current configuration to JSON file.
        
        Args:
            config_path (str): Path to save the configuration file
        """
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(self._config, f, indent=2)
            print(f"Configuration saved to {config_path}")
        except Exception as e:
            print(f"Error saving configuration: {e}")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration values."""
        return {
            "directories": {
                "input_folder": "input",
                "output_folder": "output_hl7"
            },
            "hl7_settings": {
                "sending_application": "CSV2HL7_Converter",
                "sending_facility": "Data_Processing_Center", 
                "receiving_application": "Hospital_Information_System",
                "receiving_facility": "Main_Hospital",
                "default_event_type": "A28",
                "hl7_version": "2.4",
                "processing_id": "T",
                "accept_acknowledgment_type": "AL",
                "application_acknowledgment_type": "NE"
            },
            "patient_settings": {
                "assigning_authority": "RX1"
            },
            "pv1_settings": {
                "patient_class": "O",
                "patient_type": "O", 
                "visit_institution": "MAIN_HOSPITAL",
                "attending_doctor_id": "ACON",
                "attending_doctor_name": "ANAESTHETICS CONS",
                "attending_doctor_type": "L",
                "referring_doctor_name": "ANAESTHETICS CONS",
                "referring_doctor_id": "AUSHICPR"
            },
            "processing": {
                "batch_size": 1000,
                "max_workers": None,
                "max_retries": 3
            },
            "logging": {
                "log_directory": "logs",
                "log_level": "INFO"
            }
        }
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value using dot notation.
        
        Args:
            key_path (str): Dot-separated path to the configuration value (e.g., 'hl7_settings.sending_application')
            default: Default value if key is not found
            
        Returns:
            The configuration value or default if not found
        """
        try:
            keys = key_path.split('.')
            value = self._config
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key_path: str, value: Any):
        """Set configuration value using dot notation.
        
        Args:
            key_path (str): Dot-separated path to the configuration value
            value: Value to set
        """
        keys = key_path.split('.')
        config = self._config
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        config[keys[-1]] = value
    
    def get_input_folder(self) -> str:
        """Get input folder path."""
        return self.get('directories.input_folder', 'input')
    
    def get_output_folder(self) -> str:
        """Get output folder path."""
        return self.get('directories.output_folder', 'output_hl7')
    
    def get_sending_application(self) -> str:
        """Get HL7 sending application."""
        return self.get('hl7_settings.sending_application', 'CSV2HL7_Converter')
    
    def get_sending_facility(self) -> str:
        """Get HL7 sending facility."""
        return self.get('hl7_settings.sending_facility', 'Data_Processing_Center')
    
    def get_receiving_application(self) -> str:
        """Get HL7 receiving application."""
        return self.get('hl7_settings.receiving_application', 'Hospital_Information_System')
    
    def get_receiving_facility(self) -> str:
        """Get HL7 receiving facility."""
        return self.get('hl7_settings.receiving_facility', 'Main_Hospital')
    
    def get_default_event_type(self) -> str:
        """Get default HL7 event type."""
        return self.get('hl7_settings.default_event_type', 'A28')
    
    def get_hl7_version(self) -> str:
        """Get HL7 version."""
        return self.get('hl7_settings.hl7_version', '2.4')
    
    def get_processing_id(self) -> str:
        """Get HL7 processing ID."""
        return self.get('hl7_settings.processing_id', 'T')
    
    def get_accept_acknowledgment_type(self) -> str:
        """Get HL7 accept acknowledgment type."""
        return self.get('hl7_settings.accept_acknowledgment_type', 'AL')
    
    def get_application_acknowledgment_type(self) -> str:
        """Get HL7 application acknowledgment type."""
        return self.get('hl7_settings.application_acknowledgment_type', 'NE')
    
    def get_assigning_authority(self) -> str:
        """Get patient assigning authority."""
        return self.get('patient_settings.assigning_authority', 'ABC')
    
    def get_batch_size(self) -> int:
        """Get processing batch size."""
        return self.get('processing.batch_size', 1000)
    
    def get_max_workers(self) -> int:
        """Get maximum number of worker processes."""
        configured_workers = self.get('processing.max_workers')
        if configured_workers is None:
            return max(1, multiprocessing.cpu_count() - 1)
        return max(1, configured_workers)
    
    def get_max_retries(self) -> int:
        """Get maximum number of retries for failed batches."""
        return self.get('processing.max_retries', 3)
    
    def get_log_directory(self) -> str:
        """Get log directory path."""
        return self.get('logging.log_directory', 'logs')
    
    def get_log_level(self) -> str:
        """Get logging level."""
        return self.get('logging.log_level', 'INFO')
    
    def get_pv1_patient_class(self) -> str:
        """Get PV1 patient class."""
        return self.get('pv1_settings.patient_class', 'O')
    
    def get_pv1_patient_type(self) -> str:
        """Get PV1 patient type."""
        return self.get('pv1_settings.patient_type', 'O')
    
    def get_pv1_visit_institution(self) -> str:
        """Get PV1 visit institution."""
        return self.get('pv1_settings.visit_institution', 'MAIN_HOSPITAL')
    
    def get_pv1_attending_doctor_id(self) -> str:
        """Get PV1 attending doctor ID."""
        return self.get('pv1_settings.attending_doctor_id', 'ACON')
    
    def get_pv1_attending_doctor_name(self) -> str:
        """Get PV1 attending doctor name."""
        return self.get('pv1_settings.attending_doctor_name', 'ANAESTHETICS CONS')
    
    def get_pv1_attending_doctor_type(self) -> str:
        """Get PV1 attending doctor type."""
        return self.get('pv1_settings.attending_doctor_type', 'L')
    
    def get_pv1_referring_doctor_name(self) -> str:
        """Get PV1 referring doctor name."""
        return self.get('pv1_settings.referring_doctor_name', 'ANAESTHETICS CONS')
    
    def get_pv1_referring_doctor_id(self) -> str:
        """Get PV1 referring doctor ID."""
        return self.get('pv1_settings.referring_doctor_id', 'AUSHICPR')
    
    def validate_config(self) -> list:
        """Validate configuration and return list of issues found.
        
        Returns:
            List of validation error messages
        """
        issues = []
        
        # Check required string fields
        required_strings = [
            'directories.input_folder',
            'directories.output_folder',
            'hl7_settings.sending_application',
            'hl7_settings.sending_facility',
            'hl7_settings.receiving_application',
            'hl7_settings.receiving_facility',
            'patient_settings.assigning_authority',
            'pv1_settings.patient_class',
            'pv1_settings.visit_institution'
        ]
        
        for key in required_strings:
            value = self.get(key)
            if not value or not isinstance(value, str) or not value.strip():
                issues.append(f"Configuration '{key}' is missing or empty")
        
        # Check numeric fields
        batch_size = self.get('processing.batch_size')
        if not isinstance(batch_size, int) or batch_size < 1:
            issues.append("Configuration 'processing.batch_size' must be a positive integer")
        
        max_retries = self.get('processing.max_retries')
        if not isinstance(max_retries, int) or max_retries < 0:
            issues.append("Configuration 'processing.max_retries' must be a non-negative integer")
        
        # Check event type
        event_type = self.get('hl7_settings.default_event_type')
        valid_event_types = ['A01', 'A04', 'A08', 'A28', 'A31']
        if event_type not in valid_event_types:
            issues.append(f"Configuration 'hl7_settings.default_event_type' must be one of {valid_event_types}")
        
        return issues

# Global configuration instance
config = ConfigManager()