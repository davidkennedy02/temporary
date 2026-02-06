"""
Main entry point for CSV to HL7 conversion application.

This module orchestrates the overall process by:
- Validating configuration settings
- Initializing the logger
- Delegating file processing to the file_processor module
"""

import sys
import traceback

from logger import AppLogger
from config_manager import config
from file_processor import process_files_in_folder

# Initialize logger
logger = AppLogger()

EXCLUDED_HOSPITAL_NUMBERS = {}


if __name__ == "__main__":
    try:
        logger.log("Starting CSVtoHL7 process", "INFO")
        
        # Validate configuration
        config_issues = config.validate_config()
        if config_issues:
            logger.log("Configuration validation issues found:", "ERROR")
            for issue in config_issues:
                logger.log(f"  - {issue}", "ERROR")
            logger.log("Please fix configuration issues before proceeding", "CRITICAL")
            sys.exit(1)
        
        # Log configuration summary
        logger.log(f"Using configuration: Input='{config.get_input_folder()}', Output='{config.get_output_folder()}'", "INFO")
        logger.log(f"HL7 Settings: Sending='{config.get_sending_application()}', Receiving='{config.get_receiving_application()}'", "INFO")
        logger.log(f"Processing: Batch size={config.get_batch_size()}, Workers={config.get_max_workers()}", "INFO")
        
        # Process files using the file_processor module
        process_files_in_folder(excluded_hospital_numbers=EXCLUDED_HOSPITAL_NUMBERS)
        
        logger.log("CSVtoHL7 process completed successfully", "INFO")
        
        # CRITICAL: Ensure all logs written before exit
        AppLogger.flush_queue()
        
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.log(f"Critical error in main process: {str(e)}\n{error_trace}", "CRITICAL")
        
        # Flush logs even on error
        AppLogger.flush_queue()
        sys.exit(1)

