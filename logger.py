import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

class AppLogger:
    """A logger that writes logs to a daily rotating log file (app-YYYYMMDD.log)."""

    def __init__(self, log_dir="logs"):
        """Initialize the logger with a daily rotating file handler."""
        self.logger = logging.getLogger("AppLogger")
        self.logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all log levels
        
        try:
            os.makedirs(log_dir, exist_ok=True)  # Ensure the log directory exists
            log_file = os.path.join(log_dir, "app.log")  # Base filename
            
            # Create a rotating file handler (new file daily)
            handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=7)
            handler.suffix = "%Y%m%d"  # Adds YYYYMMDD to rotated log files
            handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
            
            # Avoid duplicate handlers if this logger is re-initialized
            if not self.logger.handlers:
                self.logger.addHandler(handler)
                
                # Also add console handler for immediate feedback
                console = logging.StreamHandler(sys.stdout)
                console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
                self.logger.addHandler(console)
        except Exception as e:
            # If file logging setup fails, ensure we at least have console logging
            print(f"WARNING: Failed to set up file logging: {e}. Falling back to console logging only.")
            if not self.logger.handlers:
                console = logging.StreamHandler(sys.stdout)
                console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
                self.logger.addHandler(console)
            
            # Log the failure itself
            self.log(f"Failed to set up file logging: {e}", "ERROR")

    def log(self, message, level="INFO"):
        """Log a message at the specified level."""
        log_levels = {
            "INFO": self.logger.info,
            "WARNING": self.logger.warning,
            "ERROR": self.logger.error,
            "DEBUG": self.logger.debug,
            "CRITICAL": self.logger.critical
        }
        try:
            log_levels.get(level, self.logger.info)(message)  # Default to INFO
        except Exception as e:
            # Last resort if logging itself fails
            print(f"LOGGING FAILURE: {e}. Original message: {message}")

