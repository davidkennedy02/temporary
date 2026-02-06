import logging
import os
import atexit
import glob
from logging.handlers import TimedRotatingFileHandler
from multiprocessing import current_process
from config_manager import config


class AppLogger:
    """
    Multiprocessing-safe logger using parallel write with auto-merge strategy.
    
    Architecture:
    - Main process: Writes to app.log
    - Worker processes: Each writes to worker_[PID].log
    - At exit: All worker logs are merged into app.log and deleted
    """

    _is_main_process = None
    _worker_setup_done = False

    def __init__(self, log_dir="logs"):
        """
        Initialize logger. Automatically detects main vs worker process.
        
        Args:
            log_dir: Directory for log files
        """
        self.log_dir = log_dir
        
        # Detect if we're in the main process (only once)
        if AppLogger._is_main_process is None:
            AppLogger._is_main_process = (current_process().name == 'MainProcess')
        
        # Use root logger for global coverage
        self.logger = logging.getLogger()
        
        # Set log level from config
        log_level = getattr(logging, config.get_log_level().upper(), logging.INFO)
        self.logger.setLevel(log_level)

        # Main process: Set up main log file
        if AppLogger._is_main_process:
            self._setup_main_process(log_dir)

    def _setup_main_process(self, log_dir):
        """
        Set up the main process logger to write to app.log.
        CRITICAL: Check if handlers already exist to prevent duplication
        when AppLogger is instantiated multiple times across modules.
        """
        # SINGLETON PATTERN: If root logger already has handlers, don't add more
        if self.logger.handlers:
            return
        
        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "app.log")

        # File handler for main process
        file_handler = TimedRotatingFileHandler(
            log_file, when="midnight", interval=1, backupCount=7
        )
        file_handler.suffix = "%Y%m%d"
        
        # Set file handler level from config
        log_level = getattr(logging, config.get_log_level().upper(), logging.INFO)
        file_handler.setLevel(log_level)
        
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )

        # Console handler for immediate feedback
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(
            logging.Formatter("%(levelname)s: %(message)s")
        )
        
        # Add handlers to root logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Register consolidation on exit
        atexit.register(AppLogger.consolidate_logs, log_dir)

    @classmethod
    def setup_worker(cls, log_dir="logs"):
        """
        Set up logging in a worker process.
        Each worker writes to its own worker_[PID].log file.
        Workers should NOT output to console - only to their log files.
        """
        if cls._worker_setup_done:
            return
            
        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True)
        
        # Get worker PID for unique log file
        pid = current_process().pid
        worker_log = os.path.join(log_dir, f"worker_{pid}.log")
        
        # Configure root logger in worker
        logger = logging.getLogger()
        log_level = getattr(logging, config.get_log_level().upper(), logging.INFO)
        logger.setLevel(log_level)
        
        # CRITICAL: Remove any inherited handlers (from main process)
        logger.handlers.clear()
        
        # Disable propagation to prevent console output in workers
        logger.propagate = False
        
        # File handler for this worker (NO console handler)
        file_handler = TimedRotatingFileHandler(
            worker_log, when="midnight", interval=1, backupCount=7
        )
        file_handler.suffix = "%Y%m%d"
        file_handler.setLevel(log_level)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        
        logger.addHandler(file_handler)
        cls._worker_setup_done = True

    @staticmethod
    def consolidate_logs(log_dir="logs", main_file="app.log"):
        """
        Merge all worker_*.log files into the main app.log.
        Called automatically at script exit.
        
        Args:
            log_dir: Directory containing log files
            main_file: Name of the main log file
        """
        main_log_path = os.path.join(log_dir, main_file)
        worker_logs = glob.glob(os.path.join(log_dir, "worker_*.log"))
        
        if not worker_logs:
            return
        
        print(f"\n[LOG CONSOLIDATION] Merging {len(worker_logs)} worker log(s) into {main_file}...")
        
        # Append each worker log to main log
        with open(main_log_path, 'a', encoding='utf-8') as main_log:
            for worker_log_path in sorted(worker_logs):
                worker_pid = os.path.basename(worker_log_path).replace("worker_", "").replace(".log", "")
                
                try:
                    with open(worker_log_path, 'r', encoding='utf-8') as worker_log:
                        content = worker_log.read()
                        if content.strip():  # Only add if worker had content
                            main_log.write(f"\n--- Logs from Worker {worker_pid} ---\n")
                            main_log.write(content)
                            if not content.endswith('\n'):
                                main_log.write('\n')
                    
                    # Delete worker log after successful merge
                    os.remove(worker_log_path)
                    print(f"[LOG CONSOLIDATION] Merged and removed worker_{worker_pid}.log")
                    
                except Exception as e:
                    print(f"[LOG CONSOLIDATION] Error processing {worker_log_path}: {e}")
        
        print("[LOG CONSOLIDATION] Complete - all worker logs merged into app.log")

    def log(self, message, level="INFO"):
        """
        Log a message at the specified level.
        
        Args:
            message: The message to log
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        level = level.upper()
        if level == "DEBUG":
            self.logger.debug(message)
        elif level == "INFO":
            self.logger.info(message)
        elif level == "WARNING":
            self.logger.warning(message)
        elif level == "ERROR":
            self.logger.error(message)
        elif level == "CRITICAL":
            self.logger.critical(message)
        else:
            self.logger.info(message)


