import logging
import os
import atexit
import time
from logging.handlers import TimedRotatingFileHandler, QueueHandler, QueueListener
from multiprocessing import Queue, current_process
from config_manager import config


class AppLogger:
    """
    Multiprocessing-safe logger using QueueHandler + QueueListener pattern.
    
    Architecture:
    - Main process: Runs QueueListener thread that writes to file
    - Worker processes: Use QueueHandler to push to shared queue (non-blocking, fast)
    - Shared Queue: multiprocessing.Queue for inter-process communication
    """

    _queue = None
    _listener = None
    _is_main_process = None

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

        # Main process: Set up the shared queue and listener
        if AppLogger._is_main_process and AppLogger._queue is None:
            self._setup_main_process(log_dir)
        
        # All processes: Attach QueueHandler if not already present
        if not any(isinstance(h, QueueHandler) for h in self.logger.handlers):
            # Wait for main process to create queue
            if AppLogger._queue is not None:
                queue_handler = QueueHandler(AppLogger._queue)
                self.logger.addHandler(queue_handler)

    def _setup_main_process(self, log_dir):
        """
        Set up the queue and listener in the main process only.
        This should only be called once, in the main process.
        """
        # Create shared queue (unlimited size to prevent blocking)
        AppLogger._queue = Queue(-1)
        
        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "app.log")

        # File handler - only used by listener thread (no contention!)
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
        
        # Start listener thread - single writer, no lock contention
        AppLogger._listener = QueueListener(
            AppLogger._queue, file_handler, console_handler, respect_handler_level=True
        )
        AppLogger._listener.start()
        
        # Register cleanup on exit
        atexit.register(AppLogger._shutdown)

    @classmethod
    def get_queue(cls):
        """
        Get the shared logging queue.
        Used to pass queue to worker processes.
        """
        return cls._queue
    
    @classmethod
    def setup_worker(cls, queue):
        """
        Set up logging in a worker process.
        Called via ProcessPoolExecutor initializer.
        
        Args:
            queue: The shared multiprocessing.Queue from main process
        """
        cls._queue = queue
        
        # Configure root logger in worker
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        
        # Add QueueHandler if not already present
        if not any(isinstance(h, QueueHandler) for h in logger.handlers):
            queue_handler = QueueHandler(queue)
            logger.addHandler(queue_handler)
    
    @staticmethod
    def flush_queue():
        """
        Ensure all queued log messages are written to disk.
        """
        if AppLogger._queue is None:
            return
        
        # Wait for queue to drain with timeout
        max_wait = 10  # seconds
        interval = 0.1  # check every 100ms
        elapsed = 0
        
        initial_size = AppLogger._queue.qsize()
        if initial_size > 0:
            print(f"[LOG FLUSH] Draining {initial_size} messages...")
        
        while not AppLogger._queue.empty() and elapsed < max_wait:
            time.sleep(interval)
            elapsed += interval
            
            # Progress update every second
            if elapsed % 1.0 < interval:
                remaining = AppLogger._queue.qsize()
                if remaining > 0:
                    print(f"[LOG FLUSH] {remaining} messages remaining...")
        
        # Final wait for listener to flush handlers to disk
        time.sleep(0.5)
        
        final_size = AppLogger._queue.qsize()
        if final_size == 0:
            print(f"[LOG FLUSH] Complete - all messages written")
        else:
            print(f"[LOG FLUSH] Warning: {final_size} messages may be lost")

    @staticmethod
    def _shutdown():
        """
        Clean shutdown: flush queue then stop listener.
        Called automatically via atexit in main process only.
        """
        if AppLogger._listener:
            AppLogger.flush_queue()
            AppLogger._listener.stop()
            AppLogger._listener = None

    def log(self, message, level="INFO"):
        """
        Log a message at the specified level.
        Non-blocking for worker processes (just pushes to queue).
        
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


