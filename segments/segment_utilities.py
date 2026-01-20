from datetime import date, timedelta, datetime
import random
from logger import AppLogger

logger = AppLogger()

def create_obr_time():
    """Creates a random date for OBR segment 1-7 days in the past."""
    try:
        random_days_ago = random.randint(1, 7)
        random_date = date.today() - timedelta(days=random_days_ago)
        return random_date.strftime("%Y%m%d%H%M")
    except Exception as e:
        logger.log(f"Error creating OBR time: {e}. Using current time instead.", "WARNING")
        # Fallback to current time if random generation fails
        return datetime.now().strftime("%Y%m%d%H%M")

