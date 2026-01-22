import schedule
import time
import logging
import logging.config
import daily_job
import datetime

# Setup logging
logging.config.fileConfig('config/logging.conf')
logger = logging.getLogger('dq_engine')

def job():
    logger.info(f"--- Triggering Scheduled Job at {datetime.datetime.now()} ---")
    try:
        daily_job.main()
    except Exception as e:
        logger.error(f"Scheduled job crashed: {e}")

def main():
    # Configure the time you want the job to run (24-hour format)
    run_time = "18:50"
    
    logger.info(f"=== Custom Python Scheduler Started ===")
    logger.info(f"Job scheduled to run daily at {run_time}")
    logger.info("Keep this window open (you can minimize it).")

    # Schedule the job
    schedule.every().day.at(run_time).do(job)

    # Optional: Run immediately on startup for testing
    # job()

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
