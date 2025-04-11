#!/usr/bin/env python3
"""
Gencat Audio and Transcript Scraper - Main script
"""
import os
import signal
import sys
import traceback
import logging
from src.scraper.gencat_scraper import GencatScraper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("gencat_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Setup graceful shutdown
    def signal_handler(sig, frame):
        print("\nStopping script gracefully. Creating summary before exit...")
        try:
            # Create and save summary data even when quitting
            from src.scraper.summary_manager import SummaryManager
            summary_manager = SummaryManager("data/downloaded_audio", ["b1", "b2", "c1", "c2"])
            summary_manager.create_summary()
            print("Summary created successfully.")
        except Exception as e:
            print(f"Error creating summary: {str(e)}")
        sys.exit(0)
            
    signal.signal(signal.SIGINT, signal_handler)
    
    # Initialize and run scraper
    print("Starting Gencat Audio Scraper")
    print("Press Ctrl+C to stop (progress will be saved)")
    
    try:
        scraper = GencatScraper()
        print("Scraper initialized successfully")
        scraper.run()
    except KeyboardInterrupt:
        print("\nStopping script gracefully. Current progress is saved.")
    except Exception as e:
        print(f"Error during initialization: {str(e)}")
        traceback.print_exc()
    finally:
        if 'scraper' in locals() and hasattr(scraper, 'driver') and scraper.driver:
            print("Closing browser...")
            scraper.driver.quit()
            print("Browser closed.")
