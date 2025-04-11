"""
Gencat Audio and Transcript Scraper - Core scraper class
"""
import os
import time
import logging
import json
import re
import traceback
import urllib.parse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import requests
from src.scraper.progress_manager import ProgressManager

logger = logging.getLogger(__name__)

class GencatScraper:
    def __init__(self, output_dir="data/downloaded_audio"):
        self.output_dir = output_dir
        self.level_urls = {
            "b1": "https://llengua.gencat.cat/ca/serveis/aprendre_catala/recursos-per-al-professorat/dictats-en-linia/b1/tots/",
            "b2": "https://llengua.gencat.cat/ca/serveis/aprendre_catala/recursos-per-al-professorat/dictats-en-linia/b2/tots/",
            "c1": "https://llengua.gencat.cat/ca/serveis/aprendre_catala/recursos-per-al-professorat/dictats-en-linia/c1/tots/",
            "c2": "https://llengua.gencat.cat/ca/serveis/aprendre_catala/recursos-per-al-professorat/dictats-en-linia/c2/tots/"
        }
        
        # Create root output directory
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Initialize progress manager
        self.progress_manager = ProgressManager(output_dir)

        # Set up WebDriver
        self._init_webdriver()
            
    def _init_webdriver(self):
        """Initialize the Chrome WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920x1080")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            # Use simple Chrome WebDriver initialization
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            logger.info("Chrome WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Chrome WebDriver: {str(e)}")
            raise
        
    def get_iframe_src(self, level_url):
        """Get the iframe source URL from a level page"""
        try:
            self.driver.get(level_url)
            time.sleep(3)  # Wait for page to load
            
            # Look for iframes with the selector URL
            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            for iframe in iframes:
                src = iframe.get_attribute("src")
                if src and "selector_filtrat2_geco_tot.html" in src:
                    return src
                    
            # If we couldn't find the iframe, look for it in the HTML
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            iframe_tag = soup.find('iframe', {'src': lambda x: x and 'selector_filtrat2_geco_tot.html' in x})
            if iframe_tag and 'src' in iframe_tag.attrs:
                return iframe_tag['src']
                
            logger.error(f"Could not find iframe src in {level_url}")
            return None
        except Exception as e:
            logger.error(f"Error getting iframe src from {level_url}: {str(e)}")
            return None
        
    def get_topic_links(self, iframe_url):
        """Get all dictation topic links from the iframe page"""
        try:
            self.driver.get(iframe_url)
            time.sleep(3)  # Wait for JavaScript to load
            
            topic_links = []
            # Get all links that match the pattern for dictation topics
            links = self.driver.find_elements(By.TAG_NAME, "a")
            
            for link in links:
                href = link.get_attribute("href")
                if href and "dictat_geco_tot.html?captaclau=" in href:
                    topic_text = link.text.strip()
                    if not topic_text:
                        # Try to get the title attribute
                        topic_text = link.get_attribute("title")
                        if topic_text:
                            # Some titles have a number prefix, remove it
                            topic_text = re.sub(r'^\d+>', '', topic_text).strip()
                    
                    if not topic_text:
                        topic_text = f"Topic_{len(topic_links) + 1}"
                        
                    topic_links.append({
                        "url": href,
                        "title": topic_text
                    })
                    
            return topic_links
        except Exception as e:
            logger.error(f"Error getting topic links from {iframe_url}: {str(e)}")
            return []
            
    def extract_audio_urls(self, topic_url):
        """Extract audio file URLs and transcript from a topic page"""
        try:
            self.driver.get(topic_url)
            time.sleep(3)  # Wait for content to load
            
            # Get all audio elements
            audio_elements = self.driver.find_elements(By.TAG_NAME, "audio")
            
            # Extract source URLs
            audio_files = []
            for audio in audio_elements:
                sources = audio.find_elements(By.TAG_NAME, "source")
                for source in sources:
                    src = source.get_attribute("src")
                    audio_type = source.get_attribute("type")
                    
                    # Only keep mp3 files based on your requirements
                    if src and "mp3" in src:
                        filename = src.split("/")[-1]
                        
                        # Get download link
                        download_url = None
                        download_links = self.driver.find_elements(By.TAG_NAME, "a")
                        for link in download_links:
                            href = link.get_attribute("href")
                            if href and filename in href:
                                download_url = href
                                break
                        
                        audio_files.append({
                            "source_url": src,
                            "download_url": download_url if download_url else src,
                            "type": audio_type,
                            "filename": filename
                        })
            
            # Get transcript
            transcript = self.extract_transcript_with_button_click()
            
            return {
                "audio_files": audio_files,
                "transcript": transcript
            }
        except Exception as e:
            logger.error(f"Error extracting content from {topic_url}: {str(e)}")
            return {"audio_files": [], "transcript": None}
    
    def extract_transcript_with_button_click(self):
        """Click the Solució button and extract the revealed transcript"""
        try:
            # Multiple ways to find the "Solució" button
            solucio_button = None
            
            # Method 1: Try finding input with value="Solució"
            try:
                solucio_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//input[@value='Solució']"))
                )
            except:
                pass
                
            # Method 2: Try finding by ID
            if not solucio_button:
                try:
                    solucio_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.ID, "oculta"))
                    )
                except:
                    pass
            
            # Method 3: Try finding by CSS class
            if not solucio_button:
                try:
                    solucio_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, ".button-secondary"))
                    )
                except:
                    pass
                    
            # Method 4: Try JavaScript approach
            if not solucio_button:
                try:
                    # Try to find with JavaScript
                    self.driver.execute_script("document.querySelector('input[value=\"Solució\"]').click();")
                    time.sleep(1)  # Wait for content to be revealed
                    solucio_button = True  # Dummy value to indicate success
                except:
                    pass
                    
            # Method 5: Try finding any button or input with 'Soluc' in text or value
            if not solucio_button:
                try:
                    elements = self.driver.find_elements(By.XPATH, "//*[contains(@value, 'Soluc') or contains(text(), 'Soluc')]")
                    for element in elements:
                        if element.is_displayed():
                            solucio_button = element
                            break
                except:
                    pass
                    
            # If we still can't find it, log and return None
            if not solucio_button:
                logger.warning("Could not find 'Solució' button")
                return None
            
            # Click the button to reveal the transcript (unless we already clicked with JavaScript)
            if solucio_button is not True:  # Skip if we already used JavaScript to click
                try:
                    solucio_button.click()
                except Exception as e:
                    try:
                        # Try JavaScript click as fallback
                        self.driver.execute_script("arguments[0].click();", solucio_button)
                    except Exception as e2:
                        logger.error(f"JavaScript click also failed: {str(e2)}")
                        return None
                        
            time.sleep(1)  # Wait for content to be revealed
            
            # Look for transcript in various elements
            for div_id in ["paraocult", "paraocutar", "peraocutar"]:
                try:
                    elements = self.driver.find_elements(By.ID, div_id)
                    for element in elements:
                        if element.is_displayed():
                            text = element.text.strip()
                            if text:
                                return text
                except:
                    pass
                    
            # Try using JavaScript to get the content directly
            try:
                for div_id in ["paraocult", "paraocutar", "peraocutar"]:
                    script = f"return document.getElementById('{div_id}').innerText;"
                    text = self.driver.execute_script(script)
                    if text:
                        return text
            except:
                pass
                
            # Try other potential transcript containers
            for selector in [".text-solucion", ".paraocult", ".textesol", "div[style*='display: block']"]:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        if element.is_displayed():
                            text = element.text.strip()
                            if text:
                                return text
                except:
                    continue
                    
            return None
                
        except Exception as e:
            logger.error(f"Error extracting transcript: {str(e)}")
            return None
            
    def download_audio(self, url, filepath):
        """Download an audio file to the specified filepath"""
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
            else:
                logger.error(f"Failed to download {url}: HTTP {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error downloading {url}: {str(e)}")
            return False
            
    def save_transcript(self, transcript, filepath):
        """Save transcript to a text file"""
        try:
            if transcript:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(transcript)
                return True
            return False
        except Exception as e:
            logger.error(f"Error saving transcript to {filepath}: {str(e)}")
            return False
    
    def save_topic_metadata(self, level_code, topic_title, audio_files, transcript, topic_dir):
        """Save topic metadata to a JSON file"""
        try:
            sanitized_title = self.sanitize_filename(topic_title)
            metadata_file = os.path.join(topic_dir, "metadata.json")
            
            # Prepare audio file paths
            audio_file_data = []
            for filename in audio_files:
                # Find matching URL from the extracted audio files
                original_url = None
                for audio in content["audio_files"]:
                    if audio["filename"] == filename:
                        original_url = audio["source_url"]
                        break
                
                speed_type = "rapid" if "rapid" in filename.lower() else "lent" if "lent" in filename.lower() else "unknown"
                
                audio_file_data.append({
                    "filename": filename,
                    "path": os.path.join(topic_dir, filename),
                    "relative_path": filename,
                    "type": speed_type,
                    "original_url": original_url
                })
            
            # Prepare transcript data if available
            transcript_data = None
            if transcript:
                transcript_file = f"{sanitized_title}.txt"
                transcript_data = {
                    "filename": transcript_file,
                    "path": os.path.join(topic_dir, transcript_file),
                    "relative_path": transcript_file,
                    "content": transcript
                }
            
            # Create metadata object
            metadata = {
                "level": level_code,
                "topic": topic_title,
                "sanitized_title": sanitized_title,
                "dir_path": topic_dir,
                "audio_files": audio_file_data,
                "transcript": transcript_data,
                "processed_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Save to JSON file
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
                
            # Register this topic in the central registry
            self.progress_manager.register_topic(level_code, topic_title, topic_dir, metadata_file)
                
            return True
        except Exception as e:
            logger.error(f"Error saving metadata for topic {topic_title}: {str(e)}")
            return False
            
    def sanitize_filename(self, filename):
        """Sanitize a string to be used as a filename"""
        # Replace problematic characters
        filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
        # Remove leading/trailing spaces and dots
        filename = filename.strip(". ")
        # Ensure the filename is not empty
        if not filename:
            filename = "unnamed"
        return filename
            
    def process_level(self, level_code, level_url):
        """Process all topics for a specific level"""
        logger.info(f"Processing level: {level_code}")
        
        # Create level directory
        level_dir = os.path.join(self.output_dir, level_code)
        if not os.path.exists(level_dir):
            os.makedirs(level_dir)
            
        # Get iframe source URL
        iframe_src = self.get_iframe_src(level_url)
        if not iframe_src:
            logger.error(f"Could not find iframe for level {level_code}")
            return
            
        # Make sure the iframe URL is absolute
        if not iframe_src.startswith('http'):
            parsed_url = urllib.parse.urlparse(level_url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            iframe_src = urllib.parse.urljoin(base_url, iframe_src)
            
        # Get topic links
        topic_links = self.get_topic_links(iframe_src)
        logger.info(f"Found {len(topic_links)} topics for level {level_code}")
        
        # Track progress
        total_topics = len(topic_links)
        completed_topics = 0
        skipped_topics = 0
        
        # Process each topic
        for i, topic in enumerate(topic_links):
            topic_title = topic["title"]
            sanitized_title = self.sanitize_filename(topic_title)
            
            # Create topic directory
            topic_dir = os.path.join(level_dir, sanitized_title)
            if not os.path.exists(topic_dir):
                os.makedirs(topic_dir)
                
            # Check if this topic has already been processed
            completion_marker = os.path.join(topic_dir, ".completed")
            
            if os.path.exists(completion_marker):
                logger.info(f"Skipping topic {i+1}/{total_topics}: {topic_title} (already processed)")
                skipped_topics += 1
                completed_topics += 1
                
                # Register the topic in the registry even if skipped
                metadata_file = os.path.join(topic_dir, "metadata.json")
                if os.path.exists(metadata_file):
                    self.progress_manager.register_topic(level_code, topic_title, topic_dir, metadata_file)
                
                continue
                
            logger.info(f"Processing topic {i+1}/{total_topics}: {topic_title}")
                
            # Extract audio URLs and transcript
            content = self.extract_audio_urls(topic["url"])
            audio_files = content["audio_files"]
            transcript = content["transcript"]
            
            logger.info(f"Found {len(audio_files)} audio files for topic {topic_title}")
            
            # Save transcript
            transcript_saved = False
            transcript_file = os.path.join(topic_dir, f"{sanitized_title}.txt")
            if transcript:
                if self.save_transcript(transcript, transcript_file):
                    logger.info(f"Saved transcript to {transcript_file}")
                    transcript_saved = True
                else:
                    logger.warning(f"Failed to save transcript for {topic_title}")
            else:
                logger.warning(f"No transcript found for topic {topic_title}")
                    
            # Download audio files
            audio_downloaded = []
            for audio in audio_files:
                filename = audio["filename"]
                # Only download MP3 files as requested
                if filename.endswith('.mp3'):
                    audio_filepath = os.path.join(topic_dir, filename)
                    
                    # Skip if file already exists
                    if os.path.exists(audio_filepath):
                        logger.info(f"File {filename} already exists, skipping")
                        audio_downloaded.append(filename)
                        continue
                        
                    if self.download_audio(audio["download_url"], audio_filepath):
                        logger.info(f"Downloaded {filename} to {audio_filepath}")
                        audio_downloaded.append(filename)
                    else:
                        logger.warning(f"Failed to download {filename}")
            
            # Save metadata JSON with file paths and transcript
            self.save_topic_metadata(level_code, topic_title, audio_downloaded, transcript, topic_dir)
            
            # Mark topic as completed if we have at least one audio file
            if audio_downloaded or transcript_saved:
                with open(completion_marker, 'w') as f:
                    f.write(f"Processed on {time.strftime('%Y-%m-%d %H:%M:%S')}")
                completed_topics += 1
                logger.info(f"Marked topic {topic_title} as completed")
                
            # Sleep to avoid overwhelming the server
            time.sleep(1)
            
        # Update progress with partial stats immediately
        level_stats = {
            "total": total_topics,
            "completed": completed_topics,
            "skipped": skipped_topics
        }
        self.progress_manager.update_stats(level_stats)
        
        logger.info(f"Level {level_code} processing complete: {completed_topics}/{total_topics} topics processed ({skipped_topics} skipped)")
        return level_stats
            
    def run(self):
        """Run the scraper for all levels"""
        try:
            total_start_time = time.time()
            overall_stats = {"total": 0, "completed": 0, "skipped": 0}
            
            # Get completed levels from progress manager
            completed_levels = self.progress_manager.get_completed_levels()
            
            logger.info(f"Starting scraper. Already completed levels: {', '.join(completed_levels)}")
            
            for level_code, level_url in self.level_urls.items():
                # Skip levels that have been fully processed
                if level_code in completed_levels:
                    logger.info(f"Skipping level {level_code} (already completed)")
                    continue
                    
                level_start_time = time.time()
                logger.info(f"Starting to process level {level_code} from {level_url}")
                
                # Mark level as in-progress
                self.progress_manager.update_level_status(level_code, in_progress=True)
                
                level_stats = self.process_level(level_code, level_url)
                level_end_time = time.time()
                
                if level_stats:
                    overall_stats["total"] += level_stats["total"]
                    overall_stats["completed"] += level_stats["completed"]
                    overall_stats["skipped"] += level_stats["skipped"]
                
                    # Update progress with level stats
                    is_complete = level_stats["completed"] == level_stats["total"]
                    self.progress_manager.update_level_status(level_code, completed=is_complete)
                    self.progress_manager.update_stats(level_stats)
                
                logger.info(f"Completed level {level_code} in {level_end_time - level_start_time:.2f} seconds")
                
            total_end_time = time.time()
            duration = total_end_time - total_start_time
            hours, remainder = divmod(duration, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            logger.info(f"Total scraping completed in {int(hours)}h {int(minutes)}m {int(seconds)}s")
            logger.info(f"Overall stats: {overall_stats['completed']} topics processed out of {overall_stats['total']} ({overall_stats['skipped']} skipped)")
            
            # Create a summary file
            from src.scraper.summary_manager import SummaryManager
            summary_manager = SummaryManager(self.output_dir, self.level_urls.keys())
            summary_manager.create_summary()
            
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
        finally:
            if hasattr(self, 'driver'):
                self.driver.quit()
