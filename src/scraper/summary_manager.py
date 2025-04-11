"""
Gencat Audio and Transcript Scraper - Summary generation manager
"""
import os
import json
import time
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class SummaryManager:
    """Manages summary generation for the scraper"""
    
    def __init__(self, output_dir, level_codes):
        """Initialize the summary manager"""
        self.output_dir = output_dir
        self.level_codes = level_codes
        
    def create_summary(self):
        """Create a summary of downloaded content"""
        try:
            logger.info("Creating summary of downloaded content")
            summary = []
            master_json = {
                "levels": {},
                "total_topics": 0,
                "total_audio_files": 0,
                "topics_with_transcript": 0,
                "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            for level_code in self.level_codes:
                level_dir = os.path.join(self.output_dir, level_code)
                if os.path.exists(level_dir):
                    level_topics = []
                    topics = [d for d in os.listdir(level_dir) if os.path.isdir(os.path.join(level_dir, d))]
                    
                    for topic in topics:
                        topic_dir = os.path.join(level_dir, topic)
                        files = os.listdir(topic_dir)
                        audio_files = [f for f in files if f.endswith('.mp3')]
                        transcript_files = [f for f in files if f.endswith('.txt')]
                        
                        # Look for metadata file
                        metadata_file = os.path.join(topic_dir, "metadata.json")
                        metadata = None
                        if os.path.exists(metadata_file):
                            try:
                                with open(metadata_file, 'r', encoding='utf-8') as f:
                                    metadata = json.load(f)
                            except Exception as e:
                                logger.error(f"Error reading metadata file {metadata_file}: {str(e)}")
                        
                        topic_data = {
                            "level": level_code,
                            "topic": topic,
                            "audio_files": len(audio_files),
                            "has_transcript": len(transcript_files) > 0,
                            "path": topic_dir,
                            "metadata": metadata
                        }
                        
                        summary.append(topic_data)
                        level_topics.append(topic_data)
                        
                        # Update master counts
                        master_json["total_topics"] += 1
                        master_json["total_audio_files"] += len(audio_files)
                        if len(transcript_files) > 0:
                            master_json["topics_with_transcript"] += 1
                    
                    # Add level data to master JSON
                    master_json["levels"][level_code] = {
                        "total_topics": len(topics),
                        "topics": level_topics
                    }
            
            # Save summary to CSV
            self._save_csv_summary(summary)
            
            # Save master JSON
            self._save_master_json(master_json)
                
        except Exception as e:
            logger.error(f"Error creating summary: {str(e)}")
            
    def _save_csv_summary(self, summary):
        """Save a CSV summary of the downloaded content"""
        try:
            if not summary:
                logger.warning("No summary data to save to CSV")
                return False
                
            # Create a simplified version for CSV
            csv_data = [{
                "level": item["level"],
                "topic": item["topic"],
                "audio_files": item["audio_files"],
                "has_transcript": item["has_transcript"],
                "path": item["path"]
            } for item in summary]
            
            df = pd.DataFrame(csv_data)
            summary_path = os.path.join(self.output_dir, "summary.csv")
            df.to_csv(summary_path, index=False)
            logger.info(f"Summary saved to {summary_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving CSV summary: {str(e)}")
            return False
            
    def _save_master_json(self, master_json):
        """Save the master JSON data"""
        try:
            master_json_path = os.path.join(self.output_dir, "master_data.json")
            with open(master_json_path, 'w', encoding='utf-8') as f:
                json.dump(master_json, f, ensure_ascii=False, indent=2)
            logger.info(f"Master JSON data saved to {master_json_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving master JSON: {str(e)}")
            return False
