"""
Gencat Audio and Transcript Scraper - Progress tracking manager
"""
import os
import json
import time
import logging

logger = logging.getLogger(__name__)

class ProgressManager:
    """Manages progress tracking for the scraper"""
    
    def __init__(self, output_dir):
        """Initialize the progress manager"""
        self.output_dir = output_dir
        self.progress_file = os.path.join(output_dir, "progress.json")
        self.topic_registry_file = os.path.join(output_dir, "topic_registry.json")
        self.progress_data = self._initialize_progress_file()
        self.topic_registry = self._initialize_topic_registry()
        
    def _initialize_progress_file(self):
        """Initialize or load the progress tracking file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    progress_data = json.load(f)
                logger.info(f"Loaded existing progress file")
                return progress_data
            except Exception as e:
                logger.error(f"Error reading progress file: {str(e)}")
        
        # Create a new progress file if it doesn't exist or can't be read
        progress_data = {
            "completed_levels": [],
            "in_progress_levels": {},
            "stats": {
                "total_topics": 0,
                "completed_topics": 0,
                "skipped_topics": 0
            },
            "start_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
            logger.info(f"Created new progress file")
        except Exception as e:
            logger.error(f"Error creating progress file: {str(e)}")
            
        return progress_data
    
    def _initialize_topic_registry(self):
        """Initialize or load the topic registry file"""
        if os.path.exists(self.topic_registry_file):
            try:
                with open(self.topic_registry_file, 'r') as f:
                    topic_registry = json.load(f)
                logger.info(f"Loaded existing topic registry file")
                return topic_registry
            except Exception as e:
                logger.error(f"Error reading topic registry file: {str(e)}")
        
        # Create a new topic registry file if it doesn't exist or can't be read
        topic_registry = {
            "topics": {},
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_registered_topics": 0
        }
        
        try:
            with open(self.topic_registry_file, 'w') as f:
                json.dump(topic_registry, f, indent=2)
            logger.info(f"Created new topic registry file")
        except Exception as e:
            logger.error(f"Error creating topic registry file: {str(e)}")
            
        return topic_registry
        
    def register_topic(self, level_code, topic_title, topic_dir, metadata_path=None):
        """Register a topic in the registry"""
        try:
            # Create an entry for this topic
            topic_key = f"{level_code}/{topic_title}"
            
            if topic_key not in self.topic_registry["topics"]:
                self.topic_registry["topics"][topic_key] = {
                    "level": level_code,
                    "title": topic_title,
                    "directory": topic_dir,
                    "metadata_file": metadata_path if metadata_path else os.path.join(topic_dir, "metadata.json"),
                    "registered_time": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                self.topic_registry["total_registered_topics"] += 1
            else:
                # Update existing entry
                self.topic_registry["topics"][topic_key]["directory"] = topic_dir
                self.topic_registry["topics"][topic_key]["metadata_file"] = metadata_path if metadata_path else os.path.join(topic_dir, "metadata.json")
                self.topic_registry["topics"][topic_key]["updated_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Update timestamp
            self.topic_registry["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Save the registry
            self._save_topic_registry()
            return True
        except Exception as e:
            logger.error(f"Error registering topic {topic_title}: {str(e)}")
            return False
            
    def update_level_status(self, level_code, completed=False, in_progress=False):
        """Update the status of a level in the progress file"""
        try:
            # Update level status
            if completed:
                if level_code not in self.progress_data["completed_levels"]:
                    self.progress_data["completed_levels"].append(level_code)
                    
                # Remove from in_progress if it was there
                if level_code in self.progress_data.get("in_progress_levels", {}):
                    del self.progress_data["in_progress_levels"][level_code]
            elif in_progress:
                # Update in-progress level data
                if "in_progress_levels" not in self.progress_data:
                    self.progress_data["in_progress_levels"] = {}
                    
                self.progress_data["in_progress_levels"][level_code] = {
                    "last_processed": time.strftime("%Y-%m-%d %H:%M:%S")
                }
            
            # Update timestamp
            self.progress_data["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Write updated data back to file
            result = self._save_progress_file()
            if result:
                logger.info(f"Updated level status for {level_code}: completed={completed}, in_progress={in_progress}")
            return result
        except Exception as e:
            logger.error(f"Error updating level status: {str(e)}")
            return False
            
    def update_stats(self, stats_data):
        """Update statistics in the progress file"""
        try:
            if "stats" not in self.progress_data:
                self.progress_data["stats"] = {
                    "total_topics": 0,
                    "completed_topics": 0,
                    "skipped_topics": 0
                }
                
            if "completed" in stats_data:
                self.progress_data["stats"]["completed_topics"] += stats_data["completed"]
            if "skipped" in stats_data:
                self.progress_data["stats"]["skipped_topics"] += stats_data["skipped"]
            if "total" in stats_data:
                self.progress_data["stats"]["total_topics"] += stats_data["total"]
                
            # Update timestamp
            self.progress_data["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Write updated data back to file
            result = self._save_progress_file()
            if result:
                logger.info(f"Updated stats: +{stats_data.get('completed', 0)} completed, +{stats_data.get('skipped', 0)} skipped, +{stats_data.get('total', 0)} total")
            return result
        except Exception as e:
            logger.error(f"Error updating stats: {str(e)}")
            return False
            
    def get_completed_levels(self):
        """Get the list of completed levels"""
        return self.progress_data.get("completed_levels", [])
        
    def get_stats(self):
        """Get the current progress statistics"""
        return self.progress_data.get("stats", {})
        
    def get_all_topic_paths(self):
        """Get paths for all registered topics"""
        return self.topic_registry.get("topics", {})
        
    def _save_progress_file(self):
        """Save the progress data to the progress file"""
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress_data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving progress file: {str(e)}")
            return False
            
    def _save_topic_registry(self):
        """Save the topic registry to the registry file"""
        try:
            with open(self.topic_registry_file, 'w') as f:
                json.dump(self.topic_registry, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving topic registry file: {str(e)}")
            return False
