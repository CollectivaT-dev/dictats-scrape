"""
Gencat Audio and Transcript Scraper - Segmentation Module
This module processes scraped audio files and transcripts to segment them into sentences.
Uses ffmpeg for audio processing.
"""
import os
import json
import csv
import re
import logging
import time
import subprocess
import replicate
import pandas as pd

logger = logging.getLogger(__name__)

class GencatSegmenter:
    """Handles segmentation of audio files into sentences based on transcripts"""
    
    def __init__(self, data_dir="data/downloaded_audio", output_dir="data/corpus"):
        """Initialize the segmenter module"""
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.audio_output_dir = os.path.join(output_dir, "audio")
        self.csv_output_file = os.path.join(output_dir, "segments.csv")
        self.log_file = os.path.join(output_dir, "segmentation.log")
        
        # Create output directories
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.audio_output_dir, exist_ok=True)
        
        # Initialize CSV file with header if it doesn't exist
        if not os.path.exists(self.csv_output_file):
            with open(self.csv_output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='|')
                writer.writerow(['filename', 'transcript'])
                
        # Set up logging to file in corpus directory
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
    
    def clean_transcript(self, transcript):
        """
        Clean the transcript by:
        1. Removing metadata (like author attribution)
        2. Converting newlines to spaces
        3. Removing extra spaces
        """
        # Remove any text attribution or metadata at the end (like "Text: Sònia Moll")
        transcript = re.sub(r'\nText:.*$', '', transcript)
        transcript = re.sub(r'\n\s*Author:.*$', '', transcript, flags=re.IGNORECASE)
        
        # Remove any other metadata patterns
        transcript = re.sub(r'\n\s*Source:.*$', '', transcript, flags=re.IGNORECASE)
        
        # Convert newlines to spaces
        transcript = transcript.replace('\n', ' ')
        
        # Remove multiple spaces (replace with a single space)
        transcript = re.sub(r'\s+', ' ', transcript)
        
        return transcript.strip()
    
    def segment_audio_file(self, audio_path, transcript_text=None, transcript_path=None, level="unknown", topic_name="unknown"):
        """
        Segment an audio file into sentences using the alignment API
        
        Args:
            audio_path: Path to the audio file
            transcript_text: The transcript text (if available directly)
            transcript_path: Path to the transcript file (if transcript_text not provided)
            level: Level code (e.g., 'b1')
            topic_name: Topic name
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Processing audio file: {audio_path}")
            
            # Get transcript
            if transcript_text:
                # Use provided transcript text
                clean_text = self.clean_transcript(transcript_text)
                logger.info("Using provided transcript text")
            elif transcript_path and os.path.exists(transcript_path):
                # Read from transcript file
                logger.info(f"Reading transcript from: {transcript_path}")
                with open(transcript_path, 'r', encoding='utf-8') as f:
                    transcript = f.read()
                clean_text = self.clean_transcript(transcript)
            else:
                logger.error(f"No transcript provided for {audio_path}")
                return False
                
            logger.info(f"Cleaned transcript: {clean_text[:100]}...")
            
            # Add "Generalitat de Catalunya" to the end for alignment
            alignment_text = clean_text + " Generalitat de Catalunya"
            
            # Get the topic directory path
            topic_dir = os.path.dirname(audio_path)
            
            # Run the alignment API
            try:
                logger.info("Calling Replicate API for alignment...")
                
                # Using the new API endpoint with correct parameter names
                alignment_response = replicate.run(
                    "cureau/force-align-wordstamps:44dedb84066ba1e00761f45c1003c5c19ed3b12ae9d42c1c1883ca4c016ffa85",
                    input={
                        "audio_file": open(audio_path, "rb"),
                        "transcript": alignment_text
                    }
                )
                
                logger.info(f"Alignment API returned result of type: {type(alignment_response)}")
                
                # Extract the wordstamps from the response
                if isinstance(alignment_response, dict) and "wordstamps" in alignment_response:
                    alignment_result = alignment_response["wordstamps"]
                    logger.info(f"Extracted {len(alignment_result)} wordstamps from response")
                else:
                    logger.info(f"Unexpected response format: {str(alignment_response)[:200]}...")
                    # Try to handle different formats
                    if isinstance(alignment_response, list):
                        alignment_result = alignment_response
                        logger.info(f"Using response directly as wordstamps list")
                    else:
                        logger.error(f"Cannot process alignment response format")
                        return False
                
                # Save alignment result in the same directory as the audio file
                audio_filename = os.path.basename(audio_path)
                alignment_file = os.path.join(topic_dir, f"{audio_filename}_alignment.json")
                
                with open(alignment_file, 'w', encoding='utf-8') as f:
                    json.dump(alignment_response, f, indent=2)
                
                logger.info(f"Saved alignment data to {alignment_file}")
                
            except Exception as e:
                logger.error(f"Error calling Replicate API: {str(e)}")
                return False
            
            # Process the alignment result to identify sentence boundaries
            # and extract audio segments, ignoring the "Generalitat de Catalunya" suffix
            sentences = self._extract_sentences_ffmpeg(alignment_result, audio_path, level, topic_name)
            
            # Save segments to CSV
            self._save_segments_to_csv(sentences)
            
            logger.info(f"Successfully segmented {audio_path} into {len(sentences)} sentences")
            return True
            
        except Exception as e:
            logger.error(f"Error segmenting audio file {audio_path}: {str(e)}")
            return False
    
    def _extract_sentences_ffmpeg(self, alignment_result, audio_file_path, level, topic):
        """
        Extract sentences from the alignment result and segment the audio file
        using ffmpeg, ignoring the "Generalitat de Catalunya" suffix
        """
        try:
            sentences = []
            current_sentence = []
            current_text = ""
            
            # Check if alignment_result is a list (expected format)
            if not isinstance(alignment_result, list):
                logger.error(f"Unexpected alignment result format: {type(alignment_result)}")
                return []
                
            # Debug the structure of the first few items
            if len(alignment_result) > 0:
                logger.info(f"First alignment item structure: {str(alignment_result[0])}")
            
            # Find where "Generalitat de Catalunya" starts in the alignment
            generalitat_index = None
            for i, word_data in enumerate(alignment_result):
                if word_data.get("word") == "Generalitat":
                    # Check if followed by "de Catalunya"
                    if (i+2 < len(alignment_result) and 
                        alignment_result[i+1].get("word") == "de" and 
                        alignment_result[i+2].get("word") == "Catalunya"):
                        generalitat_index = i
                        break
            
            # Process words, excluding the "Generalitat de Catalunya" suffix
            max_index = generalitat_index if generalitat_index is not None else len(alignment_result)
            
            for i, word_data in enumerate(alignment_result[:max_index]):
                word = word_data.get("word", "")
                
                current_sentence.append(word_data)
                current_text += word + " "
                
                # Check if this word ends a sentence (has punctuation at the end)
                if word.endswith(".") or word.endswith("!") or word.endswith("?"):
                    # Found a sentence boundary
                    first_word = current_sentence[0]
                    last_word = current_sentence[-1]
                    
                    sentence_start = float(first_word.get("start", 0))
                    sentence_end = float(last_word.get("end", 0))
                    sentence_duration = sentence_end - sentence_start
                    
                    # Generate a filename for the segment
                    topic_sanitized = re.sub(r'[^\w]', '_', topic)
                    sentence_index = len(sentences) + 1
                    filename = f"{level}_{topic_sanitized}_sentence{sentence_index}.mp3"
                    filepath = os.path.join(self.audio_output_dir, filename)
                    
                    # Use ffmpeg to extract the segment
                    self._extract_audio_segment(
                        audio_file_path, 
                        filepath, 
                        sentence_start, 
                        sentence_duration
                    )
                    
                    # Clean the current text (remove extra spaces)
                    current_text = current_text.strip()
                    
                    # Add to sentences list
                    sentences.append({
                        "filename": filename,
                        "filepath": filepath,
                        "transcript": current_text,
                        "start_time": sentence_start,
                        "end_time": sentence_end
                    })
                    
                    logger.info(f"Extracted sentence {sentence_index}: {current_text}")
                    
                    # Reset for next sentence
                    current_sentence = []
                    current_text = ""
            
            # Handle any remaining words as a sentence (if the transcript doesn't end with punctuation)
            if current_sentence:
                first_word = current_sentence[0]
                last_word = current_sentence[-1]
                
                sentence_start = float(first_word.get("start", 0))
                sentence_end = float(last_word.get("end", 0))
                sentence_duration = sentence_end - sentence_start
                
                # Generate a filename for the segment
                topic_sanitized = re.sub(r'[^\w]', '_', topic)
                sentence_index = len(sentences) + 1
                filename = f"{level}_{topic_sanitized}_sentence{sentence_index}.mp3"
                filepath = os.path.join(self.audio_output_dir, filename)
                
                # Use ffmpeg to extract the segment
                self._extract_audio_segment(
                    audio_file_path, 
                    filepath, 
                    sentence_start, 
                    sentence_duration
                )
                
                # Clean the current text
                current_text = current_text.strip()
                
                # Add to sentences list
                sentences.append({
                    "filename": filename,
                    "filepath": filepath,
                    "transcript": current_text,
                    "start_time": sentence_start,
                    "end_time": sentence_end
                })
                
                logger.info(f"Extracted final sentence {sentence_index}: {current_text}")
            
            return sentences
            
        except Exception as e:
            logger.error(f"Error extracting sentences: {str(e)}")
            return []
    
    def _extract_audio_segment(self, input_file, output_file, start_time, duration):
        """
        Extract a segment from an audio file using ffmpeg
        
        Args:
            input_file: Path to the input audio file
            output_file: Path to save the output segment
            start_time: Start time in seconds
            duration: Duration in seconds
        """
        try:
            # Format the ffmpeg command
            cmd = [
                'ffmpeg',
                '-i', input_file,
                '-ss', str(start_time),
                '-t', str(duration),
                '-c:a', 'copy',  # Copy audio stream without re-encoding
                '-y',  # Overwrite output files
                output_file
            ]
            
            # Run the command
            process = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Check if successful
            if process.returncode != 0:
                logger.error(f"ffmpeg error: {process.stderr}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error extracting audio segment: {str(e)}")
            return False
    
    def _save_segments_to_csv(self, sentences):
        """Save segment information to CSV file"""
        try:
            with open(self.csv_output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='|')
                for sentence in sentences:
                    writer.writerow([
                        sentence["filename"],
                        sentence["transcript"]
                    ])
            logger.info(f"Added {len(sentences)} segments to CSV file")
            return True
        except Exception as e:
            logger.error(f"Error saving segments to CSV: {str(e)}")
            return False
    
    def process_directory(self, limit_to_one=True):
        """
        Process all audio files and transcripts in the data directory
        
        Args:
            limit_to_one: If True, only process one file then exit
        """
        try:
            logger.info(f"Starting to process audio files in {self.data_dir}")
            
            # Try to load master_data.json if available
            master_data = self._load_master_data()
            
            processed_count = 0
            
            if master_data:
                logger.info("Using master_data.json to find files")
                # Print a bit of the master_data structure for debugging
                logger.info(f"Master data keys: {str(master_data.keys())}")
                if "levels" in master_data:
                    level_keys = list(master_data["levels"].keys())
                    logger.info(f"Levels found: {level_keys}")
                    
                return self._process_from_master_data(master_data, limit_to_one)
            else:
                logger.info("No master_data.json found, scanning directory structure")
                return self._process_directory_scan(limit_to_one)
            
        except Exception as e:
            logger.error(f"Error processing directory: {str(e)}")
            import traceback
            traceback.print_exc()
            return 0
    
    def _load_master_data(self):
        """Load master_data.json if available"""
        master_data_path = os.path.join(self.data_dir, "master_data.json")
        if os.path.exists(master_data_path):
            try:
                with open(master_data_path, 'r', encoding='utf-8') as f:
                    master_data = json.load(f)
                    logger.info(f"Successfully loaded master_data.json")
                    return master_data
            except Exception as e:
                logger.error(f"Error loading master_data.json: {str(e)}")
        return None
    
    def _process_from_master_data(self, master_data, limit_to_one=True):
        """Process files using information from master_data.json"""
        processed_count = 0
        
        # Check the structure of master_data to handle possible format differences
        if "levels" not in master_data:
            logger.error("master_data.json doesn't have 'levels' key")
            return self._process_directory_scan(limit_to_one)  # Fallback to directory scan
            
        for level_code, level_data in master_data["levels"].items():
            logger.info(f"Processing level: {level_code}")
            
            # Handle different possible structures in the master_data.json
            topics_list = []
            
            if "topics" in level_data and isinstance(level_data["topics"], list):
                # If topics is a list, use it directly
                topics_list = level_data["topics"]
            elif isinstance(level_data, list):
                # If level_data itself is a list, use it
                topics_list = level_data
            else:
                logger.warning(f"Unexpected structure for level {level_code}, skipping")
                continue
                
            logger.info(f"Found {len(topics_list)} topics in level {level_code}")
            
            for topic_data in topics_list:
                if not isinstance(topic_data, dict):
                    logger.warning(f"Unexpected topic data type: {type(topic_data)}, skipping")
                    continue
                    
                topic_dir = topic_data.get("path")
                topic_name = topic_data.get("topic")
                
                if not topic_dir or not topic_name:
                    logger.warning(f"Missing path or topic name in topic data: {topic_data}")
                    continue
                    
                if not os.path.exists(topic_dir):
                    logger.warning(f"Topic directory not found: {topic_dir}")
                    continue
                
                logger.info(f"Processing topic: {topic_name}")
                
                # Get transcript from master_data.json
                transcript_text = None
                transcript_path = None
                
                if "transcript" in topic_data and isinstance(topic_data["transcript"], dict):
                    if "content" in topic_data["transcript"]:
                        # Get transcript text directly from master_data.json
                        transcript_text = topic_data["transcript"]["content"]
                        logger.info(f"Found transcript content in master_data.json")
                    elif "path" in topic_data["transcript"]:
                        # Get path to transcript file
                        transcript_path = topic_data["transcript"]["path"]
                        logger.info(f"Found transcript path in master_data.json: {transcript_path}")
                
                if not transcript_text and (not transcript_path or not os.path.exists(transcript_path)):
                    # Look for transcript files in the topic directory
                    transcript_files = [f for f in os.listdir(topic_dir) if f.endswith('.txt')]
                    if transcript_files:
                        transcript_path = os.path.join(topic_dir, transcript_files[0])
                        logger.info(f"Found transcript file by scanning directory: {transcript_path}")
                
                if not transcript_text and (not transcript_path or not os.path.exists(transcript_path)):
                    logger.warning(f"No transcript found for {level_code}/{topic_name}")
                    continue
                
                # Process audio files for this topic
                audio_files = []
                
                # Try to get audio files from topic_data
                if "audio_files" in topic_data and isinstance(topic_data["audio_files"], list):
                    rapid_files = []
                    other_files = []
                    
                    for audio_data in topic_data["audio_files"]:
                        if not isinstance(audio_data, dict) or "path" not in audio_data:
                            continue
                            
                        audio_path = audio_data["path"]
                        
                        # Check if the file exists
                        if not os.path.exists(audio_path):
                            continue
                            
                        # Prioritize rapid files
                        if "rapid" in audio_path.lower():
                            rapid_files.append(audio_path)
                        else:
                            other_files.append(audio_path)
                    
                    # Use rapid files if available, otherwise use other files
                    if rapid_files:
                        audio_files = rapid_files
                        logger.info(f"Found {len(rapid_files)} rapid audio files")
                    else:
                        audio_files = other_files
                        logger.info(f"No rapid audio files found, using {len(other_files)} other files")
                
                if not audio_files:
                    # Fallback to directory scan, prioritizing rapid files
                    all_files = [f for f in os.listdir(topic_dir) if f.endswith('.mp3')]
                    rapid_files = [os.path.join(topic_dir, f) for f in all_files if "rapid" in f.lower()]
                    other_files = [os.path.join(topic_dir, f) for f in all_files if "rapid" not in f.lower()]
                    
                    if rapid_files:
                        audio_files = rapid_files
                        logger.info(f"Found {len(rapid_files)} rapid audio files by scanning directory")
                    else:
                        audio_files = other_files
                        logger.info(f"No rapid audio files found in directory, using {len(other_files)} other files")
                
                if not audio_files:
                    logger.warning(f"No audio files found for {level_code}/{topic_name}")
                    continue
                
                for audio_path in audio_files:
                    # Check if we've already processed this file
                    alignment_file = audio_path + "_alignment.json"
                    
                    if os.path.exists(alignment_file):
                        logger.info(f"Skipping already processed file: {audio_path}")
                        continue
                    
                    # Process this audio file
                    success = self.segment_audio_file(
                        audio_path=audio_path,
                        transcript_text=transcript_text,
                        transcript_path=transcript_path,
                        level=level_code,
                        topic_name=topic_name
                    )
                    
                    if success:
                        processed_count += 1
                        logger.info(f"Successfully processed {level_code}/{topic_name}/{os.path.basename(audio_path)}")
                        
                        # If we're limiting to one file and we've processed one, exit
                        if limit_to_one and processed_count >= 1:
                            logger.info("Processed one file, stopping as requested")
                            return processed_count
                    else:
                        logger.error(f"Failed to process {level_code}/{topic_name}/{os.path.basename(audio_path)}")
                        
                    # If we're only processing one file per topic, break after the first success
                    if success and True:  # You can add an option for this if needed
                        break
        
        logger.info(f"Completed processing from master_data. Processed {processed_count} audio files.")
        return processed_count
    
    def _process_directory_scan(self, limit_to_one=True):
        """Process files by scanning the directory structure"""
        processed_count = 0
        
        for level in os.listdir(self.data_dir):
            level_dir = os.path.join(self.data_dir, level)
            if not os.path.isdir(level_dir) or level.startswith('.'):
                continue
            
            for topic in os.listdir(level_dir):
                topic_dir = os.path.join(level_dir, topic)
                if not os.path.isdir(topic_dir) or topic.startswith('.'):
                    continue
                
                # Find transcript file (should be a .txt file)
                transcript_files = [f for f in os.listdir(topic_dir) if f.endswith('.txt')]
                if not transcript_files:
                    logger.warning(f"No transcript file found for {level}/{topic}")
                    continue
                
                transcript_path = os.path.join(topic_dir, transcript_files[0])
                
                # Find audio files (MP3), prioritizing rapid files
                all_files = [f for f in os.listdir(topic_dir) if f.endswith('.mp3')]
                rapid_files = [f for f in all_files if "rapid" in f.lower()]
                other_files = [f for f in all_files if "rapid" not in f.lower()]
                
                audio_files = rapid_files if rapid_files else other_files
                
                if not audio_files:
                    logger.warning(f"No audio files found for {level}/{topic}")
                    continue
                
                # Process one file per topic (prioritizing rapid)
                audio_file = audio_files[0]
                audio_path = os.path.join(topic_dir, audio_file)
                
                # Skip if we've already processed this file
                alignment_file = audio_path + "_alignment.json"
                
                if os.path.exists(alignment_file):
                    logger.info(f"Skipping already processed file: {audio_path}")
                    continue
                
                # Process this audio file
                success = self.segment_audio_file(
                    audio_path=audio_path,
                    transcript_path=transcript_path,
                    level=level,
                    topic_name=topic
                )
                
                if success:
                    processed_count += 1
                    logger.info(f"Successfully processed {level}/{topic}/{audio_file}")
                    
                    # If we're limiting to one file and we've processed one, exit
                    if limit_to_one and processed_count >= 1:
                        logger.info("Processed one file, stopping as requested")
                        return processed_count
                else:
                    logger.error(f"Failed to process {level}/{topic}/{audio_file}")
        
        logger.info(f"Completed processing. Processed {processed_count} audio files.")
        return processed_count
            
    def check_ffmpeg(self):
        """Check if ffmpeg is installed and available"""
        try:
            result = subprocess.run(
                ['ffmpeg', '-version'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            return result.returncode == 0
        except Exception:
            return False
