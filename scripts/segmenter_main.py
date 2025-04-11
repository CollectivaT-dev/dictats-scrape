#!/usr/bin/env python3
"""
Gencat Audio Segmentation - Main script
"""
import os
import sys
import signal
import traceback
import logging
import argparse
import subprocess
from src.segmenter.gencat_segmenter import GencatSegmenter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_ffmpeg():
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

def check_replicate_api():
    """Check if REPLICATE_API_TOKEN is set"""
    return "REPLICATE_API_TOKEN" in os.environ

def main():
    """Main function for segmentation processing"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Gencat Audio Segmenter")
    parser.add_argument("--data-dir", default="data/downloaded_audio", help="Directory containing the scraped audio files")
    parser.add_argument("--output-dir", default="data/corpus", help="Directory to store output files")
    parser.add_argument("--process-one", action="store_true", help="Process only one file then stop (for testing)")
    parser.add_argument("--specific-file", help="Process a specific audio file (provide full path)")
    parser.add_argument("--transcript-file", help="Transcript file for the specific audio file (provide full path)")
    parser.add_argument("--level", help="Level code for the specific audio file (e.g., 'b1')")
    parser.add_argument("--topic", help="Topic name for the specific audio file")
    args = parser.parse_args()
    
    # Check dependencies
    if not check_ffmpeg():
        print("Error: ffmpeg is not installed or not in PATH")
        print("Please install ffmpeg and make sure it's in your PATH")
        print("  - macOS: brew install ffmpeg")
        print("  - Ubuntu/Debian: apt-get install ffmpeg")
        print("  - Windows: download from https://ffmpeg.org/download.html")
        return 1
        
    if not check_replicate_api():
        print("Error: REPLICATE_API_TOKEN environment variable is not set")
        print("Please set it with: export REPLICATE_API_TOKEN=your_token_here")
        return 1
    
    # Setup graceful shutdown
    def signal_handler(sig, frame):
        print("\nStopping script gracefully.")
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        print("Starting Gencat Audio Segmenter")
        print("This will process audio files and segment them into sentences")
        print("Press Ctrl+C to stop at any time")
        
        # Initialize the segmenter
        segmenter = GencatSegmenter(data_dir=args.data_dir, output_dir=args.output_dir)
        
        # Check if we need to process a specific file
        if args.specific_file:
            if not os.path.exists(args.specific_file):
                print(f"Error: Specific audio file not found: {args.specific_file}")
                return 1
                
            if not args.transcript_file or not os.path.exists(args.transcript_file):
                print(f"Error: Transcript file not specified or not found")
                return 1
                
            if not args.level:
                print("Error: Level code not specified (--level)")
                return 1
                
            if not args.topic:
                print("Error: Topic name not specified (--topic)")
                return 1
                
            print(f"Processing specific file: {args.specific_file}")
            success = segmenter.segment_audio_file(
                args.specific_file,
                args.transcript_file,
                args.level,
                args.topic
            )
            
            if success:
                print(f"Successfully processed specific file")
                print(f"Segmented audio files saved to: {segmenter.audio_output_dir}")
                print(f"CSV file created at: {segmenter.csv_output_file}")
                print(f"Log file: {segmenter.log_file}")
            else:
                print("Failed to process specific file. Check the logs for details.")
                return 1
        
        # Process directory 
        else:
            print(f"Processing audio files in directory: {args.data_dir}")
            print(f"Output directory: {args.output_dir}")
            if args.process_one:
                print("Mode: Process one file only (--process-one)")
            else:
                print("Mode: Process all files")
                
            # Process audio files (limit to one if requested)
            processed_count = segmenter.process_directory(limit_to_one=args.process_one)
            
            if processed_count > 0:
                print(f"Successfully processed {processed_count} audio files")
                print(f"Segmented audio files saved to: {segmenter.audio_output_dir}")
                print(f"CSV file created at: {segmenter.csv_output_file}")
                print(f"Log file: {segmenter.log_file}")
            else:
                print("No audio files were processed. Check the logs for details.")
                return 1
                
        print("\nSegmentation completed successfully.")
        return 0
            
    except KeyboardInterrupt:
        print("\nStopping segmentation gracefully.")
        return 0
    except Exception as e:
        print(f"Error during segmentation: {str(e)}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
