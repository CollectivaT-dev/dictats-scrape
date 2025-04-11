# dictats-scrape

Tools for building a sentence-segmented speech corpus from Catalan government's language learning resource [_Dictats en línia_](https://llengua.gencat.cat/ca/serveis/aprendre_catala/recursos-per-al-professorat/dictats-en-linia/).

## License Notice ⚠️

**Important:** While this code can scrape and process publicly accessible materials, the content itself is subject to a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International (CC BY-NC-ND 4.0) license as stated in the [credits page](https://llengua.gencat.cat/ca/serveis/aprendre_catala/recursos-per-al-professorat/dictats-en-linia/credits/).

This license means:
- You must give appropriate credit
- You cannot use the materials for commercial purposes
- You cannot distribute modified versions or derivatives of the materials

**Therefore, while this tool can create a corpus for research purposes, the resulting data cannot be:**
- Used to train speech models (considered a derivative work)
- Used commercially
- Redistributed in modified form

The code itself is open source and can be freely used and modified.

## Overview

This project consists of two main components:
1. **Scraper**: Downloads audio files and transcripts from Generalitat de Catalunya language resources
2. **Segmenter**: Processes the audio files to segment them into sentence-level audio clips with aligned transcripts

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/dictats-scrape.git
cd dictats-scrape

# Install dependencies
pip install -r requirements.txt

# Set up project for development
pip install -e .

# Set up API key for Replicate (required for audio alignment)
export REPLICATE_API_TOKEN=your_token_here
```

## Usage

### Step 1: Scrape Audio and Transcripts

Run the main scraper script:

```bash
python scripts/gencat_main.py
```

This will:
- Download audio files from different Catalan learning levels (B1, B2, C1, C2)
- Extract and save transcripts
- Generate metadata for each topic
- Create a structured directory of files in `downloaded_audio/`

### Step 2: Segment Audio Files

Run the segmenter script:

```bash
python scripts/segmenter_main.py
```

This will:
- Process the downloaded audio files
- Use an alignment API to segment audio into sentences
- Create a corpus of sentence-level audio clips with transcripts
- Generate a CSV file with all segments

## Command Line Options

### For the segmenter

Process only one file (for testing):
```bash
python scripts/segmenter_main.py --process-one
```

Specify custom directories:
```bash
python scripts/segmenter_main.py --data-dir custom_input --output-dir custom_output
```

Process a specific file:
```bash
python scripts/segmenter_main.py --specific-file path/to/audio.mp3 --transcript-file path/to/transcript.txt --level b1 --topic topic_name
```

## Requirements

- Python 3.7+
- ffmpeg (must be installed and in PATH)
- Replicate API access (for audio alignment)
- Chrome/Chromium (for web scraping with Selenium)

## Project Structure

- `scripts/`: Executable scripts
  - `gencat_main.py`: Main script for running the scraper
  - `segmenter_main.py`: Main script for running the segmenter
- `src/`: Source code
  - `scraper/`: Scraper components
    - `gencat_scraper.py`: Main scraper class for downloading content
    - `progress_manager.py`: Tracks progress of scraping
    - `summary_manager.py`: Generates summaries of downloaded content
  - `segmenter/`: Segmenter components
    - `gencat_segmenter.py`: Audio processing and segmentation
  - `utils/`: Utility functions
- `data/`: Data storage
  - `downloaded_audio/`: Raw downloaded audio files and transcripts
  - `corpus/`: Processed, segmented audio files and transcripts

## Ethical Use

This tool is provided for educational and research purposes only. Users are responsible for complying with the license terms of the materials they access.
