# File Type Conversion Pipeline

## Overview

This pipeline converts various file types into standardized formats for consistent processing and storage. It accepts documents, images, audio, and video files and converts them to industry-standard formats.

## Key Features

- **Document Conversion**: Converts various document formats to PDF
- **Image Conversion**: Converts various image formats to PNG
- **Audio Conversion**: Converts various audio formats to OGG
- **Video Conversion**: Converts various video formats to MP4
- **Conditional Processing**: Only processes the file types that are provided as input

## How to Use

Upload one or more files of different types (document, image, audio, or video). The pipeline will automatically detect the file type and convert it to the appropriate standardized format.

## Input Variables

- `document`: Document file to convert (optional)
- `image`: Image file to convert (optional)
- `audio`: Audio file to convert (optional)
- `video`: Video file to convert (optional)

## Output Fields

- `document`: Document file converted to PDF format
- `image`: Image file converted to PNG format
- `audio`: Audio file converted to OGG format
- `video`: Video file converted to MP4 format

## Use Cases

This pipeline is ideal for:

- Normalizing file formats across a system
- Ensuring consistent file types for downstream processing
- Preparing files for storage or archival
- Converting files before indexing in a knowledge base
