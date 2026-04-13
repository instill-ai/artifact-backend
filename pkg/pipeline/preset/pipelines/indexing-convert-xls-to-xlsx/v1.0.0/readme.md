# XLS to XLSX Conversion Pipeline

## Overview

This pipeline converts legacy XLS (Excel 97-2003) files to modern XLSX format using LibreOffice. The converted XLSX is then processed directly by excelize for structured markdown generation, preserving sheet names and tabular structure.

## Key Features

- **XLS to XLSX**: Converts legacy spreadsheets to modern Office Open XML format
- **Structure Preservation**: Sheet names, cell values, and tabular layout are retained
- **Deterministic Output**: No AI involved — conversion is mechanical and reproducible

## Input Variables

- `document`: XLS document file to convert

## Output Fields

- `document`: Document file converted to XLSX format

## Use Cases

This pipeline is used internally by the knowledge base file processing workflow to standardize XLS files before structured parsing with excelize.
