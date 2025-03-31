
# Viksit Bharat Data Analysis

Economic indicator analysis from Ministry of Statistics and Programme Implementation (MOSPI) data for India's growth story.

## Team

- **Team Name:** STATUS-200
- **Team Lead:** Pratik Rana

## Overview

This project analyzes key economic indicators from India's national accounts data to understand growth patterns, investment trends, and sectoral performance. The analysis covers Index of Industrial Production (IIP), Gross Capital Formation (GCF), Net Value Added (NVA), and Gross Value Added (GVA) metrics.

## Data Sources

The analysis uses official data from MOSPI (Ministry of Statistics and Programme Implementation):

- `1.10.xlsx`: Gross Capital Formation data
- `1.6B.xlsx`: Sectoral GVA growth percentages
- `1.7.xlsx`: Net Value Added data
- `8.18.1.xlsx`: Quarterly GVA data
- `IIP_data.xlsx`: Index of Industrial Production data

## Features

- Comprehensive cleaning and preprocessing of complex economic datasets
- 20+ visualizations showcasing different aspects of economic performance
- Time series analysis including seasonal decomposition and moving averages
- Cross-sectoral comparisons and growth trend analysis

## Key Visualizations

1. Overall IIP trend with COVID-19 impact analysis
2. Sectoral GVA growth heatmap
3. Investment intensity ratio by sector
4. Interactive investment vs. growth bubble chart
5. COVID-19 recovery index by sector
6. Sectoral composition evolution over time
7. Growth distribution analysis across policy periods

## Requirements

See `requirements.txt` for the complete list of dependencies. Key packages include:

- pandas
- numpy
- matplotlib
- seaborn
- plotly (optional, for interactive visualizations)

## Installation

```bash
# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

You can run the analysis in two ways:

### Using the Jupyter Notebook

```bash
jupyter notebook STATUS-200_Code.ipynb
```

### Using the Python Script

```bash
python STATUS-200_Code.py
```

## Project Structure

```
├── data/                  # Data files (XLSX)
├── visualizations/        # Generated visualization outputs
├── STATUS-200_Code.ipynb  # Jupyter notebook with code and outputs
├── STATUS-200_Code.py     # Python script version
├── STATUS-200_Report.pdf  # Analysis report
├── STATUS-200_Data-Sources.pdf  # Data source documentation
├── STATUS-200_Visualizations.pdf  # Visualization showcase
└── requirements.txt       # Dependencies
```

## Output

The visualizations are saved to the visualizations directory and include both static PNG files and interactive HTML files.
