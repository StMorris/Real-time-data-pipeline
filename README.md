# Real-Time Data Pipeline using Azure and PySpark

## This repository implements a real-time data pipeline using Azure Event Hubs, PySpark, and Azure Synapse Analytics.

### Features
- Real-time data ingestion from Azure Event Hubs.
- Data transformation and processing using PySpark.
- Storage and analytics integration with Azure Synapse.

### Setup Instructions
1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
2. Configure your Azure settings in `config/settings.json`.
3. Run the scripts in the following order:
   - `data_ingestion.py`
   - `data_transformation.py`
   - `data_storage.py`

### Pipeline Architecture
![Architecture Diagram](/architecture_diagram.png)