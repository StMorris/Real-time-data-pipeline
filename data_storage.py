from azure.storage.blob import BlobServiceClient  # Used to interact with Azure Blob Storage for uploading processed data.

# Function to upload processed data to Azure Blob Storage
def store_data(storage_account_connection_str, container_name, file_path):
    try:
        # Create a BlobServiceClient to interact with Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(storage_account_connection_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob="data.parquet")

        # Upload the processed data file to the specified container
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)  # Ensures the blob is replaced if it already exists.
        print("Data loading complete.")
    except FileNotFoundError:
        print(f"File not found: {file_path}. Please check the path.")
    except Exception as e:
        print(f"An error occurred during data loading: {e}")
        raise e

if __name__ == "__main__":
    import json
    try:
        # Load configuration details from settings.json
        with open("../config/settings.json") as config_file:
            config = json.load(config_file)

        # Call the store_data function with parameters from config
        store_data(
            storage_account_connection_str=config["storage_account_connection_str"],
            container_name=config["container_name"],
            file_path=config["output_path"]
        )
    except FileNotFoundError:
        print("Configuration file not found. Please ensure settings.json exists.")
    except Exception as e:
        print(f"Failed to start data storage: {e}")
