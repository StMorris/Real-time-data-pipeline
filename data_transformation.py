from pyspark.sql import SparkSession

# Function to process and transform data
def transform_data(input_path, output_path):
    try:
        # Initialize a Spark session for distributed data processing
        spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

        # Load data from the specified input path
        data = spark.read.json(input_path)

        # Perform data transformations
        transformed_data = data.selectExpr("id", "timestamp", "CAST(value AS double) as value")

        # Save transformed data to the specified output path
        transformed_data.write.parquet(output_path, mode="overwrite")
        print("Data transformation complete.")
    except Exception as e:
        print(f"An error occurred during data transformation: {e}")
        raise e



if __name__ == "__main__":
    import json
    try:
        # Load configuration details from settings.json
        with open("../config/settings.json") as config_file:
            config = json.load(config_file)

        # Call the transform_data function with paths from config
        transform_data(
            input_path=config["input_path"],
            output_path=config["output_path"]
        )
    except FileNotFoundError:
        print("Configuration file not found. Please ensure settings.json exists.")
    except Exception as e:
        print(f"Failed to start data transformation: {e}")
