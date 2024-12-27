from azure.eventhub import EventHubConsumerClient

# Function to consume data from Azure Event Hubs in real-time
def ingest_data(event_hub_connection_str, consumer_group, event_hub_name):
    try:
        client = EventHubConsumerClient.from_connection_string(
            conn_str=event_hub_connection_str,
            consumer_group=consumer_group,
            eventhub_name=event_hub_name
        )

        # Callback function to process each event
        def on_event(partition_context, event):
            try:
                print(f"Received event: {event.body_as_str()}")
                # Update checkpoint after processing event
                partition_context.update_checkpoint(event)
            except Exception as e:
                print(f"Error processing event: {e}")

        # Start receiving events
        with client:
            client.receive(on_event=on_event, starting_position="-1")
    except Exception as e:
        print(f"An error occurred during data ingestion: {e}")
        raise e



if __name__ == "__main__":
    import json
    # Load configuration details from settings.json
    try:
        with open("/config/settings.json") as config_file:
            config = json.load(config_file)

        # Call the ingest_data function with parameters from config
        ingest_data(
            event_hub_connection_str=config["event_hub_connection_str"],
            consumer_group=config["consumer_group"],
            event_hub_name=config["event_hub_name"]
        )
    except FileNotFoundError:
        print("Configuration file not found. Please ensure settings.json exists.")
    except Exception as e:
        print(f"Failed to start data ingestion: {e}")

