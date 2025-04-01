from google.cloud import dataplex_v1
from google.protobuf import struct_pb2
from google.protobuf.timestamp_pb2 import Timestamp
import sys

def update_entry(
    project_id: str, 
    location: str, 
    entry_group_id: str, 
    entry_id: str, 
    aspect_id: str, 
    aspect_name: str,
    aspect_value: str
) -> dataplex_v1.Entry:
    """Method to update Entry located in project_id, location, entry_group_id and with entry_id"""

    # Initialize client that will be used to send requests across threads. This
    # client only needs to be created once, and can be reused for multiple requests.
    # After completing all of your requests, call the "__exit__()" method to safely
    # clean up any remaining background resources. Alternatively, use the client as
    # a context manager.
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    with dataplex_v1.CatalogServiceClient() as client:
        # The resource name of the Entry
        name = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group_id}/entries/{entry_id}"
    
        entry = dataplex_v1.Entry(
            name=f"{name}",
            update_time = timestamp,
            #entry_source = {"update_time": timestamp},
            aspects={
                f"{project_id}.{location}.{aspect_id}": dataplex_v1.Aspect(
                    aspect_type=f"projects/{project_id}/locations/{location}/aspectTypes/{aspect_id}",
                    update_time = timestamp,
                    data=struct_pb2.Struct(
                        fields={
                            # "Generic" Aspect Type have fields called "type" and "system.
                            # The values below are a sample of possible options.
                            f"{aspect_name}": struct_pb2.Value(
                                string_value=f"{aspect_value}"
                            )
                        }
                    ),
                )
            },
        )

        # Update mask specifies which fields will be updated.
        # For more information on update masks, see: https://google.aip.dev/161
        #update_mask = {"paths": ["aspects", "entry_source.description"]}
        update_mask = {"paths": ["aspects"]}
        return client.update_entry(entry=entry, update_mask=update_mask)


if __name__ == "__main__":
    if len(sys.argv) != 8:
        print("Error: This script requires 7 arguments:")
        print("  project_id region entry_id $entry_group_id aspect_id aspect_name aspect_value")
        sys.exit(1)  # Exit with an error code

    project_id = sys.argv[1]
    region = sys.argv[2]
    entry_id = sys.argv[3]
    entry_group_id = sys.argv[4]
    aspect_id = sys.argv[5]
    aspect_name = sys.argv[6]
    aspect_value = sys.argv[7]


    # TODO(developer): Replace these variables before running the sample.
    #project_id = "dataplex-demo032025v4"
    # Available locations: https://cloud.google.com/dataplex/docs/locations
    #location = "us-central1"
    #entry_group_id = "oda_custom_entry_group"
    #entry_id = "retail_bucket"

    print(f"project id: {project_id}")
    print(f"region: {region}")
    print(f"entry id: {entry_id}")
    print(f"entry group id: {entry_group_id}")
    print(f"aspect id: {aspect_id}")
    print(f"aspect name: {aspect_name}")
    print(f"aspect value: {aspect_value}")

    

    updated_entry = update_entry(project_id, region, entry_group_id, entry_id, aspect_id, aspect_name, aspect_value)
    print(f"Successfully updated entry: {updated_entry.name}")


    #update_aspect(project, region, entry, entrygroup, aspectid, aspectname, aspectvalue)

