# import json
# import boto3

# def lambda_handler(event, context):
#     # Log the event in CloudWatch
#     print("Event: ", json.dumps(event))
    
#     # Extract S3 bucket and object details
#     s3 = boto3.client('s3')
#     bucket = event['Records'][0]['s3']['bucket']['name']
#     key = event['Records'][0]['s3']['object']['key']
    
#     # Process the image (log details in this case)
#     response = s3.get_object(Bucket=bucket, Key=key)
#     print(f"Processing file {key} from bucket {bucket}")

#     # New print statement for verification
#     print(f"Lambda function updated! Now processing {key} from {bucket}.")
#     print("again updated")

#     return {
#         'statusCode': 200,
#         'body': json.dumps('Image processed successfully!')
#     }




import json
import boto3
from pymongo import MongoClient

def lambda_handler(event, context):
    # Log the event for debugging purposes
    print("Event: ", json.dumps(event))
    
    # Connect to S3 and extract details of the uploaded file
    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Connect to MongoDB (replace with your MongoDB connection string)
    client = MongoClient("mongodb+srv://ShravaniAnilPatil:Shweta2509@cluster0.tspoa.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client['e-yojana']  # Replace with your database name
    collection = db['schemes']  # The name of your collection

    # Assume the user ID or email is embedded in the S3 key or event metadata (you can adjust this based on how the key is structured)
    user_id = extract_user_id_from_key(key)  # You will need to define this based on how user IDs are associated with uploads
    
    # Find the user's scheme document in the MongoDB collection
    user_scheme = collection.find_one({"user_id": user_id})
    
    if user_scheme:
        # Add the new file (document) to the documents array
        new_document = {
            "file_key": key,  # You can store the S3 key as a reference to the file
            "bucket": bucket,
            "upload_time": event['Records'][0]['eventTime'],
            "status": "pending"  # Set an initial status if needed
        }
        
        # Update the MongoDB document by appending to the 'documents' array
        collection.update_one(
            {"user_id": user_id},
            {"$push": {"documents": new_document}}
        )
        
        print(f"Document added to user {user_id}'s scheme.")
    else:
        print(f"No scheme found for user {user_id}.")
    
    return {
        'statusCode': 200,
        'body': json.dumps('File processed and added to MongoDB!')
    }

def extract_user_id_from_key(key):
    # Define how you will extract the user_id from the S3 key or event
    # For example, if the key is structured like "user_id/somefile.pdf", split on "/"
    return key.split('/')[0]  # This is just an example. Adjust as needed.
