import os
import boto3
import logging

# Enhanced Logging Setup
logging.basicConfig(
    filename='santex_s3_upload.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)  # Create a logger object


# Main ETL Process

if __name__ == "__main__":
    # Retrieve AWS Credentials
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    bucket_name = os.environ.get('AWS_S3_BUCKET_NAME')

    # File and Folder Configuration
    s3_folder = 'snowpipe_data/' 
    file_name = 'customer_segmentation_data.csv'
    file_path = os.path.join('Python', 'Raw_Data', file_name)  
    object_key = s3_folder + file_name  

    # S3 Client Initialization
    s3 = boto3.client('s3', 
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)

    # Upload File
    try:
        s3.upload_file(file_path, bucket_name, object_key)  
        logger.info(f"File '{file_name}' uploaded successfully to S3 bucket '{bucket_name}' in folder '{s3_folder}'.")

    except FileNotFoundError:
        logger.error(f"File '{file_path}' not found. Please check the path.")
    except boto3.exceptions.S3UploadFailedError as e:
        logger.error(f"Upload failed: {e}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}") 
