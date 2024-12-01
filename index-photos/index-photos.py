import json
import boto3
import logging
from base64 import b64encode
import imghdr
from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import base64


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
rekognition_client = boto3.client('rekognition')


# Elasticsearch configuration
host = os.environ['ES_HOST']
region = "us-east-1"

# AWS Requests Authentication for Elasticsearch
credentials = boto3.Session().get_credentials()
aws_auth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    'es',
    session_token=credentials.token
)

# Initialize Elasticsearch client
es_client = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    http_auth=aws_auth
)

def lambda_handler(event, context):
    try:
        # Log the incoming event
        logger.info(f"Received event: {json.dumps(event)}")

        # Extract S3 bucket and object details
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        logger.info(f"Processing file: {object_key} from bucket: {bucket_name}")

        # Get S3 object
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        image_data = s3_object['Body'].read()

        # Validate image data
        if not image_data or len(image_data) == 0:
            raise ValueError("S3 object is empty or corrupted.")
        logger.info(f"Image size: {len(image_data)} bytes")

        # Check Content-Type
        content_type = s3_object['ContentType']
        supported_types = ['image/jpeg', 'image/png']
        if content_type not in supported_types:
            raise ValueError(f"Unsupported Content-Type: {content_type}")
        logger.info(f"Content-Type: {content_type}")

        response = rekognition_client.detect_labels(Image={'Bytes': image_data})
        
        rekognition_labels = [label['Name'] for label in response['Labels']]
        logger.info(f"Rekognition Labels: {rekognition_labels}")

        # Retrieve custom labels from object metadata
        metadata = s3_object.get('Metadata', {})
        custom_labels = metadata.get('customlabels', '')
        custom_labels_list = [label.strip() for label in custom_labels.split(',')] if custom_labels else []
        logger.info(f"Custom Labels: {custom_labels_list}")

        # Combine Rekognition labels with custom labels
        all_labels = list(set(rekognition_labels + custom_labels_list))
        logger.info(f"Combined Labels: {all_labels}")

        # Prepare the document for Elasticsearch
        document = {
            "objectKey": object_key,
            "bucket": bucket_name,
            "labels": all_labels
        }

        # Index the document in Elasticsearch
        es_response = es_client.index(index='photos', id=object_key, body=document)
        logger.info(f"Elasticsearch Response: {es_response}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully processed image',
                'labels': all_labels
            })
        }

    except rekognition_client.exceptions.InvalidImageFormatException as e:
        logger.error(f"Rekognition Invalid Image Format: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps(f"Invalid image format: {str(e)}")
        }

    except ValueError as ve:
        logger.error(f"Validation Error: {ve}")
        return {
            'statusCode': 400,
            'body': json.dumps(f"Validation error: {str(ve)}")
        }

    except Exception as e:
        logger.error(f"Unexpected Error: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing the image: {str(e)}")
        }
