import os
import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# Elasticsearch Configuration
ES_HOST = os.environ['ES_HOST']
ES_INDEX = os.environ['ES_INDEX']
REGION = os.environ.get('AWS_REGION', 'us-east-1')

# AWS Authentication
credentials = boto3.Session().get_credentials()
aws_auth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    REGION,
    'es',
    session_token=credentials.token
)

# Initialize Elasticsearch client
es_client = Elasticsearch(
    hosts=[{'host': ES_HOST, 'port': 443}],
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    http_auth=aws_auth
)

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Main handler for processing search queries and generating pre-signed URLs.
    """
    try:
        # Debug: Log the incoming event
        print("Received event:", json.dumps(event))

        # Determine the query source
        if "queryStringParameters" in event:  # API Gateway input
            search_query = event.get("queryStringParameters", {}).get("q", "").strip()
        elif "inputTranscript" in event:  # Lex input
            search_query = event.get("inputTranscript", "").strip()
        else:
            return build_response(
                400, {"message": "Invalid input. No search query provided."}
            )

        if not search_query:
            return build_response(
                400, {"message": "Search query is missing or empty."}
            )

        # Elasticsearch query
        must_clauses = [{"match": {"labels": search_query}}]
        search_body = {
            "query": {
                "bool": {
                    "must": must_clauses
                }
            }
        }

        es_response = es_client.search(index=ES_INDEX, body=search_body)

        # Extract results and generate pre-signed URLs
        results = []
        for hit in es_response["hits"]["hits"]:
            object_key = hit["_source"]["objectKey"]
            bucket_name = hit["_source"]["bucket"]

            # Generate pre-signed URL
            try:
                presigned_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': object_key},
                    ExpiresIn=3600  # URL expires in 1 hour
                )
            except Exception as e:
                print(f"Error generating pre-signed URL for {object_key}: {e}")
                presigned_url = None

            results.append({
                "objectKey": object_key,
                "bucket": bucket_name,
                "labels": hit["_source"]["labels"],
                "url": presigned_url  # Add pre-signed URL to the response
            })

        # Prepare the response
        if results:
            return build_response(
                200, {"message": f"Found {len(results)} result(s).", "results": results}
            )
        else:
            return build_response(
                200, {"message": "No matching results found.", "results": []}
            )

    except Exception as e:
        print(f"Error: {e}")
        return build_response(
            500, {"message": "An error occurred while processing your request.", "error": str(e)}
        )


def build_response(status_code, body):
    """
    Helper function to construct a valid Lambda Proxy response.
    """
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"  # Adjust for your specific CORS policy
        },
        "body": json.dumps(body)
    }
