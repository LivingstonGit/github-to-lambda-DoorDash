import json
import pandas as pd
import boto3
import os

def lambda_handler(event, context):
    # Retrieve S3 bucket and key from the S3 event
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']

    # Create an S3 client
    s3_client = boto3.client('s3')

    try:
        # Step 1: Read the JSON file into a pandas DataFrame directly from S3
        json_data = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)['Body'].read().decode('utf-8')
        df = pd.json_normalize(json.loads(json_data))

        # Step 2: Filter records where status is "delivered"
        delivered_records = df[df['status'] == 'delivered']

        # Step 3: Convert the filtered DataFrame to JSON format
        json_output = delivered_records.to_json(orient='records')

        # Step 4: Write the filtered JSON data to a new S3 object in the target bucket
        target_s3_bucket = 'doordash-target-zn-assign3'
        target_s3_key = 'delivered_records.json'
        s3_client.put_object(Body=json_output, Bucket=target_s3_bucket, Key=target_s3_key)

        # Step 5: Publish a success message to the SNS topic
        sns_topic_arn = 'arn:aws:sns:ap-south-1:767397926411:aws-sns-assign3'
        sns_client = boto3.client('sns')

        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject='Lambda Execution Success',
            Message='The Lambda function processed the file successfully.'
        )

        return {
            'statusCode': 200,
            'body': json.dumps('Lambda execution completed successfully!')
        }

    except Exception as e:
        # Step 6: Publish a failure message to the SNS topic
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject='Lambda Execution Failure',
            Message=f'The Lambda function encountered an error: {str(e)}'
        )

        return {
            'statusCode': 500,
            'body': json.dumps(f'Lambda execution failed: {str(e)}')
        }
