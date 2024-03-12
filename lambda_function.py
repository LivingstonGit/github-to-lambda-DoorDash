import json
import pandas as pd
import boto3

def lambda_handler(event, context):
    # Retrieve S3 bucket and key from the S3 event
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']

    # Generate file paths for input and output
    input_file_path = f'/tmp/{s3_key.split("/")[-1]}'  # Temporary file path in Lambda environment
    output_file_path = f'/tmp/delivered_records.json'  # Temporary file path in Lambda environment

    # S3 client
    s3_client = boto3.client('s3')

    try:
        # Step 1: Read the JSON file into a pandas DataFrame
        s3_client.download_file(s3_bucket, s3_key, input_file_path)
        with open(input_file_path, 'r') as file:
            json_data = json.load(file)

        df = pd.DataFrame(json_data)

        # Step 2: Filter records where status is "delivered"
        delivered_records = df[df['status'] == 'delivered']

        # Step 3: Write the filtered DataFrame to a new JSON file
        delivered_records.to_json(output_file_path, orient='records')

        # Optionally, you can move the output file to a specific S3 bucket
        target_s3_bucket = 'doordash-target-zn-assign3'
        target_s3_key = 'delivered_records.json'
        s3_client.upload_file(output_file_path, target_s3_bucket, target_s3_key)

        # Step 4: Publish a success message to the SNS topic
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
        # Step 5: Publish a failure message to the SNS topic
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject='Lambda Execution Failure',
            Message=f'The Lambda function encountered an error: {str(e)}'
        )

        return {
            'statusCode': 500,
            'body': json.dumps(f'Lambda execution failed: {str(e)}')
        }