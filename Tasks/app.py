import boto3
import json


s3_client = boto3.client('s3')
sqs = boto3.client('sqs')
rekognition = boto3.client('rekognition')
dynamodb = boto3.resource('dynamodb')
queue_url = 'https://sqs.us-east-1.amazonaws.com/826441246835/taskqueue'
table = dynamodb.Table('PdfTable')


def lambda_handler(event, context):
    print(event['Records'])
    if event['Records'][0]['eventSource'] == 'aws:sqs':
        # Handle SQS event
        message = json.loads(event['Records'][0]['body'])
        print(message)
        bucket = message['bucket']
        filename = message['filename']
        print("bucket: " + bucket)
        print("fileName: " + filename)
        rekognition_response = rekognition.detect_text(
            Image={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': filename
                }
            }
        )
        print("rekognition response: " + str(rekognition_response))
        detected_text = [text_detection['DetectedText'] for text_detection in rekognition_response['TextDetections']]
        print("detected_labels: " + str(detected_text))
        try:
            table.put_item(
                Item={
                    'file_name': filename,
                    'detected_labels': json.dumps(detected_text),
                }
            )
            print('DB insert success')
            return {
                'statusCode': 200,
                'body': json.dumps('DB insert success')
            }
        except Exception as e:
            print(e)
            receipt_handle = message['receiptHandle']
            response = sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            return {
                'statusCode': 200,
                'body': json.dumps(str(e))
            }
        
    elif event['Records'][0]['eventSource'] == 'aws:s3':
        # Handle S3 event
        try:
            bucket = event['Records'][0]['s3']['bucket']['name']
            filename = event['Records'][0]['s3']['object']['key']
            message_body = json.dumps({"bucket": bucket, "filename": filename})
            if str(filename).endswith(".pdf"):
                return {
                'statusCode': 400,
                'body': json.dumps("your file is not a valid format")
            }      
            response = sqs.send_message(QueueUrl=queue_url, MessageBody=message_body)
            print(f'Message sent to SQS: {response["MessageId"]}')
            return {
                'statusCode': 200,
                'body': json.dumps('File name inserted into SQS queue.')
            }
        except Exception as e:
            print(e)
            return {
                'statusCode': 400,
                'body': json.dumps(str(e))
            }