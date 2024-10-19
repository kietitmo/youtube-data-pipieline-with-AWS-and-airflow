import json
import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')
    
    # Name of your Glue job
    glue_job_name = 'youtube-trending-videos-job'
    
    try:
        # Start the Glue job
        response = glue.start_job_run(
            JobName=glue_job_name,
        )
        
        # Return the job run ID
        job_run_id = response['JobRunId']
        return {
            'statusCode': 200,
            'body': json.dumps(f'Glue job started successfully with JobRunId: {job_run_id}')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error starting Glue job: {str(e)}')
        }
