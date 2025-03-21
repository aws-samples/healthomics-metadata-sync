import boto3
from botocore.config import Config
import json
import os

METADATA_TABLE = os.getenv('HEALTHOMICS_STORE_METADATA_TABLE_NAME')

def update_set_status(read_set_arn, new_status, ddb_table):
    """
    updates the status of an existing entry based on the read set ARN
    """

    ddb_table.update_item(
        Key={
            'set_arn': read_set_arn
        },
        UpdateExpression = 'SET #status = :new_status',
        ExpressionAttributeNames={
            '#status':'status'
        },
        ExpressionAttributeValues={
            ':new_status': new_status
        }
    )

def delete_set(read_set_arn, ddb_table):
    """
    deletes the read set entry if it's flagged as deleted
    """

    ddb_table.delete_item(
        Key={
            'set_arn': read_set_arn
        }
    )

def write_new_set(read_set_id, store_id, omics_client, ddb_table):
    """
    creates a new entry, or does a full update, each time a read set is set to active
    """

    set_metadata_response = omics_client.get_read_set_metadata(id=read_set_id,sequenceStoreId=store_id)
    
    
    # read set metadata structure
    read_set_item = {
        'set_arn': set_metadata_response.get('arn'),
        'set_id': set_metadata_response.get('id'),
        'set_type': set_metadata_response.get('fileType'),
        'set_name':set_metadata_response.get('name'),
        'set_description': set_metadata_response.get('description'),
        'set_reference_arn': set_metadata_response.get('referenceArn'),
        'set_sample_id': set_metadata_response.get('sampleId'),
        'set_subject_id': set_metadata_response.get('subjectId'),
        'set_status':set_metadata_response.get('status')
    }

    # tags
    tag_response = omics_client.list_tags_for_resource(resourceArn=set_metadata_response.get('arn'))
    tags = tag_response.get('tags',{})
    if tags:
        read_set_item['tags'] = tag_response.get('tags',{})

    
    # file metadata
    file_response = set_metadata_response.get('files')
    file_info = []

    if(file_response):
        for f in file_response.keys():
            f_info = file_response.get(f)
            file_info.append({
                'file_path': f_info.get('s3Access').get('s3Uri'),
                'etag': set_metadata_response.get('etag').get(f,''),
                'file_type': f,
                'content_length': f_info.get('contentLength'),
                'part_size': f_info.get('partSize'),
                'total_parts': f_info.get('totalParts')
            })
    
    # store info
    store_metadata_response = omics_client.get_sequence_store(id=store_id)
    store_info = {
        'store_arn': store_metadata_response.get('arn'),
        'store_id': store_metadata_response.get('id'),
        'store_type': 'sequence_store',
        'store_name': store_metadata_response.get('name'),
        'store_ap_arn': store_metadata_response.get('s3Access').get('s3AccessPointArn'),
        'store_uri': store_metadata_response.get('s3Access').get('s3Uri')
    }

    read_set_item['files'] = file_info
    read_set_item['store'] = store_info
    
    print(read_set_item)
    ddb_table.put_item(Item=read_set_item)

def set_management(read_set_id, store_id, read_set_arn, status, omics_client, ddb_table):
    """
    Main configuration to determine what part of the metadata in a store needs to be configured
    """

    # write full if status active
    if status == 'ACTIVE':
        write_new_set(read_set_id, store_id, omics_client, ddb_table)
        return f'Set {read_set_arn} row created or fully updated for status {status}'
    
    # delete if status delete
    elif status == 'DELETED':
        delete_set(read_set_arn, ddb_table)
        return f'Set {read_set_arn} row deleted for status {status}'
    
    # update statuses
    elif status in ['ACTIVATING','ARCHIVED','DELETING',]:
        update_set_status(read_set_arn, status, ddb_table)
        return f'Set {read_set_arn} row status updated for status {status}'
    
    # do nothing 
    else:
        return "No updates needed"




def handler(event, context):
    """
    Main lambda handler for file status polling and cleanup on completion
    """

    message = ''

    try:
        #configure retries with standard backoff 
        config = Config(retries = {'max_attempts': 10,'mode': 'standard'})

        # resource and client creation
        ddb_resource = boto3.resource('dynamodb', config=config)
        table_resource = ddb_resource.Table(METADATA_TABLE)
        omics_client = boto3.client('omics', config=config)

        for record in event['Records']:
           # Parse the message body which contains the EventBridge event details
           detail = json.loads(record['body'])
            
           if detail:
               read_set_id = detail.get('id')
               store_id = detail.get('sequenceStoreId')
               read_set_arn = detail.get('arn')
               status = detail.get('status')
    
               message = set_management(
                   read_set_id, 
                   store_id, 
                   read_set_arn, 
                   status, 
                   omics_client, 
                   table_resource
               )
               print(message)
               
    except Exception as e:
       print(f"Error processing message: {str(e)}")
       raise e

    return {
        'statusCode': 200,
        'body': message
    }
