import boto3
from botocore.config import Config
import json
import time
import argparse

def backfill_set(read_set, store_info, omics_client, ddb_table, wait):
    time.sleep(wait)
    read_set_id = read_set.get('id')
    store_id = store_info.get('store_id')
    
    #print(f'{read_set_id}, {store_id}')
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
                'etag': set_metadata_response.get('etag',{}).get(f,''),
                'file_type': f,
                'content_length': f_info.get('contentLength'),
                'part_size': f_info.get('partSize'),
                'total_parts': f_info.get('totalParts')
            })
    
    # store info

    read_set_item['files'] = file_info
    read_set_item['store'] = store_info
    
    #print(read_set_item)
    ddb_table.put_item(Item=read_set_item)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--seq-store-id', type=str, required=True, 
                        help='The ID of the sequence store to sync.')
    parser.add_argument('-t', '--table', type=str, required=True, 
                        help='The table name to write to in dynamo.  This should exist in the same region as the sequence store.')
    parser.add_argument('-r', '--region', type=str, required=False, 
                        help='The region where the sequence stores and table are in.')
    parser.add_argument('--profile', type=str, 
                        help='(optional) The profile name for boto3 to use if you do not want it to use the default profile configured.')
    
    args = parser.parse_args()

    print('Sequence Store ID:', args.seq_store_id)
    # print optional inputs only if they're specified
    print('Table Name:', args.table)
    print('Region:', args.region)
    if args.profile:
        print('Profile:', args.profile)


    # default parameters
    table_name = args.table
    store_id = args.seq_store_id
    max_results = 100
    wait = 0.025 # prevent TPS hit
    processed = 0

    #configure retries with standard backoff 
    config = Config(retries = {'max_attempts': 10,'mode': 'standard'})

    # resource creation
    aws_session = boto3.session.Session(profile_name=args.profile)
    ddb_resource = aws_session.resource('dynamodb', region_name=args.region, config=config)
    table_resource = ddb_resource.Table(table_name)
    omics_client = aws_session.client('omics', region_name=args.region, config=config)

    
    set_list_raw = omics_client.list_read_sets(sequenceStoreId=store_id, maxResults=max_results)
    next_token = set_list_raw.get('nextToken')
    set_list = set_list_raw.get('readSets')

    if set_list:
        store_metadata_response = omics_client.get_sequence_store(id=store_id)
        store_info = {
            'store_arn': store_metadata_response.get('arn'),
            'store_id': store_metadata_response.get('id'),
            'store_type': 'sequence_store',
            'store_name': store_metadata_response.get('name'),
            'store_ap_arn': store_metadata_response.get('s3Access').get('s3AccessPointArn'),
            'store_uri': store_metadata_response.get('s3Access').get('s3Uri')
        }

        while(next_token):
            # get read set info
            for rs in set_list:
                backfill_set(rs, store_info, omics_client, table_resource, wait)
            
            # print progress
            processed += len(set_list)
            print(f'completed {processed} read sets')

            # get next item
            next_token = set_list_raw.get('nextToken')
            if not next_token:
                break
            
            set_list_raw = omics_client.list_read_sets(sequenceStoreId=store_id, nextToken=next_token, maxResults=max_results)
            set_list = set_list_raw.get('readSets')
