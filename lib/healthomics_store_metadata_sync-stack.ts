import path = require('path');

import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';

import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda_event_sources from 'aws-cdk-lib/aws-lambda-event-sources';

export interface HealthomicsStoreMetadataSyncStackProps extends cdk.StackProps {
  dynamoTableName?: string,
  SQSQueueName?: string,
}

export class HealthomicsStoreMetadataSyncStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: HealthomicsStoreMetadataSyncStackProps) {
    super(scope, id, props);

    // if a table name is not provided, create a dynamo table 

    let dynamoTableName: string; 
    let dynamoTableArn: string; 

    if (props.dynamoTableName) {
      const dynamoTable = dynamodb.Table.fromTableName(this, 'metadataTable', props.dynamoTableName
        );

      dynamoTableName = props.dynamoTableName
      dynamoTableArn = dynamoTable.tableArn
    } else {
      const dynamoTable = new dynamodb.Table(this, 'metadataTable', {
        tableName: 'healthomics_set_metadata',
        partitionKey: {name: 'set_arn', type: dynamodb.AttributeType.STRING},
        sortKey: {name: 'set_status', type:dynamodb.AttributeType.STRING}
      });

      dynamoTableName = dynamoTable.tableName
      dynamoTableArn = dynamoTable.tableArn
    }

    let SQSQueueName: string;
    let SQSQueueArn: string;
    let SQSQueueUrl: string;
    let metadataQueue: sqs.IQueue;

    if (props.SQSQueueName) {
      const accountId = cdk.Stack.of(this).account;
      const region = cdk.Stack.of(this).region;
      const queueArn = `arn:aws:sqs:\${region}:\${accountId}:\${props.SQSQueueName}`;
     
      metadataQueue = sqs.Queue.fromQueueAttributes(this, 'metadataQueue', {
        queueName: props.SQSQueueName,
	queueArn: queueArn       
      });
      SQSQueueName = props.SQSQueueName;
      SQSQueueArn = metadataQueue.queueArn;
      SQSQueueUrl = metadataQueue.queueUrl;
    } else {
      metadataQueue = new sqs.Queue(this, 'metadataQueue', {
        queueName: 'healthomics_set_queue.fifo',
        fifo: true,
        contentBasedDeduplication: true,
        deliveryDelay: cdk.Duration.seconds(0),
        retentionPeriod: cdk.Duration.days(14),
	visibilityTimeout: cdk.Duration.minutes(15)
      });
      SQSQueueName = metadataQueue.queueName;
      SQSQueueArn = metadataQueue.queueArn;
      SQSQueueUrl = metadataQueue.queueUrl;
    }


    console.log(`DynamoDB Table ARN: ${dynamoTableArn} and name ${dynamoTableName}`);
    console.log(`SQS Queue ARN: ${SQSQueueArn} and name ${SQSQueueName}`);


    // stage manifest  lib/aho_importer_1_split_raw_import
    const fnMetadataWriter = new lambda.Function(this, 'healhtomicsMetadataWriter', {
      runtime: lambda.Runtime.PYTHON_3_12,
      code: lambda.Code.fromAsset(path.join(__dirname, 'lambda/aho_metadata_writer')),
      handler: 'handler.handler',
      timeout: cdk.Duration.minutes(10)
    });

    fnMetadataWriter.addEnvironment('HEALTHOMICS_STORE_METADATA_TABLE_NAME', dynamoTableName);

    fnMetadataWriter.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'omics:GetReadSetMetadata',
        'omics:GetSequenceStore',
        'omics:ListTagsForResource'
      ],
      resources: [
        '*'
      ]
    }));
    
    fnMetadataWriter.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'sqs:SendMessage',
        'sqs:ReceiveMessage',
        'sqs:DeleteMessage',
        'sqs:GetQueueAttributes'
      ],
      resources: [
        SQSQueueArn
      ]
    }));

    fnMetadataWriter.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'dynamodb:PutItem',
        'dynamodb:DeleteItem',
        'dynamodb:UpdateItem'
      ],
      resources: [
        dynamoTableArn
      ]
    }));

    const ruleTriggerMetadataWrite = new events.Rule(this, 'healthomicsTriggerMetadataWrite', {
      ruleName: 'healthomicsTriggerMetadataWrite',
      eventPattern: {
        source: ['aws.omics'],
        detailType: ['Read Set Status Change']
      }
    });

    const eventSource = new lambda_event_sources.SqsEventSource(metadataQueue, {
	  batchSize: 1,
	  enabled: true
	});

    ruleTriggerMetadataWrite.addTarget(new targets.SqsQueue(metadataQueue, {
 	  message: events.RuleTargetInput.fromObject({
	  detail: events.RuleTargetInput.fromEventPath('\$.detail'),
          detailType: events.RuleTargetInput.fromEventPath('\$.detail-type'),
          source: events.RuleTargetInput.fromEventPath('\$.source'),
          time: events.RuleTargetInput.fromEventPath('\$.time')
 	 }),
  	messageGroupId: 'healthomics-metadata-sync'
     }));
	  
    fnMetadataWriter.addEventSource(eventSource);

  }
}
