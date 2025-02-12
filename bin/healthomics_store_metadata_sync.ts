#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import {HealthomicsStoreMetadataSyncStack} from '../lib/healthomics_store_metadata_sync-stack';

const app = new cdk.App();

console.log("dynamoTableName: " + app.node.tryGetContext('dynamoTableName'));
console.log("SQSQeueuName: " + app.node.tryGetContext('SQSQueueName'));


new HealthomicsStoreMetadataSyncStack(app, 'HealthomicsStoreMetadataSyncStack', {
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
  dynamoTableName: app.node.tryGetContext('dynamoTableName'); SQSQueueName: app.node.tryGetContext('SQSQueueName')
});
