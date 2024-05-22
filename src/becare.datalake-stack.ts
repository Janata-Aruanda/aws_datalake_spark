import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as glue from '@aws-cdk/aws-glue-alpha';

import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import {RawMedicalReportsRecordsGlueJob } from './jobs/raw-medical-reports-records-job';


// import { RawMedicalReportsGlueJob } from './jobs/raw_medical_report';

export interface BecareDatalakeStackProps extends cdk.StackProps {
	readonly environment?: string;
}

export class BecareDatalakeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: BecareDatalakeStackProps) {
    super(scope, id, props);

	const { environment = "hml" } = props;

    const timelineBucketName = `s3-${environment}-timeline`;
    const jobsBucketName = `s3-${environment}-glue-jobs`;
	// const datalakeBucketName = `${environment}-datalake--use1-az6--x-s3`;
    const datalakeBucketName = `${environment}-datalake`;

    const dataS3Bucket = s3.Bucket.fromBucketName(this, 'timeline-bucket', timelineBucketName);

    const jobsBucket = new s3.Bucket(this, 'jobs-bucket', {
		versioned: false,
		bucketName: jobsBucketName,
		removalPolicy: cdk.RemovalPolicy.DESTROY,
		blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
		autoDeleteObjects: true,
    });

	const database = new glue.Database(this, 'GlueDatabase', {
		databaseName: `${environment}_database`,
		description: `Data Lakehouse database`
	});

	/*
    const datalakeBucket = new s3express.CfnDirectoryBucket(this, 'datalake-bucket', {
		bucketName: datalakeBucketName,
		dataRedundancy: "SingleAvailabilityZone",
		locationName: "use1-az6"
	});
	*/

	const datalakeBucket = new s3.Bucket(this, 'datalake-s3bucket', {
		versioned: false,
		bucketName: datalakeBucketName,
		removalPolicy: cdk.RemovalPolicy.DESTROY,
		blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
		autoDeleteObjects: true,
    });

	// add permissions
	const role = new iam.Role(this, 'glue-role', { assumedBy: new iam.ServicePrincipal('glue.amazonaws.com') });
    role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"));

	const databaseAccessPolicy = new iam.Policy(this, 'database-access-policy', {
		statements: [
		  	new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: [
					'glue:*',
					'glue:StartJobRun',
					's3:*',
					's3:Get*',
					's3:GetObject',
					's3:List*',
					's3:*Object*',
					's3:PutObject'
				], 
				resources: [
					`arn:aws:glue:${this.region}:${this.account}:catalog`,
					`arn:aws:glue:${this.region}:${this.account}:database/*`,
					`arn:aws:glue:${this.region}:${this.account}:table/*`,
					`arn:aws:s3:::*/*`
				], 
		  	}),
		],
	});

	role.attachInlinePolicy(databaseAccessPolicy);
	
	/*
	role.addToPolicy(new PolicyStatement({
		effect: Effect.ALLOW,
		actions: ['s3express:*'],
		resources: [datalakeBucket.attrArn]
	}))
	*/

	const rawMedicalReportsRecordsGlueJob = new RawMedicalReportsRecordsGlueJob(this, 'raw-medical-reports-record-job', {
		environment,
		jobRole: role,
		jobSourceBucket: jobsBucket,
		datalakeBucket: datalakeBucket,
      	databaseName: database.databaseName
	});
	
	new s3deploy.BucketDeployment(this, 'deploy-jobs', {
		sources: [s3deploy.Source.asset('./resources/jobs')],
		destinationBucket: jobsBucket,
		destinationKeyPrefix: 'jobs'
	});
  }
}