import * as cdk from 'aws-cdk-lib';
import * as glue from '@aws-cdk/aws-glue-alpha';

import { Construct } from 'constructs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Role } from 'aws-cdk-lib/aws-iam';

export interface PrdReportNestleJobProps extends cdk.StackProps {
	readonly environment: string
  readonly jobRole: Role
  readonly jobSourceBucket: Bucket
  readonly datalakeBucket: Bucket
}

export class PrdReportNestleJob extends Construct {
  constructor(scope: Construct, id: string, props: PrdReportNestleJobProps) {
    super(scope, id);

    const { 
      jobRole,
      jobSourceBucket
    } = props;

    new glue.Job(this, 'prd-relatorio_nestle-job', {
      jobName: `prd-relatorio_nestle-job`,
      description: 'Relatório Nesttle sobre diagnóstico e hipóteses diagnóstico',
      role: jobRole,
      executable: glue.JobExecutable.pythonEtl({
        glueVersion: glue.GlueVersion.V4_0,
        script: glue.Code.fromBucket(jobSourceBucket, `jobs/prd-relatorio-nestle.py`),
        pythonVersion: glue.PythonVersion.THREE
      }),
      defaultArguments: {
        '--job-language': 'python',
        '--packages': 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,org.apache.iceberg:iceberg-aws-bundle:1.4.3',
        '--datalake-formats': 'iceberg',
        '--enable-continuous-cloudwatch-log': 'true'
      },
    });
  }
}