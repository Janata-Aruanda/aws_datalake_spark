import * as cdk from 'aws-cdk-lib';
import * as glue from '@aws-cdk/aws-glue-alpha';
import { Construct } from 'constructs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Role } from 'aws-cdk-lib/aws-iam';
import { CfnTrigger } from 'aws-cdk-lib/aws-glue';

export interface RawMedicalReportsRecordsGlueJobProps extends cdk.StackProps {
    readonly environment: string
    readonly jobRole: Role
    readonly jobSourceBucket: Bucket
    readonly datalakeBucket: Bucket
    readonly databaseName: string
}

export class RawMedicalReportsRecordsGlueJob extends Construct {
    constructor(scope: Construct, id: string, props: RawMedicalReportsRecordsGlueJobProps) {
        super(scope, id);

        const { 
            environment, 
            jobRole,
            jobSourceBucket,
            datalakeBucket,
            databaseName
        } = props;

        const catalogName = "glue_catalog";
        const catalogImplementation = "org.apache.iceberg.aws.glue.GlueCatalog"
        const storageImplementation = "org.apache.iceberg.aws.s3.S3FileIO"
        const tableName = "raw_medical_records" 
        const bucketPath = `s3://${environment}-timeline-bucket/`
        const bucketPathOutput =  `s3://${datalakeBucket.bucketName}/raw/medical_records/`

        const conf = `--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \        
            --conf spark.sql.catalog.${catalogName}=org.apache.iceberg.spark.SparkCatalog \        
            --conf spark.sql.catalog.${catalogName}.warehouse=${bucketPathOutput} \       
            --conf spark.sql.catalog.${catalogName}.catalog-impl=${catalogImplementation} \
            --conf spark.sql.catalog.${catalogName}.io-impl=${storageImplementation} \       
            --conf spark.sql.catalog.${catalogName}.s3.write.tags.environment=hml`
        ;

        const job = new glue.Job(this, 'raw-medical-reports-record-job', {
            jobName: `${environment}-raw-medical-reports-job`,
            description: 'Job transformação dos dados json para tatular, catalogando no glue no formato da tabela iceberg e salvando o arquivo em parquet',
            role: jobRole,
            executable: glue.JobExecutable.pythonEtl({
                glueVersion: glue.GlueVersion.V4_0,
                script: glue.Code.fromBucket(jobSourceBucket, `jobs/raw-medical-reports-records-job.py`),
                pythonVersion: glue.PythonVersion.THREE
            }),

            defaultArguments: {
                '--job-language': 'python',
                '--packages': 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,org.apache.iceberg:iceberg-aws-bundle:1.4.3',
                '--datalake-formats': 'iceberg',
                '--enable-glue-datacatalog': 'true',
                '--enable-job-insights': 'true',
                '--enable-s3-parquet-optimized-committer': 'true',
                '--conf': conf,
                '--enable-continuous-cloudwatch-log': 'true',
                '--DATABASE_NAME': `${databaseName}`,
                '--TABLE_NAME': `${tableName}`,
                '--BUCKET_PATH': `${bucketPath}`,
                '--BUCKET_PATH_OUTPUT': `${bucketPathOutput}`
            },
        });
        
        new CfnTrigger(this, 'raw-medical-reports-record-trigger', {
            type: 'SCHEDULED',
            description: 'SCHEDULED to run every day at 03 AM - Monday - Friday ',
            actions: [{ jobName: job.jobName }],
            schedule: 'cron(25 01 ? * MON,TUE,WED,THU,FRI *)',
            startOnCreation: true
        });
    }
}