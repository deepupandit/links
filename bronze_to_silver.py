 def execute_query(self, sql):
        """
        Execute SQL query on Iceberg tables using Spark SQL
        
        Args:
            sql (str): SQL query to execute
            
        Returns:
            DataFrame: Result of the SQL query
        """
        try:
            return self.glue.spark.sql(sql)
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            raise


==========
import sys
from awsglue.utils import getResolvedOptions
import time
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, max, current_timestamp
from boto3.dynamodb.conditions import Key
from dataclasses import dataclass, asdict, field
from typing import Optional
from datetime import datetime


@dataclass
class ProcessDetails:
    target_db: str
    target_table: str
    source_db: str
    source_table: str
    column_transformations: dict[str, str]
    item_count: int
    ordered_column_list: list[str]


@dataclass
class MappingDetail:
    column_name: str
    source_mapping: str
    field_order: int


@dataclass
class JobRunItem:
    layer_db_table_pk: str
    max_created_at: Optional[str] = field(default=None)
    job_status: Optional[str] = field(default=None)
    checked_at: Optional[str] = field(default=None)
    started_at: Optional[str] = field(default=None)
    ended_at: Optional[str] = field(default=None)
    source_row_count: Optional[str] = field(default="0"),
    target_row_count: Optional[str] = field(default="0")
    target_column_count: Optional[str] = field(default="0"),
    target_table: Optional[str] = field(default=None),
    status_message: Optional[str] = field(default="JOB_TRACKER")

    def __dict__(self):
        return {
            "layer_db_table_pk": self.layer_db_table_pk,
            "max_created_at": self.max_created_at,
            "job_status": self.job_status,
            "checked_at": self.checked_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "source_row_count": self.source_row_count,
            "target_row_count": self.target_row_count,
            "target_column_count": self.target_column_count,
            "status_message": self.status_message
        }


# Initialize job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'warehouse_path',
    'aws_region',
    'aws_account_id',
    'catalog_name'
])

catalog_name = args['catalog_name']
aws_region = args['aws_region']
aws_account_id = args['aws_account_id']
warehouse_path = args['warehouse_path']
pk_target_database = "demo_silver_db"
JOB_RUNNING: str = "RUNNING"
JOB_NOT_RUNNING: str = "NOT_RUNNING"
JOB_STARTING: str = "JOB_STARTING"
JOB_STATUS_TABLE: str = "test_event_b_s_job_run"

# Initialize Spark Session with merge-specific configurations
spark = SparkSession.builder \
    .appName("IcebergWithSparkAndGlue") \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}") \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{catalog_name}.client.region", f"{aws_region}") \
    .config(f"spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .config("spark.sql.iceberg.check-ordering", "false") \
    .config("spark.sql.iceberg.merge.optimization.enabled", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()


def perform_merge_operation(source_df, target_identifier):
    """
    Perform merge operation using _created_at as the merge key
    """
    try:
        # Add processing timestamp and optimize partitioning
        source_df = source_df.withColumn("processed_time", current_timestamp())
        source_df = source_df.repartition(col("_created_at"))

        # Create temporary view for the merge operation
        source_df.createOrReplaceTempView("source_updates")

        # Record pre-merge count
        pre_count = spark.sql(f"SELECT COUNT(*) FROM {target_identifier}").collect()[0][0]

        # Construct and execute merge query
        merge_query = f"""
        MERGE INTO {target_identifier} t
        USING (
            SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY _created_at
                ORDER BY processed_time DESC
            ) as row_num
            FROM source_updates
            WHERE row_num = 1
        ) s
        ON t._created_at = s._created_at
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        spark.sql(merge_query)

        # Record post-merge count and print statistics
        post_count = spark.sql(f"SELECT COUNT(*) FROM {target_identifier}").collect()[0][0]
        print(f"Records before merge: {pre_count}")
        print(f"Records after merge: {post_count}")
        print(f"Net change: {post_count - pre_count}")

        return True, "Merge completed successfully"
    except Exception as e:
        return False, f"Merge failed: {str(e)}"
    finally:
        # Clean up temporary view
        spark.sql("DROP VIEW IF EXISTS source_updates")


def query_table(table_name, partition_key_name, partition_key_value):
    """
    Query a DynamoDB table using its primary key.
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    response = table.query(KeyConditionExpression=Key(partition_key_name).eq(partition_key_value))
    return response


def query_gsi(table_name, index_name, partition_key_name, partition_key_value):
    """
    Query a Global Secondary Index (GSI) on a DynamoDB table.
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    response = table.query(
        IndexName=index_name,
        KeyConditionExpression=Key(partition_key_name).eq(partition_key_value)
    )
    return response


def read_mappings(event_name: str) -> tuple[ProcessDetails or None, int]:
    table_name: str = "test_iceberg_schema_registry"
    index_name: str = "layer_db_table_gsi-index"
    partition_key_name: str = "layer_db_table_gsi"
    partition_key_value: str = f"silver*{pk_target_database}*{event_name}"

    print(f"Getting mapping for {partition_key_name} value: {partition_key_value}")
    response = query_gsi(table_name, index_name, partition_key_name, partition_key_value)
    item_count: int = response["Count"] if response is not None and "Count" in response else 0

    if response is not None and "Count" in response and response["Count"] > 0 and "Items" in response:
        items = response["Items"]
        source_db = items[0]["source_db"]
        source_table = items[0]["source_table"]
        target_db = items[0]["db_name"]
        target_table = items[0]["table_name"]

        col_map_list: list[MappingDetail] = []
        for item in response["Items"]:
            col_map_list.append(MappingDetail(
                column_name=item["column_name"],
                source_mapping=item["source_mapping"],
                field_order=item["field_order"],
            ))

        sorted_by_col_map = sorted(col_map_list, key=lambda md: md.field_order)
        transformations: dict[str, col] = {}
        for md in sorted_by_col_map:
            transformations[md.column_name] = col(md.source_mapping)

        process_details: ProcessDetails = ProcessDetails(
            target_db=target_db,
            target_table=target_table,
            source_db=source_db,
            source_table=source_table,
            column_transformations=transformations,
            item_count=item_count,
            ordered_column_list=[md.column_name for md in sorted_by_col_map],
        )
        return process_details, item_count
    else:
        return None, item_count


def check_running_job(event_name: str) -> JobRunItem:
    job_table_name: str = JOB_STATUS_TABLE
    partition_key_name: str = "layer_db_table_pk"
    partition_key_value: str = f"silver*{pk_target_database}*{event_name}"

    response = query_table(job_table_name, partition_key_name, partition_key_value)
    record_count: int = response["Count"]

    if record_count == 0:
        print(f"No running jobs found for {event_name}")
        return JobRunItem(
            layer_db_table_pk=partition_key_value,
            max_created_at="",
            job_status=JOB_NOT_RUNNING,
            checked_at=datetime.now().isoformat(),
            started_at="",
            ended_at="",
            source_row_count="",
            target_row_count="",
            target_column_count="",
            status_message=f"No running jobs found for {event_name}"
        )
    else:
        print(f"Items found for {event_name}")
        items = response['Items'][0]
        return JobRunItem(
            layer_db_table_pk=partition_key_value,
            max_created_at=items.get('max_created_at', ""),
            job_status=items.get('job_status', JOB_NOT_RUNNING),
            started_at=items.get('started_at', ""),
            ended_at=items.get('ended_at', ""),
            checked_at=items.get('checked_at', ""),
            source_row_count=items.get('source_row_count', ""),
            target_row_count=items.get('target_row_count', ""),
            target_column_count=items.get('target_column_count', ""),
            status_message=items.get('status_message', ""),
        )


def update_job_status(job_run_item: JobRunItem):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(JOB_STATUS_TABLE)
    table.put_item(Item=job_run_item.__dict__())


def main(event_name: str):
    job_run: JobRunItem = check_running_job(event_name)

    if job_run.job_status is JOB_RUNNING or job_run.job_status is JOB_STARTING:
        job_run.checked_at = datetime.now().isoformat()
        update_job_status(job_run)
        print(f"Job is for event {event_name} is {job_run.job_status}. Cannot run 2 jobs at same time.")
        return

    print(f"No Job is running for event {event_name}. Starting work")

    # Mark job as starting
    job_run.job_status = JOB_STARTING
    job_run.started_at = datetime.now().isoformat()
    job_run.checked_at = datetime.now().isoformat()
    job_run.status_message = "Job starting"
    update_job_status(job_run)

    # Get mapping details
    mapping_result: tuple[ProcessDetails or None, int] = read_mappings(event_name)
    pd: ProcessDetails = mapping_result[0]
    target_identifier: str = f"{catalog_name}.{pd.target_db}.{pd.target_table}"
    job_run.target_table = target_identifier
    item_count: int = mapping_result[1]

    print(f"Item count: {item_count}")

    if item_count > 0:
        job_run.job_status = JOB_RUNNING
        job_run.started_at = datetime.now().isoformat()
        job_run.status_message = "Job started"
        update_job_status(job_run)

        # Construct source query
        if job_run.max_created_at and len(job_run.max_created_at) > 0:
            max_created_at = datetime.fromisoformat(job_run.max_created_at)
            source_sql = f"""
                SELECT * 
                FROM {catalog_name}.{pd.source_db}.{pd.source_table} 
                WHERE _created_at > '{max_created_at}'
                """
        else:
            source_sql = f"SELECT * FROM {catalog_name}.{pd.source_db}.{pd.source_table}"

        print(f"Executing source query: {source_sql}")
        s_df = spark.sql(source_sql)
        s_row_count: int = s_df.count()
        job_run.source_row_count = s_row_count
        print(f"Source Row Count: {s_row_count}")

        if s_row_count > 0:
            # Update max_created_at
            max_created_at = s_df.select(max("_created_at")).collect()[0][0]
            job_run.max_created_at = max_created_at.isoformat()

            # Apply transformations
            print(f"Applying {len(pd.column_transformations)} transformations")
            t_df = s_df.withColumns(pd.column_transformations).select(*pd.ordered_column_list)

            print(f"Target column count: {len(t_df.columns)}")
            job_run.target_column_count = len(t_df.columns)
            job_run.target_row_count = t_df.count()

            print(f"Writing to target: {target_identifier}")

            try:
                # Check if target table exists
                spark.table(target_identifier)
                table_exists = True
            except Exception as e:
                table_exists = False

            if table_exists:
                # Perform merge operation for existing table
                success, message = perform_merge_operation(t_df, target_identifier)
                if success:
                    job_run.status_message = message
                    job_run.job_status = JOB_NOT_RUNNING
                else:
                    job_run.status_message = message
                    job_run.job_status = "FAILED"
            else:
                # Create new table if it doesn't exist
                try:
                    t_df.writeTo(target_identifier) \
                        .tableProperty("format-version", "2") \
                        .createOrReplace()
                    job_run.
