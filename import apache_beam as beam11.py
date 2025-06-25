import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import json
from datetime import datetime

# BigQuery table details
BQ_PURCHASE_TABLE = "studied-beanbag-462316-g3:retail_data.purchase_events"
BQ_ABANDONED_TABLE = "studied-beanbag-462316-g3:retail_data.abandoned_events"

# BigQuery schemas
BQ_PURCHASE_SCHEMA = {
    "fields": [
        {"name": "event_date", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "purchase_count", "type": "INTEGER", "mode": "REQUIRED"}
    ]
}

BQ_ABANDONED_SCHEMA = {
    "fields": [
        {"name": "event_date", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "abandoned_count", "type": "INTEGER", "mode": "REQUIRED"}
    ]
}

# Parse JSON lines
class ParseJSON(beam.DoFn):
    def process(self, line):
        try:
            record = json.loads(line)
            timestamp = record.get('timestamp')
            date_str = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
            record['event_date'] = date_str
            yield record
        except Exception as e:
            print(f"Skipping record due to error: {e}")

# Filter purchases
class FilterPurchase(beam.DoFn):
    def process(self, record):
        if record.get("event_type") == "purchase":
            yield ((record["event_date"], record["product_id"]), 1)

# Filter non-purchases (abandoned)
class FilterAbandoned(beam.DoFn):
    def process(self, record):
        if record.get("event_type") != "purchase":
            yield ((record["event_date"], record["product_id"]), 1)

# Format for BigQuery
def FormatPurchaseForBQ(record):
    key, count = record
    return {
        "event_date": key[0],
        "product_id": key[1],
        "purchase_count": count
    }

def FormatAbandonedForBQ(record):
    key, count = record
    return {
        "event_date": key[0],
        "product_id": key[1],
        "abandoned_count": count
    }

# Pipeline setup
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'studied-beanbag-462316-g3'
google_cloud_options.region = 'us-central1'
google_cloud_options.job_name = 'batch-clickstream-analysis'
google_cloud_options.staging_location = 'gs://casestudy3data/staging'
google_cloud_options.temp_location = 'gs://casestudy3data/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

with beam.Pipeline(options=options) as pipeline:
    parsed = (
        pipeline
        | 'ReadFromGCS' >> beam.io.ReadFromText('gs://casestudy3data/sample_clickstream.json')
        | 'Parse JSON' >> beam.ParDo(ParseJSON())
    )

    # Purchases
    (
        parsed
        | 'Filter Purchases' >> beam.ParDo(FilterPurchase())
        | 'Count Purchases' >> beam.CombinePerKey(sum)
        | 'Format Purchases for BQ' >> beam.Map(FormatPurchaseForBQ)
        | 'Write Purchases to BQ' >> WriteToBigQuery(
            table=BQ_PURCHASE_TABLE,
            schema=BQ_PURCHASE_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location='gs://casestudy3data/temp'
        )
    )

    # Abandoned
    (
        parsed
        | 'Filter Abandoned' >> beam.ParDo(FilterAbandoned())
        | 'Count Abandoned' >> beam.CombinePerKey(sum)
        | 'Format Abandoned for BQ' >> beam.Map(FormatAbandonedForBQ)
        | 'Write Abandoned to BQ' >> WriteToBigQuery(
            table=BQ_ABANDONED_TABLE,
            schema=BQ_ABANDONED_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location='gs://casestudy3data/temp'
        )
    )