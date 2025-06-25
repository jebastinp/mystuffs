import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import json
from datetime import datetime

# BigQuery table details
BQ_PURCHASE_TABLE = "jun-jebastinp-16jun-cts:retail_data.purchase_events"
BQ_NOT_PURCHASED_TABLE = "jun-jebastinp-16jun-cts:retail_data.not_purchased_events"

# BigQuery schemas
BQ_PURCHASE_SCHEMA = {
    "fields": [
        {"name": "event_date", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "purchase_count", "type": "INTEGER", "mode": "REQUIRED"}
    ]
}

BQ_NOT_PURCHASED_SCHEMA = {
    "fields": [
        {"name": "event_date", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "not_purchased_count", "type": "INTEGER", "mode": "REQUIRED"}
    ]
}

# Parse multi-line JSON array
class ParseMultilineJSON(beam.DoFn):
    def process(self, json_string):
        try:
            records = json.loads(json_string)
            for record in records:
                if record is None:
                    continue
                timestamp = record.get('timestamp')
                if timestamp:
                    date_str = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
                    record['event_date'] = date_str
                    yield record
        except Exception as e:
            print(f"Skipping due to error: {e}")

# Filter purchases
class FilterPurchase(beam.DoFn):
    def process(self, record):
        if record and record.get("event_type") == "purchase":
            print("Purchase:", record)
            yield ((record["event_date"], record["product_id"]), 1)

# Filter not purchased
class FilterNotPurchased(beam.DoFn):
    def process(self, record):
        if record and record.get("event_type") != "purchase":
            print("Not Purchased:", record)
            yield ((record["event_date"], record["product_id"]), 1)

# Format for BigQuery
def FormatPurchaseForBQ(record):
    key, count = record
    formatted = {
        "event_date": key[0],
        "product_id": key[1],
        "purchase_count": count
    }
    print("Formatted Purchase:", formatted)
    return formatted

def FormatNotPurchasedForBQ(record):
    key, count = record
    formatted = {
        "event_date": key[0],
        "product_id": key[1],
        "not_purchased_count": count
    }
    print("Formatted Not Purchased:", formatted)
    return formatted

# Pipeline setup
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'jun-jebastinp-16jun-cts'
google_cloud_options.region = 'us-east1'
google_cloud_options.job_name = 'clickstream-analysis-job'
google_cloud_options.staging_location = 'gs://casestudy3data1/staging'
google_cloud_options.temp_location = 'gs://casestudy3data1/temp'
options.view_as(StandardOptions).runner = 'DirectRunner'  # Change to 'DataflowRunner' for cloud

with beam.Pipeline(options=options) as pipeline:
    parsed = (
        pipeline
        | 'Read Multiline JSON' >> beam.io.ReadFromText('gs://casestudy3data1/sample_clickstream.json')
        | 'Group Lines' >> beam.combiners.ToList()
        | 'Join Lines' >> beam.Map(lambda lines: '\n'.join(lines))
        | 'Parse JSON Array' >> beam.ParDo(ParseMultilineJSON())
        | 'Filter None Records' >> beam.Filter(lambda x: x is not None)
        | 'Print Parsed Records' >> beam.Map(lambda x: print("Parsed:", x) or x)
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
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location='gs://casestudy3data1/temp'
        )
    )

    # Not Purchased
    (
        parsed
        | 'Filter Not Purchased' >> beam.ParDo(FilterNotPurchased())
        | 'Count Not Purchased' >> beam.CombinePerKey(sum)
        | 'Format Not Purchased for BQ' >> beam.Map(FormatNotPurchasedForBQ)
        | 'Write Not Purchased to BQ' >> WriteToBigQuery(
            table=BQ_NOT_PURCHASED_TABLE,
            schema=BQ_NOT_PURCHASED_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location='gs://casestudy3data1/temp'
        )
    )
