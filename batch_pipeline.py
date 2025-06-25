import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

class ParseEventFn(beam.DoFn):
    def process(self, element):
        import json
        record = json.loads(element)
        yield {
            'event_type': record.get('event_type'),
            'product_id': record.get('product_id'),
            'user_id': record.get('user_id'),
            'timestamp': record.get('timestamp')
        }

class AddEventDateFn(beam.DoFn):
    def process(self, element):
        from datetime import datetime
        element['event_date'] = datetime.strptime(element['timestamp'], "%Y-%m-%dT%H:%M:%S").date().isoformat()
        yield element

def run():
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'studied-beanbag-462316-g3'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.job_name = 'batch-clickstream-analysis'
    google_cloud_options.staging_location = 'gs://casestudy3data/staging'
    google_cloud_options.temp_location = 'gs://casestudy3data/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromGCS' >> beam.io.ReadFromText('gs://casestudy3data/sample_clickstream.json')
            | 'ParseJSON' >> beam.ParDo(ParseEventFn())
            | 'AddEventDate' >> beam.ParDo(AddEventDateFn())
            | 'FilterPurchases' >> beam.Filter(lambda x: x['event_type'] == 'purchase')
            | 'MapToKey' >> beam.Map(lambda x: ((x['event_date'], x['product_id']), 1))
            | 'CountPurchases' >> beam.CombinePerKey(sum)
            | 'FormatForBQ' >> beam.Map(lambda x: {
                'event_date': x[0][0],
                'product_id': x[0][1],
                'purchase_count': x[1]
            })
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                'studied-beanbag-462316-g3:ecommerce.ecommerce_trends',
                schema='event_date:DATE, product_id:STRING, purchase_count:INTEGER',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()

#gs://casestudy3data/sample_clickstream.json