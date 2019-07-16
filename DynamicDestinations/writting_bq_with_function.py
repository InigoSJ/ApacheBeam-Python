import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.core import ParDo
from datetime import datetime
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import yaml

# TODO
default_topic = 'projects/<project>/topics/<topic>'
project = '<project>'


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic',
                        dest='topic',
                        default=default_topic)

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend(['--project={}'.format(project), '--streaming', '--experiments=allow_non_updatable_job'])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # TODO
    schema = 'name:string,age:integer,timestamp:timestamp'

    def to_dict(element):
        return [yaml.load(element)]

    def get_table(element):
        # table format is DATASET.TABLE
        table = element['table']
        element['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        element.pop('table')
        return '{}:{}'.format(project, table)

    with beam.Pipeline(options=pipeline_options) as p:
        (p | "ReadTopic" >> beam.io.ReadFromPubSub(topic=known_args.topic)
         | "ToDict" >> beam.ParDo(to_dict)
         | "WriteToBQ" >> WriteToBigQuery(table=get_table, schema=schema,
                                          create_disposition=BigQueryDisposition.CREATE_NEVER,
                                          write_disposition=BigQueryDisposition.WRITE_APPEND))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
