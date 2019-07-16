import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.core import ParDo
from datetime import datetime
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io import WriteToText
from apache_beam import window
import yaml

# TODO
default_topic = 'projects/<project>/topics/<topic>'
project = '<project>'
default_bucket = 'gs://<bucket>'

WINDOW_LENGTH = 60 * 2
GROUP_BY_KEY = 'type'


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic',
                        dest='topic',
                        default=default_topic)
    parser.add_argument('--bucket',
                        dest='bucket',
                        default=default_bucket)

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend(['--project={}'.format(project), '--streaming', '--experiments=allow_non_updatable_job'])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    def add_key(element):
        # makes the string a dict and returns the key and the rest of the dict
        parsed_dict = yaml.load(element)
        key = parsed_dict[GROUP_BY_KEY]
        parsed_dict.pop(GROUP_BY_KEY)
        return (key, parsed_dict)


    with beam.Pipeline(options=pipeline_options) as p:
        (p | "ReadTopic" >> beam.io.ReadFromPubSub(topic=known_args.topic)
         | "AddKey" >> beam.Map(lambda element: add_key(element))
         | "Window" >> beam.WindowInto(window.FixedWindows(WINDOW_LENGTH))
         | "GroupByKey" >> beam.GroupByKey()
         | "WriteToGCS" >> WriteToText(known_args.bucket))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
