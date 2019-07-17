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
import pandas as pd

# TODO
default_topic = 'projects/<project>/topics/<topic>'
project = '<project>'
default_bucket = 'gs://<bucket>'

WINDOW_LENGTH = 60 * 2
WINDOW_PERIOD = 60 * 0.5
GROUP_BY_KEY = 'type'
VALUE_TO_AVG = 'value'


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

    def avg_value(element):
        # element[0] is the key, element[1] rest of the dictionary
        df = pd.DataFrame(element[1])
        avg = df[VALUE_TO_AVG].mean()
        return (element[0], avg)

    with beam.Pipeline(options=pipeline_options) as p:
        (p | "ReadTopic" >> beam.io.ReadFromPubSub(topic=known_args.topic)
         | "AddKey" >> beam.Map(lambda element: add_key(element))
         | "Window" >> beam.WindowInto(window.SlidingWindows(size=WINDOW_LENGTH, period=WINDOW_PERIOD))
         | "GroupByKey" >> beam.GroupByKey()
         | "AvgValue" >> beam.ParDo(avg_value)
         | "WriteToGCS" >> WriteToText(known_args.bucket))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
