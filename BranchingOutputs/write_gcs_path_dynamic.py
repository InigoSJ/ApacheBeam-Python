import argparse
import logging

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.core import ParDo
from datetime import datetime
from apache_beam.io import filesystems
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam import window
import yaml

# TODO
default_topic = 'projects/inigo-sj/topics/dynamic'
project = 'inigo-sj'


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic',
                        dest='topic',
                        default=default_topic)

    class WriteToSeparateFiles(beam.DoFn):
        def process(self, element):
            dictionary = yaml.load(element)
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            writer = filesystems.FileSystems.create(dictionary['destination']+'{}-report.json'.format(now_str))
            dictionary.pop('destination')
            writer.write(str(dictionary))
            writer.close()

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend(['--project={}'.format(project), '--streaming', '--experiments=allow_non_updatable_job'])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True


    with beam.Pipeline(options=pipeline_options) as p:
        (p | "ReadTopic" >> beam.io.ReadFromPubSub(topic=known_args.topic)
           | "WriteToGCSDynamic" >> beam.ParDo(WriteToSeparateFiles()))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
