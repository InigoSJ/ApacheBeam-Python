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
default_topic = 'projects/<project>/topics/<topic>'
default_bucket = 'gs://<bucket>'
project = '<project>'

WINDOW_LENGTH = 60 * 2  # 60 secs * 2 min


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic',
                        dest='topic',
                        default=default_topic)
    parser.add_argument('--bucket',
                        dest='bucket',
                        default=default_bucket)

    class WriteToSeparateFiles(beam.DoFn):
        def __init__(self, outdir):
            self.outdir = outdir

        def process(self, element):
            now = datetime.now()
            writer = filesystems.FileSystems.create(
                path=self.outdir + '{}/{}/{}/{}:{}-report.json'.format(now.year, now.month, now.day, now.hour,
                                                                       now.minute))
            writer.write(element)
            writer.close()

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend(['--project={}'.format(project), '--streaming', '--experiments=allow_non_updatable_job'])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    def string_join(elements):
        string = str(elements)
        return string.replace('},', '};')

    with beam.Pipeline(options=pipeline_options) as p:
        (p | "ReadTopic" >> beam.io.ReadFromPubSub(topic=known_args.topic)
         | "Window" >> beam.WindowInto(window.FixedWindows(WINDOW_LENGTH))
         | "Combine" >> beam.CombineGlobally(string_join).without_defaults()
         | "WriteToGCSwithDate" >> beam.ParDo(WriteToSeparateFiles(known_args.bucket)))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
