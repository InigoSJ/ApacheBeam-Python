import argparse
import logging

import apache_beam as beam
from apache_beam import pvalue
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
default_bucket = 'gs://<bucket>'
project = '<project>'

WINDOW_LENGTH = 60 * 10  # 60 secs *10 min


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic',
                        dest='topic',
                        default=default_topic)
    parser.add_argument('--bucket',
                        dest='bucket',
                        default=default_bucket)

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend(['--project={}'.format(project)])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    class DiffOutputsFn(beam.DoFn):
        # These tags will be used to tag the outputs of this DoFn.
        OUTPUT_TAG_BUY = 'buy'
        OUTPUT_TAG_SELL = 'sell'
        OUTPUT_TAG_ERROR = 'error'

        def process(self, element):
            dictionary = yaml.load(element)
            dictionary['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if dictionary['type'] == 'buy':
                dictionary.pop('type')
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_BUY, dictionary)
            elif dictionary['type'] == 'sell':
                dictionary.pop('type')
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_SELL, dictionary)
            else:
                # we don't drop the key here, since we want to know where the mistake was
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_ERROR, dictionary)

    def string_join(elements):
        string = str(elements)
        return string.replace('},', '};')

    with beam.Pipeline(options=pipeline_options) as p:

        diff_outputs = (p | "ReadTopic" >>
                        beam.io.ReadFromPubSub(topic=known_args.topic) |
                        "SplitOutputs" >> beam.ParDo(DiffOutputsFn()).with_outputs(
                    DiffOutputsFn.OUTPUT_TAG_BUY,
                    DiffOutputsFn.OUTPUT_TAG_SELL,
                    DiffOutputsFn.OUTPUT_TAG_ERROR))

        buy = (diff_outputs.buy | "WindowBuy" >> beam.WindowInto(window.FixedWindows(WINDOW_LENGTH))
               | "CombineBuy" >> beam.CombineGlobally(string_join).without_defaults()
               | "WriteToGCSBuy" >> WriteToText(file_path_prefix=known_args.bucket + 'buy/'))

        sell = (diff_outputs.sell | "WindowSell" >> beam.WindowInto(window.FixedWindows(WINDOW_LENGTH))
                | "CombineSell" >> beam.CombineGlobally(string_join).without_defaults()
                | "WriteToGCSSell" >> WriteToText(file_path_prefix=known_args.bucket + 'sell/'))

        # We want to know what 'type' gave the error, so no ParDo here
        error = (diff_outputs.error | "WindowError" >> beam.WindowInto(window.FixedWindows(WINDOW_LENGTH))
                 | "CombineError" >> beam.CombineGlobally(string_join).without_defaults()
                 | "WriteToGCSError" >> WriteToText(file_path_prefix=known_args.bucket + 'error/'))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
