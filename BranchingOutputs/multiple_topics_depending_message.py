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
default_topic = 'projects/<project>/topics/<topic_prefix>'  # this is a prefix
default_bucket = 'gs://<bucket>'
project = '<project>'

WINDOW_LENGTH = 60 * 2  # 60 secs * 2 min
BRANCHES = 3


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic_prefix',
                        dest='topic_prefix',
                        default=default_topic)
    parser.add_argument('--bucket',
                        dest='bucket',
                        default=default_bucket)

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend(['--project={}'.format(project), '--streaming', '--experiments=allow_non_updatable_job'])

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

        output_buy = []
        output_sell = []
        output_error = []

        for branch in range(BRANCHES):
            current_topic = known_args.topic_prefix + str(branch)
            diff_outputs = (p | "ReadTopic{}".format(branch) >>
                            beam.io.ReadFromPubSub(topic=current_topic) |
                            "SplitOutputs{}".format(branch) >> beam.ParDo(DiffOutputsFn()).with_outputs(
                        DiffOutputsFn.OUTPUT_TAG_BUY,
                        DiffOutputsFn.OUTPUT_TAG_SELL,
                        DiffOutputsFn.OUTPUT_TAG_ERROR))

            # We need to make a list for each output type
            output_buy.append(diff_outputs.buy)
            output_sell.append(diff_outputs.sell)
            output_error.append(diff_outputs.error)

        buy = (tuple(output_buy) | "FlattenBuy" >> beam.Flatten()
               | "WindowBuy" >> beam.WindowInto(window.FixedWindows(WINDOW_LENGTH))
               | "CombineBuy" >> beam.CombineGlobally(string_join).without_defaults()
               | "WriteToGCSBuy" >> WriteToText(file_path_prefix=known_args.bucket + 'buy/'))

        sell = (tuple(output_sell) | "FlattenSell" >> beam.Flatten()
                | "WindowSell" >> beam.WindowInto(window.FixedWindows(WINDOW_LENGTH))
                | "CombineSell" >> beam.CombineGlobally(string_join).without_defaults()
                | "WriteToGCSSell" >> WriteToText(file_path_prefix=known_args.bucket + 'sell/'))

        error = (tuple(output_error) | "FlattenError" >> beam.Flatten()
                 | "WindowError" >> beam.WindowInto(window.FixedWindows(WINDOW_LENGTH))
                 | "CombineError" >> beam.CombineGlobally(string_join).without_defaults()
                 | "WriteToGCSError" >> WriteToText(file_path_prefix=known_args.bucket + 'error/'))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
