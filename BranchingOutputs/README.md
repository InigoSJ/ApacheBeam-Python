Run with:
 
`python <file> --streaming --temp_location=<path to gcs folder> --runner=DataflowRunner  --experiments=allow_non_updatable_job`

Optional flags:

--bucket <patch to gcs folder>

For `depending_on_pubsub_message.py`

`--topic <full topic name>`

For `multiple_topics_depending_message.py`

`--topic_prefix <full topic name prefix>`