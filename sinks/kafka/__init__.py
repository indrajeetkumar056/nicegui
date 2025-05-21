from sinks import SinkBase

class KafkaSinkBase(SinkBase):
    def __init__(self, topic, callback_function=None, kwargs=None):
        super().__init__(callback_function, kwargs)
        self.topic = topic

    def process_fetched_data(self, products, start_datetime, end_datetime):
        """Process data before sending to Kafka."""
        return True, f"Processing successful for topic {self.topic}"

    def dump_data_to_sink(self):
        """Send processed data to Kafka."""
        print(f"Dumping data to Kafka topic: {self.topic}")
        return True, f"Data dumped successfully to {self.topic}"