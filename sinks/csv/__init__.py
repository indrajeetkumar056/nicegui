from sinks import SinkBase

class CSVSinkBase(SinkBase):
    def __init__(self, file, callback_function=None, kwargs=None):
        super().__init__(callback_function, kwargs)
        self.file = file

    def process_fetched_data(self, products, start_datetime, end_datetime):
        """Process data before sending to CSV."""
        return True, f"Processing successful for file {self.file}"

    def dump_data_to_sink(self):
        """Send processed data to CSV."""
        print(f"Dumping data to CSV file: {self.file}")
        return True, f"Data dumped successfully to {self.file}"