from sinks import SinkBase

class DruidSinkBase(SinkBase):
    def __init__(self, table, callback_function=None, kwargs=None):
        super().__init__(callback_function, kwargs)
        self.table = table

    def process_fetched_data(self, products, start_datetime, end_datetime):
        """Process data before sending to Druid."""
        return True, f"Processing successful for Druid table {self.table}"

    def dump_data_to_sink(self):
        """Send processed data to Druid."""
        print(f"Dumping data to Druid table: {self.table}")
        return True, f"Data dumped successfully to {self.table}"