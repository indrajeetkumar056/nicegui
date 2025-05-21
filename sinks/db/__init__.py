from sinks import SinkBase

class DBSinkBase(SinkBase):
    def __init__(self, db, callback_function=None, kwargs=None):
        super().__init__(callback_function, kwargs)
        self.db = db

    def process_fetched_data(self, products, start_datetime, end_datetime):
        """Process data before sending to DB."""
        return True, f"Processing successful for db {self.db}"

    def dump_data_to_sink(self):
        """Send processed data to DB."""
        print(f"Dumped data to DB db: {self.db}")
        return True, f"Data dumped successfully to {self.db}"