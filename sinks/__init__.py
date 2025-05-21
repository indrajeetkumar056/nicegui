from abc import ABC, abstractmethod

class SinkBase(ABC):
    def __init__(self, callback_function=None, kwargs=None):
        self.callback_function = callback_function
        self.kwargs = kwargs or {}

    @abstractmethod
    def process_fetched_data(self, products, start_datetime, end_datetime):
        """Processes and prepares data for dumping"""
        pass

    @abstractmethod
    def dump_data_to_sink(self):
        """Dumps processed data to the specified sink"""
        pass
