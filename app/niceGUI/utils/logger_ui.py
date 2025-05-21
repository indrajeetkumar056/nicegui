from nicegui import ui
from datetime import datetime
# Global dictionary to store loggers
LOGGER_REGISTRY = {}

class LoggerUI:
    def __init__(self,container, name: str):
        """Initialize a log container inside the provided UI container."""
        self.name = name
        with container:  # Place logger inside the specific UI section
            self.log_container = ui.column().style(
                "max-height: 200px; overflow-y: auto; border: 0px solid #ddd; padding: 10px; display: block")

        LOGGER_REGISTRY[self.name] = self
            
    def clear_logs(self):
        self.log_container.clear()

    def log_message(self, message: str):
        """Add log messages dynamically with timestamps"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Get current timestamp
        log_entry = f"{timestamp}:{message}"  # Format message with timestamp
        self.log_container.style("display: block;")
        with self.log_container:
            ui.label(log_entry) # Display log
            ui.separator().classes('my-2') 


