import pandas as pd

class Datastore:
    filtered_df=pd.DataFrame()
    procsessed_dict={}
    process_tables_container=None
    
    @classmethod
    def set_process_table_container(cls, container):
        """Set the UI container instance for processed tables."""
        cls.process_tables_container = container