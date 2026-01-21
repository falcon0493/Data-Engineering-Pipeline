
from builtins import Exception, ValueError, isinstance, len, str
from sqlalchemy import create_engine
import pandas as pd
import logging
from pathlib import Path



logging.basicConfig(level=logging.INFO)

def load_data(file_path: str, table: str, conn_str: str) -> None:

    """
    Append transformed earthquake data into SQL Server table 'EarthquakeData'.
    Skips rows if 'id' already exists (via IF NOT EXISTS in the query).
    """

    
    try:
        logging.info(f"Connecting to SQL Server")

        try: 

            conn_string = conn_str

            engine = create_engine(conn_string)
            
            print(f'{engine} is created..')
            

        except Exception as e:

            print(f"{e}")

            print("Please ensure 'ODBC Driver 17 for SQL Server' is installed and connection details are correct.")

        

        try:

            if True:

                table_name = table

                path = Path(file_path)

                print(path)


        except Exception as e:

            print(f'something went wrong: {e}')
        

        try:

            df = pd.read_csv(path)

            df = df.rename(columns={
                                    'time': 'event_time',
                                    'id': 'event_id',
                                    'status': 'event_status'
                                })
            
            
            cols = [
                "event_id", "magnitude", "place", "event_time",
                "longitude", "latitude", "depth", "event_type",
                "event_status", "tsunami"
            ]


            df = df[cols]


            print(f'the dataframe has size of {df.size}')

        except Exception as e:
            print(f'something went wrong: {e}')

        
        try:


            df.to_sql(name=table_name, schema='AutomationDB.dbo', con=engine, if_exists='append', index=False)

            print(f"\nDataFrame successfully loaded to SQL Server table '{table_name}'.")


        except Exception as e:
            print(f"\nError loading DataFrame to SQL Server: {e}")
            print("Please check your connection string, SQL Server configuration, and table schema for compatibility.")

        
    
    except Exception as e:
        logging.error(f"Error during data loading: {e}")
        raise
