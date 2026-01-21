from builtins import Exception, str
from datetime import datetime
import os
import pandas as pd
import requests
import json
import logging
from src.config import RAW_DIR





def extract_data(   url: str,
                    start_date: str | None = None,
                    end_date: str | None = None, **kwargs) -> list[str]:
    
    """
    This function Returns a JSON Payload from the API through http end-point, and writes the returned data into the localhost.
    
    """
    
    try:

        if start_date is None:
            start_date = (datetime.today() - pd.Timedelta(days=3)).strftime('%Y-%m-%d')
        
        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
        
        base_url = url


        params = {
            'format': "geojson",
            'starttime' : start_date,
            'endtime' : end_date
            }
        
        RAW_DIR.mkdir(parents=True, exist_ok=True)

        raw_path = RAW_DIR / f'api_response_{start_date}_to_{end_date}.json'

        os.makedirs(os.path.dirname(raw_path), exist_ok=True)

        logging.info(f"Extracting data from {base_url} with params: {params}")

        
        response = requests.get(base_url, params=params)

        
        response.raise_for_status()


        with open(raw_path, 'w') as j:
            json.dump(response.json(), j)


        return [str(raw_path)]

        
    except Exception as e:

        logging.error(f"Error during data extraction: {e}")

        raise
