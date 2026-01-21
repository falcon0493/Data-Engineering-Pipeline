
from builtins import Exception, isinstance, len, list
import os
import json
from pathlib import Path
import pandas as pd
import logging
from src.config import PROCESSED_DIR




logging.basicConfig(level=logging.INFO)


def transform_data(raw_files: list[str]) -> list[str]:
    """
    This function transforms the api response json file to pandas dataframe
    """

    
    try:
            
            logging.info("Starting data transformation...")

            raw_path = Path(raw_files)


            if not raw_path.exists():
                raise FileNotFoundError(f"Raw file not found: {raw_path}")
            

            with open(raw_path, "r") as f:
                data = json.load(f)



            records = []


            for feature in data["features"]:

                props = feature.get("properties", {})
                geom = feature.get("geometry", {})
                coords = geom.get("coordinates", [])


                records.append({
                    "id": feature.get("ids"),
                    "magnitude": props.get("mag"),
                    "place": props.get("place"),
                    "time": props.get("time"),
                    "longitude": coords[0] if len(coords) > 0 else None,
                    "latitude": coords[1] if len(coords) > 1 else None,
                    "depth": coords[2] if len(coords) > 2 else None,
                    "event_type": props.get("type"),
                    "status": props.get("status"),
                    "tsunami": props.get("tsunami"),
                })
            
            
            df = pd.DataFrame(records)

            
            logging.info(f"Data transformation complete. Rows after transformation: {len(df)}")
            
            # 4. BUILD PROCESSED PATH SAFELY
            processed_dir = raw_path.parents[1] / "processed"


            processed_dir.mkdir(parents=True, exist_ok=True)


            processed_path = processed_dir / raw_path.with_suffix(".csv").name


            df.to_csv(processed_path, index=False)


            return str(processed_path)
        
    

    except Exception as e:

        logging.error(f"Error during data transformation: {e}")

        raise
