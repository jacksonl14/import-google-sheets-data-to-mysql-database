import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List, Any
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from configparser import ConfigParser

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sheets_etl.log'),
        logging.StreamHandler()
    ]
)

class SheetsETL:
    def __init__(self, config_file: str = 'config.ini'):
        self.config = self._load_config(config_file)
        self.sheets_service = self._init_sheets_service()
        
    def _load_config(self, config_file: str) -> ConfigParser:
        """Load configuration from ini file"""
        config = ConfigParser()
        config.read(config_file)
        return config

    def _init_sheets_service(self):
        """Initialize Google Sheets API service"""
        try:
            credentials = Credentials.from_service_account_file(
                self.config['google']['credentials_file'],
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            return build('sheets', 'v4', credentials=credentials)
        except Exception as e:
            logging.error(f"Failed to initialize Google Sheets service: {str(e)}")
            raise

    def read_sheet(self, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
        """Read data from a Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            return result.get('values', [])
        except HttpError as e:
            logging.error(f"Error reading from sheet: {str(e)}")
            raise

    def write_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> None:
        """Write data to a Google Sheet"""
        try:
            body = {
                'values': values
            }
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            logging.info(f"Successfully updated {len(values)} rows in {range_name}")
        except HttpError as e:
            logging.error(f"Error writing to sheet: {str(e)}")
            raise

    def append_to_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> None:
        """Append data to a Google Sheet"""
        try:
            body = {
                'values': values
            }
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            logging.info(f"Successfully appended {len(values)} rows to {range_name}")
        except HttpError as e:
            logging.error(f"Error appending to sheet: {str(e)}")
            raise

    def process_travel_data(self) -> None:
        """Process travel form data"""
        try:
            # Read from source sheet
            source_data = self.read_sheet(
                self.config['google']['source_travel_sheet_id'],
                self.config['google']['source_travel_range']
            )
            
            if not source_data:
                logging.info("No travel data to process")
                return

            # Transform data if needed (example transformation)
            processed_data = []
            headers = source_data[0]  # Assuming first row contains headers
            
            for row in source_data[1:]:  # Skip header row
                # Ensure row has same length as headers
                row_data = row + [''] * (len(headers) - len(row))
                processed_data.append(row_data)

            # Write to destination sheet
            self.append_to_sheet(
                self.config['google']['dest_travel_sheet_id'],
                self.config['google']['dest_travel_range'],
                processed_data
            )
            
            logging.info(f"Processed {len(processed_data)} travel records")
            
        except Exception as e:
            logging.error(f"Error processing travel data: {str(e)}")

    def process_building_data(self) -> None:
        """Process building form data"""
        try:
            source_data = self.read_sheet(
                self.config['google']['source_building_sheet_id'],
                self.config['google']['source_building_range']
            )
            
            if not source_data:
                logging.info("No building data to process")
                return

            processed_data = []
            headers = source_data[0]
            
            for row in source_data[1:]:
                row_data = row + [''] * (len(headers) - len(row))
                processed_data.append(row_data)

            self.append_to_sheet(
                self.config['google']['dest_building_sheet_id'],
                self.config['google']['dest_building_range'],
                processed_data
            )
            
            logging.info(f"Processed {len(processed_data)} building records")
            
        except Exception as e:
            logging.error(f"Error processing building data: {str(e)}")

    def process_incident_data(self) -> None:
        """Process incident form data"""
        try:
            source_data = self.read_sheet(
                self.config['google']['source_incident_sheet_id'],
                self.config['google']['source_incident_range']
            )
            
            if not source_data:
                logging.info("No incident data to process")
                return

            processed_data = []
            headers = source_data[0]
            
            for row in source_data[1:]:
                row_data = row + [''] * (len(headers) - len(row))
                processed_data.append(row_data)

            self.append_to_sheet(
                self.config['google']['dest_incident_sheet_id'],
                self.config['google']['dest_incident_range'],
                processed_data
            )
            
            logging.info(f"Processed {len(processed_data)} incident records")
            
        except Exception as e:
            logging.error(f"Error processing incident data: {str(e)}")

    def process_all_data(self) -> None:
        """Process all form data"""
        try:
            self.process_travel_data()
            self.process_building_data()
            self.process_incident_data()
            logging.info("Completed processing all data")
        except Exception as e:
            logging.error(f"Error in process_all_data: {str(e)}")

if __name__ == "__main__":
    etl = SheetsETL()
    etl.process_all_data()