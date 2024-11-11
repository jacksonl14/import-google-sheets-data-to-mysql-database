import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from configparser import ConfigParser
import mysql.connector
from mysql.connector import Error
import uuid

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
        self.db_connection = self._init_database_connection()
        
    def _load_config(self, config_file: str) -> ConfigParser:
        """Load configuration from ini file"""
        config = ConfigParser()
        config.read(config_file)
        return config

    def _init_sheets_service(self):
        """Initialize Google Sheets API service"""
        try:
            if not self.config['google']['credentials_file']:
                raise ValueError("Google credentials file path not specified in config.ini")
                
            credentials = Credentials.from_service_account_file(
                self.config['google']['credentials_file'],
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            return build('sheets', 'v4', credentials=credentials)
        except Exception as e:
            logging.error(f"Failed to initialize Google Sheets service: {str(e)}")
            raise

    def _init_database_connection(self):
        """Initialize MySQL database connection"""
        try:
            connection = mysql.connector.connect(
                host=self.config['database']['host'],
                port=self.config.getint('database', 'port'),
                database=self.config['database']['database'],
                user=self.config['database']['user'],
                password=self.config['database']['password']
            )
            connection.autocommit = False
            return connection
        except Error as e:
            logging.error(f"Failed to connect to database: {str(e)}")
            raise

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to MySQL compatible format"""
        try:
            if not date_str:
                return None
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                try:
                    return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                except ValueError:
                    continue
            return None
        except Exception as e:
            logging.error(f"Error parsing date {date_str}: {str(e)}")
            return None

    def read_sheet(self, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
        """Read data from a Google Sheet"""
        try:
            if not spreadsheet_id or not range_name:
                logging.error("Missing spreadsheet ID or range")
                return []
                
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            return result.get('values', [])
        except HttpError as e:
            logging.error(f"Error reading from sheet: {str(e)}")
            return []

    def process_travel_data(self) -> None:
        """Process travel form data"""
        try:
            if not self.config['google']['source_travel_sheet_id']:
                logging.error("Travel sheet ID not configured")
                return

            source_data = self.read_sheet(
                self.config['google']['source_travel_sheet_id'],
                self.config['google']['source_travel_range']
            )
            
            if not source_data:
                logging.info("No travel data to process")
                return

            cursor = self.db_connection.cursor(prepared=True)
            headers = source_data[0]

            for row in source_data[1:]:
                try:
                    row_data = row + [''] * (len(headers) - len(row))
                    
                    name = row_data[headers.index('Name')] if 'Name' in headers else ''
                    email = row_data[headers.index('Email')] if 'Email' in headers else ''
                    department = row_data[headers.index('Department')] if 'Department' in headers else ''
                    destination = row_data[headers.index('Destination')] if 'Destination' in headers else ''
                    start_date = self._parse_date(row_data[headers.index('Start Date')] if 'Start Date' in headers else '')
                    end_date = self._parse_date(row_data[headers.index('End Date')] if 'End Date' in headers else '')
                    purpose = row_data[headers.index('Purpose')] if 'Purpose' in headers else ''

                    response_id = str(uuid.uuid4())

                    cursor.callproc('process_travel_form', (
                        response_id,
                        self.config['google']['source_travel_sheet_id'],
                        self.config['google']['source_travel_range'],
                        source_data.index(row) + 1,
                        name,
                        email,
                        department,
                        destination,
                        start_date,
                        end_date,
                        purpose
                    ))
                    
                    for result in cursor.stored_results():
                        if result.with_rows:
                            logging.info(f"Procedure result: {result.fetchall()}")
                    
                    self.db_connection.commit()
                    logging.info(f"Processed travel record for {name}")
                    
                except Error as e:
                    logging.error(f"Database error processing travel record: {str(e)}")
                    self.db_connection.rollback()
                except Exception as e:
                    logging.error(f"Error processing travel record: {str(e)}")
                    self.db_connection.rollback()

            cursor.close()
            
        except Exception as e:
            logging.error(f"Error in process_travel_data: {str(e)}")

    def process_building_data(self) -> None:
        """Process building form data"""
        try:
            if not self.config['google']['source_building_sheet_id']:
                logging.error("Building sheet ID not configured")
                return

            source_data = self.read_sheet(
                self.config['google']['source_building_sheet_id'],
                self.config['google']['source_building_range']
            )
            
            if not source_data:
                logging.info("No building data to process")
                return

            cursor = self.db_connection.cursor(prepared=True)
            headers = source_data[0]

            for row in source_data[1:]:
                try:
                    response_id = str(uuid.uuid4())
                    row_data = row + [''] * (len(headers) - len(row))
                    
                    name = row_data[headers.index('Building Name')] if 'Building Name' in headers else ''
                    address = row_data[headers.index('Address')] if 'Address' in headers else ''
                    total_rooms = row_data[headers.index('Total Rooms')] if 'Total Rooms' in headers else 0

                    cursor.callproc('process_building_form', (
                        response_id,
                        self.config['google']['source_building_sheet_id'],
                        self.config['google']['source_building_range'],
                        source_data.index(row) + 1,
                        name,
                        address,
                        total_rooms
                    ))
                    
                    for result in cursor.stored_results():
                        if result.with_rows:
                            logging.info(f"Procedure result: {result.fetchall()}")
                    
                    self.db_connection.commit()
                    logging.info(f"Processed building record for {name}")
                    
                except Error as e:
                    logging.error(f"Database error processing building record: {str(e)}")
                    self.db_connection.rollback()
                except Exception as e:
                    logging.error(f"Error processing building record: {str(e)}")
                    self.db_connection.rollback()

            cursor.close()
            
        except Exception as e:
            logging.error(f"Error in process_building_data: {str(e)}")

    def process_incident_data(self) -> None:
        """Process incident form data"""
        try:
            if not self.config['google']['source_incident_sheet_id']:
                logging.error("Incident sheet ID not configured")
                return

            source_data = self.read_sheet(
                self.config['google']['source_incident_sheet_id'],
                self.config['google']['source_incident_range']
            )
            
            if not source_data:
                logging.info("No incident data to process")
                return

            cursor = self.db_connection.cursor(prepared=True)
            headers = source_data[0]

            for row in source_data[1:]:
                try:
                    response_id = str(uuid.uuid4())
                    row_data = row + [''] * (len(headers) - len(row))
                    
                    csa_id = row_data[headers.index('CSA ID')] if 'CSA ID' in headers else None
                    incident_type = row_data[headers.index('Incident Type')] if 'Incident Type' in headers else ''
                    location = row_data[headers.index('Location')] if 'Location' in headers else ''

                    cursor.callproc('process_incident_form', (
                        response_id,
                        self.config['google']['source_incident_sheet_id'],
                        self.config['google']['source_incident_range'],
                        source_data.index(row) + 1,
                        csa_id,
                        incident_type,
                        location
                    ))
                    
                    for result in cursor.stored_results():
                        if result.with_rows:
                            logging.info(f"Procedure result: {result.fetchall()}")
                    
                    self.db_connection.commit()
                    logging.info(f"Processed incident record for CSA ID {csa_id}")
                    
                except Error as e:
                    logging.error(f"Database error processing incident record: {str(e)}")
                    self.db_connection.rollback()
                except Exception as e:
                    logging.error(f"Error processing incident record: {str(e)}")
                    self.db_connection.rollback()

            cursor.close()
            
        except Exception as e:
            logging.error(f"Error in process_incident_data: {str(e)}")

    def process_all_data(self) -> None:
        """Process all form data"""
        try:
            self.process_travel_data()
            self.process_building_data()
            self.process_incident_data()
            logging.info("Completed processing all data")
        except Exception as e:
            logging.error(f"Error in process_all_data: {str(e)}")
        finally:
            if hasattr(self, 'db_connection') and self.db_connection.is_connected():
                self.db_connection.close()
                logging.info("Database connection closed")

if __name__ == "__main__":
    etl = SheetsETL()
    etl.process_all_data()