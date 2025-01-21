import pandas as pd
import os
import time
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring,fromstring, ElementTree, ParseError
import paramiko
import re
import requests
import json
import sys
import time
import os
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from urllib3.util.retry import Retry
import ssl
from urllib3 import PoolManager

class POProcessor:
    def __init__(self, input_file: str, xml_template_path: str, wait_time: int = 30):
        self.input_file_path = input_file
        self.xml_template_path = xml_template_path
        self.wait_time = wait_time
        self.start_time = time.time()
        self.sftp_host = "customerinsights-ai.smartfile.com"
        self.sftp_port = 22
        self.sftp_username = "customerinsights-ai"
        self.sftp_password = "C2}8JDv\Qt7H"
        self.sftp_upload_path = "/Dermavant/IRP_Testing/PO_Orders/Synapse_Test/1_VALIDATION"

        # Load input file and XML template
        self.input_file = pd.read_excel("https://dermavant.customerinsights.ai/ds/yFZXNJAXtDsSZlo",dtype={"Order_P.O_Number": str},engine='openpyxl')
        print("\nInput File Contents:")
        print(self.input_file.head(10))  # Print the first 10 rows of the input file
        print(f"\nTotal rows in Input File: {len(self.input_file)}")
        self.xml_template = pd.read_excel("https://dermavant.customerinsights.ai/ds/7xJ4KEXPAfZXUq9",engine='openpyxl')
        print("\nXML Template Contents:")
        print(self.xml_template.head(10))  # Print the first 10 rows of the XML template
        print(f"\nTotal rows in XML Template: {len(self.xml_template)}")
        self.template_columns = self.xml_template.columns.tolist()

        # Initialize tracking for Order_ID
        self.last_order_id = self._get_last_order_id()
        


    def upload_to_sftp(self, file_content: str, file_name: str):
        """Upload a file to the SFTP server."""
        try:
            transport = paramiko.Transport((self.sftp_host, self.sftp_port))
            transport.connect(username=self.sftp_username, password=self.sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            # Specify the file path on the server
            remote_file_path = f"{self.sftp_upload_path}/{file_name}"

            # Create a temporary local file to upload
            local_file_path = f"/tmp/{file_name}"
            with open(local_file_path, "w") as temp_file:
                temp_file.write(file_content)

            # Upload the file
            sftp.put(local_file_path, remote_file_path)
            print(f"Successfully uploaded {file_name} to {self.sftp_upload_path}")

            # Close connections
            sftp.close()
            transport.close()
        except Exception as e:
            print(f"Failed to upload file to SFTP server: {e}")

    def _get_last_order_id(self) -> int:
        """Retrieve the last generated Order_ID from all possible sources."""
        last_id = 4999
        
        try:
            # Check updated_input_table
            updated_table = pd.read_excel("https://dermavant.customerinsights.ai/ds/L9W9ozHKFtSJ8aW",engine='openpyxl')
            if not updated_table.empty and 'Order_ID' in updated_table.columns:
                numeric_order_ids = pd.to_numeric(
                    updated_table['Order_ID'].str.extract(r'CIAI(\d+)')[0], 
                    errors='coerce'
                )
                if not numeric_order_ids.empty and not numeric_order_ids.isna().all():
                    last_id = max(last_id, int(numeric_order_ids.max()))
        except Exception as e:
            print(f"Error checking updated_input_table: {e}")

        try:
            # Also check latest_record_table
            latest_table = pd.read_excel("https://dermavant.customerinsights.ai/ds/n3t6XG40369gwlr",engine='openpyxl')
            if not latest_table.empty and 'Order_ID' in latest_table.columns:
                numeric_order_ids = pd.to_numeric(
                    latest_table['Order_ID'].str.extract(r'CIAI(\d+)')[0], 
                    errors='coerce'
                )
                if not numeric_order_ids.empty and not numeric_order_ids.isna().all():
                    last_id = max(last_id, int(numeric_order_ids.max()))
        except Exception as e:
            print(f"Error checking latest_record_table: {e}")

        print(f"Retrieved last Order_ID: CIAI{str(last_id).zfill(11)}")
        return last_id

    def generate_order_id(self) -> str:
        """Generate a unique Order_ID."""
        self.last_order_id += 1
        new_order_id = f"CIAI{str(self.last_order_id).zfill(11)}"
        print(f"Generated new Order_ID: {new_order_id}")
        return new_order_id

    def format_po_number(self, po_number: str) -> str:
        """Truncate PO number to 20 characters and ensure it is a string."""
        if pd.isnull(po_number) or str(po_number).strip() == "":
            return "UNKNOWN_PO"
        return str(po_number).strip()[:20]

    def format_datetime(self, dt: datetime) -> str:
        """Format datetime to 'yyddmmhhmmss' format."""
        return dt.strftime('%y%m%d%H%M%S')

    def transform_record(self, record: pd.Series) -> dict:
        """Transform a single record."""
        formatted_order_id = self.generate_order_id()
        po_number = str(record.get('Order_P.O_Number', 'UNKNOWN_PO'))
        formatted_po_number = self.format_po_number(po_number)
        #formatted_po_number = self.format_po_number(record.get('Order_P.O_Number', 'UNKNOWN_PO'))
        order_date = pd.to_datetime(record['Order_Date']).strftime('%Y-%m-%d')
        date_time_stamp = pd.to_datetime(record['Date_Time_Stamp']).strftime('%Y-%m-%d %H:%M:%S')
        formatted_datetime = self.format_datetime(pd.to_datetime(record['Date_Time_Stamp']))

        return {
            'User_Name': record['User_Name'],
            'Order_Date': order_date,
            'Order_P.O_Number': formatted_po_number,
            'Quantity': record['Quantity'],
            'Date_Time_Stamp': date_time_stamp,
            'Formatted_date_time_stamp': formatted_datetime,
            'Customer_Name': record['Customer_Name'],
            'Pharmacy_NPI': record['Pharmacy_NPI'],
            'Order_ID': formatted_order_id,
        }

    def generate_dynamic_xml(self, record: dict, root_element="temp"):
        """Generate dynamic XML content for a single record."""
        root = Element(root_element)
        for key, value in record.items():
            print(f"processing key is {key}")
            if key == "InfinisId":  # Skip processing this key
                continue
            hierarchy = key.split("/")
            self.create_nested_elements(root, hierarchy, value)
        return  "".join([tostring(child, 'utf-8').decode('utf-8') for child in root])

    def create_nested_elements(self, root, hierarchy, value):
        """Recursively create nested elements for XML."""
        if not hierarchy:
            return
        current_tag = hierarchy[0]
        child = root.find(current_tag)
        if child is None:
            child = SubElement(root, current_tag)
        if len(hierarchy) == 1:
            child.text = str(value) if pd.notnull(value) else ""
        else:
            self.create_nested_elements(child, hierarchy[1:], value)
            
    def generate_xml_format_record(self, transformed_record: dict) -> dict:
        """Transform a record into the XML generation format."""
        xml_format = {col: '' for col in self.template_columns}
        formatted_order_date = pd.to_datetime(transformed_record['Order_Date']).strftime('%Y-%m-%d')
        # Update specific columns
        xml_format.update({
            'Medusa/CostCenter': '1651',
            'Medusa/Template': '1651',
            'Medusa/FileType': 'OrdersAndCustomerNoLicense',
            'Medusa/Info/CustomerID': transformed_record['Pharmacy_NPI'],
            'Medusa/Info/OrderID': transformed_record['Order_ID'],
            'Medusa/Info/OrderHeader/OrderDate': formatted_order_date,
            'Medusa/Info/OrderHeader/OrderReference': self.format_po_number(transformed_record['Order_P.O_Number']),
            'Medusa/Info/OrderDetail/OrderDate': formatted_order_date,
            'Medusa/Info/OrderDetail/ProductCode': '81672505101',  # Static value
            'Medusa/Info/OrderDetail/Quantity': transformed_record['Quantity'],
        })

        print(xml_format)

        return xml_format


    def process_latest_order(self):
        """Process all unprocessed records by joining with the updated table."""
        # Handle nulls in input_file
        self.input_file['Order_P.O_Number'].fillna('UNKNOWN_PO', inplace=True)
        self.input_file['Pharmacy_NPI'].fillna('UNKNOWN_NPI', inplace=True)
        self.input_file['Date_Time_Stamp'].fillna('UNKNOWN_DATE', inplace=True)

        self.input_file['Unique_Key'] = (
            self.input_file['Order_P.O_Number'].apply(self.format_po_number).str.strip() + '_' +
            self.input_file['Pharmacy_NPI'].astype(str).str.strip() + '_' +
            self.input_file['Date_Time_Stamp'].astype(str).str.strip()
        )

        # Load the updated table
        try:
            updated_table = pd.read_excel("https://dermavant.customerinsights.ai/ds/L9W9ozHKFtSJ8aW",engine='openpyxl')
            print(f"updated table is {updated_table.head(5)}")
            print(f"updated table column are {updated_table.columns}")
        except Exception as e:
            print(f"No existing updated_input_table found: {e}")
            updated_table = pd.DataFrame(columns=['Unique_Key', 'status'])

        # Create a complete status tracking DataFrame
        all_records_status = pd.DataFrame({
            'Unique_Key': self.input_file['Unique_Key'].unique(),
            'status': 'unprocessed'
        })

        # Update status from existing updated_table
        if not updated_table.empty:
            existing_status = updated_table[['Unique_Key', 'status']].drop_duplicates(subset=['Unique_Key'], keep='last')
            all_records_status = all_records_status.merge(
                existing_status,
                on='Unique_Key',
                how='left',
                suffixes=('', '_updated')
            )
            all_records_status['status'] = all_records_status['status_updated'].fillna(all_records_status['status'])
            all_records_status = all_records_status[['Unique_Key', 'status']]

        # Merge with input file
        merged_data = self.input_file.merge(
            all_records_status,
            on='Unique_Key',
            how='left'
        )

        # Filter for unprocessed records
        unprocessed_data = merged_data[merged_data['status'] == 'unprocessed']
        unprocessed_data = unprocessed_data.sort_values(by='Date_Time_Stamp', ascending=True)

        print(f"Unprocessed Records Count: {len(unprocessed_data)}")
        print(f"Unprocessed Records Preview:\n{unprocessed_data[['Unique_Key', 'status', 'Date_Time_Stamp']]}")

        if unprocessed_data.empty:
            print("No unprocessed records found.")
            return False

        # Process each unprocessed record
        for _, record in unprocessed_data.iterrows():
            transformed_record = self.transform_record(record)
            print(f"Transofrmed record is {transformed_record}")
            json = {
                'data':[
                    transformed_record
                ]
            }
            response = requests.post('https://ciparthenon-api.azurewebsites.net/apiRequest?account=dmvtsynapse&route=data/816530/insert?api_version=2022.01',json=json)

            print(response.json())
            

            # Save processed records to tables
            #DataScript.get_output_table('latest_record_table').insert_rows([transformed_record])
            xml_format_record = self.generate_xml_format_record(transformed_record)
            xml_format_record.pop('InfinisId', None)
            print(f"xml_format_record is {xml_format_record}")
            json_xml = {
                'data':[
                    xml_format_record
                ]
            }

            response = requests.post('https://ciparthenon-api.azurewebsites.net/apiRequest?account=dmvtsynapse&route=data/816118/insert?api_version=2022.01',json=json_xml)
            print(response.json())
            #DataScript.get_output_table('xml_format_table').insert_rows(xml_format_record)
            xml_record = self.generate_dynamic_xml(xml_format_record)
            print(f"xml record is {xml_record}")
            formatted_datetime_xml = pd.to_datetime(transformed_record['Date_Time_Stamp']).strftime('%Y%m%d%H%M%S')
            file_name = f"Dermavant_ICS_PickReq_{transformed_record['Order_ID']}_{formatted_datetime_xml}.xml"
            self.upload_to_sftp(xml_record, file_name)
            json_final_xml = {
                'data':[
                    {"<!--This is a comment-->":xml_record}
                ]
            }
            response = requests.post('https://ciparthenon-api.azurewebsites.net/apiRequest?account=dmvtsynapse&route=data/816165/insert?api_version=2022.01',json=json_final_xml)
            print(response.json())
            #DataScript.get_output_table('xml_output_table').insert_rows([{'<!--This is a comment-->': xml_record}])
            
            # Create a complete record with status
            completed_record = record.to_dict()
            completed_record['status'] = 'completed'
            completed_record['validation_status'] ='Placed in validation'
            completed_record['Order_ID'] = transformed_record['Order_ID']
            order_date_complete = pd.to_datetime(completed_record['Order_Date']).strftime('%Y-%m-%d')
            date_time_stamp_complete = pd.to_datetime(completed_record['Date_Time_Stamp']).strftime('%Y-%m-%d %H:%M:%S')
            completed_record['Order_Date'] = order_date_complete
            completed_record['Date_Time_Stamp']=date_time_stamp_complete

            # Update the updated_input_table
            json_complete_record = {
                'data':[
                    completed_record
                ]
            }
            response = requests.post('https://ciparthenon-api.azurewebsites.net/apiRequest?account=dmvtsynapse&route=data/816494/insert?api_version=2022.01',json=json_complete_record)
            print(response.json())
            #DataScript.get_output_table('updated_input_table').insert_rows([completed_record])

            print(f"Processed and updated record: {transformed_record['Order_ID']}")
        

        return True

    def start_processing(self):
        """Process all unprocessed orders once."""
        processed = self.process_latest_order()
        if not processed:
            print("No unprocessed records found.")
        print("Processing completed.")
        
    def monitor_and_process(self):
        """
        Continuously monitor the input table for new records and process them.
        """
        print("Monitoring input table for new records...")
        
        # Track previously processed Unique_Keys
        updated_table = pd.read_excel("https://dermavant.customerinsights.ai/ds/L9W9ozHKFtSJ8aW",engine='openpyxl')
        processed_keys = set(updated_table['Unique_Key'])
        print(processed_keys)
        
        while True:
            # Reload the input table
            try:
                current_input = pd.read_excel("https://dermavant.customerinsights.ai/ds/yFZXNJAXtDsSZlo",engine='openpyxl')
                current_input['Unique_Key'] = (
                    current_input['Order_P.O_Number'].astype(str).str.strip() + '_' +
                    current_input['Pharmacy_NPI'].astype(str).str.strip() + '_' +
                    current_input['Date_Time_Stamp'].astype(str).str.strip()
                )
            except Exception as e:
                print(f"Error loading input table: {e}")
                break
            
            # Identify new records
            new_records = current_input[~current_input['Unique_Key'].isin(processed_keys)]
            
            if not new_records.empty:
                print(f"New records found: {len(new_records)}")
                self.input_file = current_input  # Update the current input table
                self.start_processing()  # Re-run the process
                processed_keys.update(new_records['Unique_Key'])  # Update processed keys
            else:
                print("No new records found. Exiting monitoring...")
                break
            
            # Wait for a specific interval before checking again
            time.sleep(self.wait_time)

import pandas as pd
from xml.etree.ElementTree import fromstring, ParseError
import paramiko
import re

class POProcessorValidation:
    def __init__(self, sftp_host, sftp_port, sftp_username, sftp_password):
        self.sftp_host = sftp_host
        self.sftp_port = sftp_port
        self.sftp_username = sftp_username
        self.sftp_password = sftp_password

    def find_file_by_order_id(self, directory_path, order_id):
        """
        Find a file in the directory that matches the given Order_ID.
        """
        try:
            # Connect to the SFTP server
            transport = paramiko.Transport((self.sftp_host, self.sftp_port))
            transport.connect(username=self.sftp_username, password=self.sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            # List all files in the directory
            files = sftp.listdir(directory_path)
            print(f"Files in {directory_path}: {files}")

            # Filter files based on Order_ID
            matching_files = [file for file in files if str(order_id) in file]

            if not matching_files:
                print(f"No matching files found for Order_ID: {order_id}")
                return None

            # Assuming there's only one matching file
            matching_file = matching_files[0]
            print(f"Found matching file: {matching_file}")

            # Close the SFTP connection
            sftp.close()
            transport.close()

            return f"{directory_path}/{matching_file}"
        except Exception as e:
            print(f"Error finding file by Order_ID: {e}")
            return None
    def extract_info_from_filename(self, filename):
        """Extract OrderID (TEST...) and timestamp (2024...) from filename with format Dermavant_ICS_PickReq_TEST{numbers}_2024{numbers}.xml"""
        try:
            # Pattern to match the exact format
            pattern = r'Dermavant_ICS_PickReq_(CIAI\d+)_(\d{14})'
            match = re.search(pattern, filename)
            
            if match:
                order_id = match.group(1)  # Gets the TEST00000000XX part
                timestamp = match.group(2)  # Gets the YYYYMMDDhhmmss part
                print(f"Extracted from filename - OrderID: {order_id}, Timestamp: {timestamp}")
                
                # Format timestamp to match the table format (YYYYMMDD)
                formatted_timestamp = timestamp[:8]  # Take only YYYYMMDD part
                
                return {
                    'order_id': order_id,
                    'timestamp': timestamp
                }
            else:
                print(f"Could not extract information from filename: {filename}")
                return {
                    'order_id': None,
                    'timestamp': None
                }
        except Exception as e:
            print(f"Error extracting information from filename: {e}")
            return {
                'order_id': None,
                'timestamp': None
            }


    def read_xml_to_dataframe_from_sftp(self, remote_path):
        """Read an XML file from the SFTP server, convert it to a DataFrame, and return."""
        try:
            # Connect to the SFTP server
            transport = paramiko.Transport((self.sftp_host, self.sftp_port))
            transport.connect(username=self.sftp_username, password=self.sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            # Open the file on the SFTP server
            with sftp.open(remote_path, "r") as remote_file:
                xml_content = remote_file.read().decode("utf-8")
                print("Successfully read XML file content.")
                print(f"XML Content:\n{xml_content}")

            # if not xml_content.strip().startswith("<Root>"):
            #     xml_content = f"<Root>{xml_content}</Root>"

            # Parse the XML content
            try:
                root = fromstring(xml_content)
            except ParseError as e:
                print(f"XML Parse Error: {e}")
                return xml_content, pd.DataFrame()

            # Convert XML to a list of dictionaries
            records = []
            for medusa in root.findall("."):
                record_data = {
                    "CustomerID": medusa.findtext("./Info/CustomerID"),
                    "OrderID": medusa.findtext("./Info/OrderID"),
                    "OrderHeader_OrderDate": medusa.findtext("./Info/OrderHeader/OrderDate"),
                    "OrderReference": medusa.findtext("./Info/OrderHeader/OrderReference"),
                    "Quantity": medusa.findtext("./Info/OrderDetail/Quantity"),
                }
                records.append(record_data)

            # Convert list of dictionaries to a DataFrame
            df = pd.DataFrame(records)

            # Close the SFTP connection
            sftp.close()
            transport.close()

            return xml_content, df
        except Exception as e:
            print(f"Error reading and converting XML file to DataFrame: {e}")
            return "", pd.DataFrame()  # Return an empty DataFrame in case of failure

    def validate_xml_with_updated_table(self, xml_df, updated_input_table, order_id):
        """Validate XML data against the updated_input_table for the running Order_ID."""
        try:
           
            # Filter updated_input_table for the specific Order_ID
            matching_record = updated_input_table[updated_input_table['Order_ID'] == order_id]
            print(matching_record)

            if matching_record.empty:
                print(f"No matching record found in updated_input_table for Order_ID: {order_id}")
                return xml_df

            # Concatenate fields for the matching record
            concat_transformed = (
                str(matching_record['Pharmacy_NPI'].values[0]).strip() +
                str(matching_record['Order_P.O_Number'].values[0])[:20].strip() +
                str(matching_record['Quantity'].values[0]).strip() +
                str(matching_record['Order_ID'].values[0]).strip() +
                pd.to_datetime(matching_record['Order_Date'].values[0]).strftime('%Y%m%d')
            )

            # Validate XML rows one by one
            validation_results = []
            for _, xml_row in xml_df.iterrows():
                concat_xml = (
                    str(xml_row['CustomerID']).strip() +
                    str(xml_row['OrderReference']).strip() +
                    str(xml_row['Quantity']).strip() +
                    str(xml_row['OrderID']).strip() +
                    # str(xml_row['OrderHeader_OrderDate']).strip()+
                    pd.to_datetime(str(xml_row['OrderHeader_OrderDate']).strip()).strftime('%Y%m%d')
                )

                print(f"\nValidating XML Concatenated: {concat_xml}")
                print(f"Transformed Record Concatenated: {concat_transformed}")

                validation_result = 'Success' if concat_transformed == concat_xml else 'Fail'
                validation_results.append(validation_result)

                if validation_result == 'Fail':
                    print("\nFailed Validation Details:")
                    print(f"XML Concatenated: {concat_xml}")
                    print(f"Transformed Record Concatenated: {concat_transformed}")

            # Add validation results to XML DataFrame
            xml_df['Validation'] = validation_results
            print(xml_df)
            return xml_df
        except Exception as e:
            print(f"Error during validation: {e}")
            return pd.DataFrame()

    def validate_and_move_in_ciai_ftp(self, file_name, file_content, transformed_record):
        """Validate and move XML files within the CIAI FTP."""
        try:
            hostname = self.sftp_host
            username = self.sftp_username
            password = self.sftp_password
            validation_path = '/Dermavant/IRP_Testing/PO_Orders/Synapse_Test/1_VALIDATION'
            processed_path = '/Dermavant/IRP_Testing/PO_Orders/Synapse_Test/3_PROCESSED'
            review_path = '/Dermavant/IRP_Testing/PO_Orders/Synapse_Test/2_READYTOREVIEW'

            # Connect to the SFTP server
            transport = paramiko.Transport((hostname, 22))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            order_id_match = re.search(r"<OrderID>(.*?)</OrderID>", file_content)
            order_id = order_id_match.group(1)
            updated_input_table = pd.read_excel("https://dermavant.customerinsights.ai/ds/L9W9ozHKFtSJ8aW", engine="openpyxl")
            matching_record = updated_input_table[updated_input_table['Order_ID'] == order_id]

            # Extract OrderID from file content
            
            print(f"order_id_match from regex :{order_id_match}")
            filename_info = self.extract_info_from_filename(file_name)
            print(f"Filename info extracted: {filename_info}")

        # Initialize validation results
            validations = {
                'existing_validation': False,
                'orderid_match': False,
                'timestamp_match': False
            }
            if order_id_match:
                order_id_from_file = order_id_match.group(1)
                print(order_id_from_file)

                # Compare with transformed_record['Order_ID']
                print(f"Transformed record Order_ID: {transformed_record['Order_ID']}")
                print(f"Validation status: {transformed_record.get('Validation')}")
                print(transformed_record)
                if (transformed_record['Order_ID'] is not None and order_id_from_file == transformed_record['Order_ID'] and transformed_record.get('Validation') == 'Success'):
                    validations['existing_validation'] = True                                      
                    # target_path = f"{processed_path}/{file_name}"
                    # print(f"Validation success for OrderID: {order_id_from_file}. Moving to PROCESSED.")
                    # file_success = True
                if filename_info['order_id'] == transformed_record['Order_ID']:
                    validations['orderid_match'] = True
                    print("OrderID in filename matches input table")
                else:
                    print("OrderID mismatch between filename and input table")
                print(f"matching record is {matching_record}")
                table_timestamp = pd.to_datetime(matching_record['Date_Time_Stamp'].values[0]).strftime('%Y%m%d%H%M%S')
                print(f"table_timestamp  is {table_timestamp}")
                if filename_info['timestamp'] == table_timestamp:
                    validations['timestamp_match'] = True
                    print("Timestamp in filename matches input table")
                else:
                    print("Timestamp mismatch between filename and input table")
                if validations['existing_validation'] and (validations['orderid_match'] and validations['timestamp_match']):
                    target_path = f"{processed_path}/{file_name}"
                    file_success = True
                    new_validation_status = 'Placed in Processed'
                    print("Moving to PROCESSED - validation checks passed")
                else:
                    target_path = f"{review_path}/{file_name}"
                    print(f"Validation failed for OrderID: {order_id_from_file}. Moving to READYTOREVIEW.")
                    new_validation_status = 'Placed in Review'
                    file_success = False
            else:
                target_path = f"{review_path}/{file_name}"
                print(f"OrderID not found in file. Moving to READYTOREVIEW.")
                new_validation_status = 'Placed in Review'
                file_success = False
            if target_path:
            # Write the file to the target folder
                with sftp.file(target_path, 'w') as dest_file:
                    dest_file.write(file_content)

                # Remove the file from validation folder
                validation_file_path = f"{validation_path}/{file_name}"
                sftp.remove(validation_file_path)
                print(f"File moved and removed from validation folder: {file_name}")
                if order_id and not matching_record.empty:
                    try:
                        # Take the existing record and update its validation status
                        record_to_update = matching_record.iloc[0].to_dict()
                        record_to_update['validation_status'] = new_validation_status
                        order_date_complete_val = pd.to_datetime(record_to_update['Order_Date']).strftime('%Y-%m-%d')
                        date_time_stamp_complete_val = pd.to_datetime(record_to_update['Date_Time_Stamp']).strftime('%Y-%m-%d %H:%M:%S')
                        record_to_update['Order_Date'] = order_date_complete_val
                        updated_date_at = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                        record_to_update['Updated_AT'] = updated_date_at
                        record_to_update['Date_Time_Stamp']=date_time_stamp_complete_val

                        print(record_to_update)
                        # record_to_update['validation_status'] = new_validation_status
                        # Post update to API
                        json_update = {
                            "data": [
                                record_to_update
                            ]
                        }
                        response = requests.post(
                            'https://ciparthenon-api.azurewebsites.net/apiRequest?account=dmvtsynapse&route=data/820414/insert?api_version=2022.01',
                            json=json_update
                        )
                        print(f"Updated validation status to {new_validation_status} for Order_ID {order_id}")
                        print(response.json())
                    except Exception as e:
                        print(f"Error updating validation status: {e}")



            # Close the SFTP connection
            sftp.close()
            transport.close()

            return file_success
        except Exception as e:
            print(f"Error during CIAI FTP validation and file movement: {e}")
            return False

    def process_all_files_in_directory(self, directory_path, updated_input_table):
        """
        Process and validate all files in the given directory.
        """
        try:
            # Connect to the SFTP server
            transport = paramiko.Transport((self.sftp_host, self.sftp_port))
            transport.connect(username=self.sftp_username, password=self.sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            # List all files in the directory
            files = sftp.listdir(directory_path)
            print(f"Files in {directory_path}: {files}")

            if not files:
                print("No files found in the directory.")
                return False

            # Loop through each file
            for file_name in files:
                remote_path = f"{directory_path}/{file_name}"
                print(f"\nProcessing file: {remote_path}")

                # Read XML content and DataFrame
                file_content, xml_df = self.read_xml_to_dataframe_from_sftp(remote_path)
                if xml_df.empty:
                    print(f"Skipping file {file_name}: XML content is invalid or empty.")
                    continue

                # Extract Order_ID from file content
                order_id_match = re.search(r"<OrderID>(.*?)</OrderID>", file_content)
                if not order_id_match:
                    print(f"Skipping file {file_name}: Order_ID not found in XML content.")
                    continue

                order_id = order_id_match.group(1)
                print(f"Order_ID extracted: {order_id}")

                # Match Order_ID with updated_input_table
                matching_record = updated_input_table[updated_input_table['Order_ID'] == order_id]
                if matching_record.empty:
                    print(f"No matching record found for Order_ID: {order_id}. Skipping validation.")
                    self.validate_and_move_in_ciai_ftp(file_name, file_content, {'Order_ID': None,'Validation':None})
                    continue

                # Validate XML against the matching record
                validation_results = self.validate_xml_with_updated_table(xml_df, updated_input_table, order_id)

                # Log validation results
                print(f"Validation Results for OrderID {order_id}:")
                print(validation_results[['OrderID', 'Validation']])
                print(validation_results['Validation'].iloc[0])

                # Determine final file movement
                if validation_results['Validation'].iloc[0] == 'Success':
                    print(f"Validation successful for OrderID {order_id}. Moving file to PROCESSED.")
                    self.validate_and_move_in_ciai_ftp(file_name, file_content, {
                        'Order_ID': order_id,
                        'Validation':'Success'
                    })
                else:
                    print(f"Validation failed for OrderID {order_id}. Moving file to READYTOREVIEW.")
                    self.validate_and_move_in_ciai_ftp(file_name, file_content, {
                        'Order_ID': order_id,
                        'Validation':'Fail'
                    })

            # Close the SFTP connection
            sftp.close()
            transport.close()
            return True
        except Exception as e:
            print(f"Error during file processing: {e}")
            return False


def transfer_file_to_ics():
    # Source SFTP server details
    source_hostname = 'customerinsights-ai.smartfile.com'
    source_port = 22
    source_username = 'customerinsights-ai'
    source_password = 'C2}8JDv\Qt7H'
    source_remote_path = '/Dermavant/IRP_Testing/PO_Orders/Synapse_Test/3_PROCESSED'

    dest_remote_path = '/Dermavant/IRP_Testing/PO_Orders/Synapse_Test/3_PROCESSED/Archive'

    base_url = 'https://sftp.absg.com'
    username = 'DermavantSftp'
    password = 'w9W=J3#abFznKkjX'

    # Create SSH clients for source and destination servers
    source_transport = paramiko.Transport((source_hostname, 22))
    source_transport.connect(username=source_username, password=source_password)
    source_sftp = paramiko.SFTPClient.from_transport(source_transport)

    # Download file from source SFTP
    xml_files = [file_name for file_name in source_sftp.listdir(source_remote_path) if file_name.endswith('.xml')]
    print(xml_files)

    success_files = []
    comments = []
    for xml_file in xml_files:
        print('start on', xml_file)
        file_comment = {}
        file_comment['filename'] = xml_file
        comment = ""
        for i in range(10):
            try:
                with source_sftp.file(f"{source_remote_path}/{xml_file}", 'r') as source_file:
                    file_content = source_file.read()

                token_url = f"{base_url}/api/v1/token"
                token_data = {
                    'grant_type': 'password',
                    'username': username,
                    'password': password,
                }
                token_response = requests.post(token_url, data=token_data)
                token = token_response.json().get('access_token')
                print('token',token_response.status_code, token_response.json())
                if token_response.status_code != 200:
                    comment = "token request failed: "+ token_response.text
                    continue
                else:
                    file_url = f"{base_url}/api/v1/folders/132065798/files" #CIAI_TEST
                    #file_url = f"{base_url}/api/v1/folders/931648856/files" #Inbound
                    headers = {
                        'Authorization': f'Bearer {token}',
                    }
                    files = {'file': ("TEST_AK_"+xml_file,file_content)}
                    #files = {'file': (xml_file,file_content)}
                    response = requests.post(file_url, headers=headers,files=files)
                    print(response.status_code, response.text)
                    if response.status_code != 201:
                        comment = "upload response failed: "+ response.text
                        continue
                    else:
                        with source_sftp.file(f"{dest_remote_path}/{xml_file}", 'w') as dest_file:
                            dest_file.write(file_content)
                        time.sleep(10)
                        if xml_file in source_sftp.listdir(dest_remote_path):
                            source_sftp.remove(f"{source_remote_path}/{xml_file}")
                            print('success:',xml_file)
                            success_files.append(xml_file)
                            comment = 'success'
                            break
                        else:
                            comment = 'file move to PROCESSED folder failed'
                            continue
            except Exception as e: 
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                print(str(e))
                time.sleep(10)
                print('try again',i)
                comment = str(e)
        file_comment['comment'] = comment
        comments.append(file_comment)
    print(comments)
    # Close SFTP sessions and SSH connections
    source_sftp.close()
    source_transport.close()

    if success_files == xml_files:
        status = 'success'
    elif len(success_files) == 0:
        status = 'failed'
    else:
        status = 'partially success'

    json_result = {'status':status,'files':','.join(success_files),'comment':comments}
    string_result = json.dumps(json_result)
    return string_result
    
class WorkflowManager:
    def __init__(self):
        input_file = 'ICS_PO_Form'
        xml_template_path = 'RPT_PO_FORMAT'
        self.po_processor = POProcessor(input_file, xml_template_path)
        self.validator = POProcessorValidation(
            sftp_host="customerinsights-ai.smartfile.com",
            sftp_port=22,
            sftp_username="customerinsights-ai",
            sftp_password="C2}8JDv\\Qt7H"
        )
        self.processed_keys = set()

    def monitor_and_process(self):
        """
        Continuously monitor the input table for new records and process them, including validation.
        """
        print("Monitoring input table for new records...")

        # Track previously processed Unique_Keys
        try:
            updated_table = pd.read_excel("https://dermavant.customerinsights.ai/ds/L9W9ozHKFtSJ8aW",engine='openpyxl')
            self.processed_keys = set(updated_table['Unique_Key'])
            print(f"Loaded processed keys: {self.processed_keys}")
        except Exception as e:
            print(f"Error loading updated table: {e}")
            self.processed_keys = set()

        while True:
            try:
                # Reload the input table
                current_input =  pd.read_excel("https://dermavant.customerinsights.ai/ds/yFZXNJAXtDsSZlo",engine='openpyxl')
                current_input['Unique_Key'] = (
                    current_input['Order_P.O_Number'].astype(str).str.strip() + '_' +
                    current_input['Pharmacy_NPI'].astype(str).str.strip() + '_' +
                    current_input['Date_Time_Stamp'].astype(str).str.strip()
                )
            except Exception as e:
                print(f"Error loading input table: {e}")
                return {"status": "error", "message": f"Error loading input table: {e}"}
                break

            # Identify new records
            new_records = current_input[~current_input['Unique_Key'].isin(self.processed_keys)]
            if not new_records.empty:
                print(f"New records found: {len(new_records)}")

                try:# Step 1: XML Processing
                    self.po_processor.input_file = current_input  # Update the current input table
                    self.po_processor.start_processing() 
                except Exception as processing_error:
                    return {"status": "error", "message": f"Error during XML processing: {processing_error}"} # Process XML

                # Step 2: Validation
                try:
                    updated_input_table = pd.read_excel("https://dermavant.customerinsights.ai/ds/L9W9ozHKFtSJ8aW",engine='openpyxl')
                    self.validator.process_all_files_in_directory(
                        "/Dermavant/IRP_Testing/PO_Orders/Synapse_Test/1_VALIDATION",
                        updated_input_table
                    )
                except Exception as validation_error:
                    print(f"Validation error: {validation_error}")
                    return {"status": "error", "message": f"Error during validation: {validation_error}"}
                
                try:
                    print("Starting transfer to ICS...")
                    output = transfer_file_to_ics()
                    response = make_response(output, 200)
                    response.headers["Content-Type"] = "application/json"
                    return response
                    #print(f"Transfer to ICS completed with result: {transfer_result}")
                except Exception as transfer_error:
                    print(f"Error during file transfer to ICS: {transfer_error}")
                    return {"status": "error", "message": f"Error during file transfer to ICS: {transfer_error}"}               

                # Mark records as processed
                self.processed_keys.update(new_records['Unique_Key'])
            else:
                print("No new records found. Exiting monitoring...")
                break

            # Wait for a specific interval before checking again
            time.sleep(self.po_processor.wait_time)
        return {"status": "success", "message": "Workflow monitoring and processing completed successfully"}

    def run_complete_workflow(self):
        """
        Continuously monitor and process input records.
        """
        try:
            print("Starting complete workflow...")
            result=self.monitor_and_process()
            if result is None:
                result = {"status": "error", "message": "Unknown error occurred during workflow"}
            return result    
            # print("Complete workflow finished successfully.")
            # return {"status": "success", "message": "Workflow completed successfully"}
        except Exception as e:
            print(f"Error during complete workflow execution: {e}")
            return {"status": "error", "message": str(e)}




from flask import Flask, jsonify,make_response
import pandas as pd
import os
import time
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring
import paramiko
import requests
import json
import logging

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@app.route('/xml_generation', methods=['GET'])
def generate_xml():
    try:
        logger.info("Starting XML generation process")
        initial = time.time()
        
        # Initialize processor with file paths
        input_file = 'ICS_PO_Form'
        xml_template_path = 'RPT_PO_FORMAT'
        processor = POProcessor(input_file, xml_template_path)
        
        # Start processing
        processed = processor.start_processing()
        
        if processed:
            response = {
                'status': 'success',
                'message': 'XML generation completed successfully'
            }
            status_code = 200
        else:
            response = {
                'status': 'success',
                'message': 'No unprocessed records found'
            }
            status_code = 200
            
        logger.info(f"XML generation completed with status: {response['status']}")
        final = time.time()
        total_time = final -initial 
        print(f"total time taken is {total_time}")
        return jsonify(response), status_code
    
    except Exception as e:
        logger.error(f"Error during XML generation: {str(e)}", exc_info=True)
        response = {
            'status': 'error',
            'message': f'XML generation failed: {str(e)}'
        }
        return jsonify(response), 500
    
@app.route("/validate_files", methods=["GET"])
def validate_files():
    try:
        processor = POProcessorValidation(
            sftp_host="customerinsights-ai.smartfile.com",
            sftp_port=22,
            sftp_username="customerinsights-ai",
            sftp_password="C2}8JDv\\Qt7H",
        )
        remote_directory_path = "/Dermavant/IRP_Testing/PO_Orders/Synapse_Test/1_VALIDATION"
        updated_input_table = pd.read_excel("https://dermavant.customerinsights.ai/ds/L9W9ozHKFtSJ8aW", engine="openpyxl")
        processor.process_all_files_in_directory(remote_directory_path, updated_input_table)
        return jsonify({"status": "success", "message": "Validation completed"}), 200
    except Exception as e:
        logger.error(f"Error in validate_files: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

@app.route('/test_connection', methods=['GET'])
def test_connection():
    return "Connection successful", 200

@app.route('/transfer_xml_from_ciai_to_ics_test',methods = ['GET'])
def transfer_xml_from_ciai_to_ics():
    
    response = make_response()
    output=transfer_file_to_ics()
    response.data = output

    return response

@app.route('/complete_workflow', methods=['GET'])
def run_complete_workflow():
    try:
        logger.info("Starting complete workflow")
        initial = time.time()
        workflow_manager = WorkflowManager()
        
        result = workflow_manager.monitor_and_process()
        
        final = time.time()
        total_time = final - initial
        logger.info(f"Total time taken: {total_time}")
        
        return jsonify(result), 200 if result["status"] == "success" else 500
    
    except Exception as e:
        logger.error(f"Error during complete workflow execution: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Complete workflow failed: {str(e)}'
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
