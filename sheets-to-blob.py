from __future__ import print_function
import pickle, os.path, json, sys, csv, re
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
import datetime as dt
from azure.storage.blob import BlockBlobService, ContentSettings

class GSheets():
    def __init__(self):
        try:
            with open(os.path.join(sys.path[0], "sheets_config.json"), "r") as f:
                self.config = json.loads(f.read())
                self.scopes = self.config['scopes']
                self.token_filename = self.config['token_filename']
                self.credentials_filename = self.config['credentials_filename']
                self.service = self.config['service']
                self.workbooks = self.config['workbooks']
                self.main_data_folder = 'data'
                self.backup_folder = 'bkp'
        except Exception as e:
            print(e)

    def get_data(self, sheet_id, sheet_name_range):
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(self.token_filename):
            with open(self.token_filename, 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_filename, self.scopes)
                creds = flow.run_local_server()
            # Save the credentials for the next run
            with open(self.token_filename, 'wb') as token:
                pickle.dump(creds, token)

        service = build(self.service[0], self.service[1], credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id,
                                    range=sheet_name_range).execute()                       
        values = result.get('values', [])
 
        if not values:
            print('No data found.')
            return(None)
        else:
            df = pd.DataFrame(values)
            header = df.iloc[0]
            df = df[1:]
            df.columns = header
            return(df)                  
    
    def concatenate_name_range(self, sheet_name, sheet_range):
        return(sheet_name + "!" + sheet_range)

    def concatenate_filename(self, workbook_filename, sheet_filename, extension):
        return(workbook_filename + "_" + sheet_filename + "." + extension)

    def create_directory(self, workbook_filename):
        main = self.main_data_folder
        bkp = self.backup_folder
        if not os.path.exists(os.path.join(sys.path[0], main)):
            os.makedirs(os.path.join(sys.path[0], main))
        if not os.path.exists(os.path.join(sys.path[0], main, workbook_filename)):
            os.makedirs(os.path.join(sys.path[0], main, workbook_filename))
        if not os.path.exists(os.path.join(sys.path[0], main, bkp)):
            os.makedirs(os.path.join(sys.path[0], main, bkp))

    def get_backup_filename(self, csv_filename):
        date = dt.datetime.now()
        time = date.strftime("%H%M%S")
        date = date.strftime("%Y%m%d")
        filename = date + "_" + time + "_bkp_" + csv_filename
        return(filename)

    def get_current_csv(self, wb_filename, csv_filename):
        main = self.main_data_folder
        try:
            df = pd.read_csv(os.path.join(sys.path[0], main, wb_filename, csv_filename))
            return(df)
        except Exception as e:
            print(e)
            return(None)

    def save_csv(self, df, path_file):
        try:
            df.to_csv(path_file, index=False, encoding='utf-8')
        except Exception as e:
            print(e)

    def save_data(self):
        for wb in self.workbooks:
            if wb['active'] == True:
                for sheet in wb['sheets']:
                    if sheet['active'] == True:
                        wb_filename = wb['file_name']
                        sheet_filename = sheet['file_name']
                        sheet_id = wb['id']
                        sheet_name_range = self.concatenate_name_range(sheet_name=sheet['name'],
                                                                        sheet_range=sheet['range'])
                        
                        self.create_directory(wb_filename)
                        
                        df = self.get_data(sheet_id=sheet_id, 
                                        sheet_name_range=sheet_name_range)

                        if df is not None:
                            csv_filename = self.concatenate_filename(workbook_filename=wb_filename,
                                                                    sheet_filename=sheet_filename,
                                                                    extension="csv")
                            # Save backup
                            self.save_csv(df=self.get_current_csv(wb_filename=wb_filename, csv_filename=csv_filename), 
                                            path_file=os.path.join(sys.path[0], self.main_data_folder, self.backup_folder, self.get_backup_filename(csv_filename=csv_filename)))

                            # Save new file
                            self.save_csv(df=df, path_file=os.path.join(sys.path[0], self.main_data_folder, wb_filename, csv_filename))

class Blob():
    def __init__(self):
        try:
            with open(os.path.join(sys.path[0], "blob_config.json"), "r") as f:
                self.config = json.loads(f.read())
                self.blob_account_name = self.config['blob_account_name']
                self.blob_account_key = self.config['blob_account_key']
                self.blob_image_container = self.config['blob_image_container']
        except Exception as e:
            print(e)

    def save_in_blob(self, df, filename):
        data = df.to_csv(index=False, encoding='utf-8')
        try:
            block_blob_service = BlockBlobService(
                account_name=self.config['blob_account_name'], 
                account_key=self.config['blob_account_key'])
            block_blob_service.create_blob_from_text(self.config['blob_image_container'], filename, data)
        except Exception as e:
            print(e)
    
    def get_blobs_list(self):
        try:
            block_blob_service = BlockBlobService(
                account_name=self.config['blob_account_name'], 
                account_key=self.config['blob_account_key'])
            blobs_list = block_blob_service.list_blobs(
                container_name=self.config['blob_image_container'])
            return(blobs_list)
        except Exception as e:
            print(e)
    
    def list_blobs(self):
        try:
            blobs_list = self.get_blobs_list()
            for blob in blobs_list:
                print(blob.name)
        except Exception as e:
            print(e)
    
    def remove_blob(self, blob):
        try:
            block_blob_service = BlockBlobService(
                account_name=self.config['blob_account_name'],  
                account_key=self.config['blob_account_key'])
            block_blob_service.delete_blob(
                container_name=self.config['blob_image_container'],
                blob_name=blob
            )
        except Exception as e:
            print(e)

    def search_and_save_in_blob(self, path_file, ignore_folder):
        for root, dirs, files in os.walk(path_file):
            dirs[:] = [item for item in dirs[:] if item not in ignore_folder]
            for file in files:
                df = pd.read_csv(os.path.join(root, file))
                self.save_in_blob(df=df, filename=file)


   
if __name__ == '__main__':
    gsheets = GSheets()
    gsheets.save_data()
    
    blob = Blob()
    ignore = ['bkp']
    blob.search_and_save_in_blob(path_file=os.path.join(sys.path[0], gsheets.main_data_folder), 
                                ignore_folder=ignore)
    blob.list_blobs()













