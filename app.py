import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Class for transferring data from excel to DB
class ToSQL:
    def __init__(self, path, name_sheet, db_uri):
        # Path of the excel sheet
        self.path = path

        # The sheet name passed
        self.name_sheet = name_sheet

        # The URL of the database passed
        self.db_uri = db_uri

        # Reading the excel sheet
        self.df = pd.read_excel(self.path, sheet_name=name_sheet, engine='openpyxl', header=1)
        self.df_excel_file = pd.ExcelFile(path, engine='openpyxl')

        # Names of all the sheets
        self.sheet_names = self.df_excel_file.sheet_names

        # Splitting tables in a sheet
        self.tables = self.split_tables(self.df)

        # Getting all the headings of tables in a sheet
        self.sheet_table_headings = self.extract_table_names_in_sheet(name_sheet)

        # Telling user which sheet is being transferred
        print(f"In Sheet {name_sheet}")

        # This function transfers all the tables from a sheet give to the class
        self.transfer_all_tables_from_sheet(self.tables, self.sheet_table_headings)

    # This function splits the tables in a sheet
    def split_tables(self, df):

        # This will hold all the tables
        tables = {}

        # This holds the empty columns which helps us to separate the tables
        col_ranges = df.columns[df.isna().all()].tolist()

        # Marks the beginning of first table
        start = 0

        # Will be used to store the date column
        date_column = None

        # Used to detect the table separator entry column
        for i, col in enumerate(df.columns):

            # Checking if the column is an empty one or if the column is the last one
            if col in col_ranges or i == len(df.columns) - 1:

                # Sets the end index for slicing the table from dataframe. If last column then end range is in the end
                end = i if col in col_ranges else i + 1

                # Slicing the table using the start and end indexes
                table_df = df.iloc[:, start:end]

                # Testing the code
                # Dropping the empty rows and getting their indexes
                empty_row_indexes = table_df[table_df.isna().all(axis=1)].index
                # print(f"These are the empty indexes {empty_row_indexes}")

                table_df = table_df.dropna(how='all')


                # Checking if the DataFrame has any valid columns before dropping rows
                table_df_filtered = self.filter_unnamed_columns(table_df)
                if table_df_filtered.dropna(how='all').empty and i != len(df.columns) - 1:
                    print(f"Skipping empty DataFrame between columns {start} and {end}")

                    # Increments to mark the start of next table
                    start = i + 1

                    # Going to the next iteration
                    continue

                # drop empty rows after checking columns
                table_df = table_df_filtered.dropna(how='all')
                if not table_df.columns.empty: 
                    table_name = table_df.columns[0]

                    # For situations where each table already has its own column in sheets
                    no_date_column_present = True

                    for column_name, _ in table_df.dtypes.iteritems():
                        if 'date' in column_name.lower():
                            no_date_column_present = False
                            break

                    # Capture the date column from the first table if it is empty
                    
                    if date_column is None:
                        date_column = table_df.iloc[:, 0]
                    elif no_date_column_present:
                        # Dropping the indexes of the date column to align with the tables after the first table
                        valid_indexes = date_column.index.intersection(empty_row_indexes)
                        date_column = date_column.drop(valid_indexes)
                        table_df = pd.concat([date_column.reset_index(drop=True), table_df.reset_index(drop=True)], axis=1)

                    tables[table_name] = table_df
                else:
                    print(f"Empty DataFrame between columns {start} and {end}")
                start = i + 1

        # Checking the last segment if it was not added so all tables are added
        if start < len(df.columns):
            table_df = df.iloc[:, start:].dropna(how='all')
            table_df = self.filter_unnamed_columns(table_df)
            if not table_df.columns.empty:
                table_name = table_df.columns[0]
                if date_column is not None:
                    table_df = pd.concat([date_column.reset_index(drop=True), table_df.reset_index(drop=True)], axis=1)
                tables[table_name] = table_df

        return tables

    # Transferring all the tables to DB from a sheet
    def transfer_all_tables_from_sheet(self, tables, table_headings):
        for table_heading, (table_name, table_df) in zip(table_headings, tables.items()):
            table_df = self.filter_unnamed_columns(table_df)  # Filter unnamed columns again
            table_df.columns = self.clean_column_names(table_df.columns)
            self.df = table_df  # Set the current dataframe to the table dataframe
            print(f"Transferring data of table {table_heading}")

            if len(table_heading) > 59:
                table_heading = "long_table"

            self.connect_DB(table_heading)
            self.drop_empty_row()
            self.drop_empty_col()
            self.fill_nan_median() 
            self.preprocess_data()
            self.insert_into_DB()

    def filter_unnamed_columns(self, df):
        return df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]

    def clean_column_names(self, columns):
        cleaned_columns = []
        seen = set()
        for i, col in enumerate(columns):
            col_clean = col.strip().lower().replace(' ', '_').replace('(', '').replace(')', '').split('.')[0] if col else f'unnamed_{i}'
            if col_clean in seen:
                count = 1
                new_col_clean = f"{col_clean}_{count}"
                while new_col_clean in seen:
                    count += 1
                    new_col_clean = f"{col_clean}_{count}"
                col_clean = new_col_clean
            seen.add(col_clean)
            cleaned_columns.append(col_clean)
        return cleaned_columns

    def connect_DB(self, table_name):
        self.DATABASE_URI = self.db_uri
        self.engine = create_engine(self.DATABASE_URI)
        self.Base = declarative_base()

        truncated_table_name = self.truncate_table_name(table_name)

        self.NetworkData = self.create_dynamic_model(truncated_table_name)
        self.Base.metadata.create_all(self.engine)

    def truncate_table_name(self, table_name):
        max_length = 64
        name_sheet_part = self.name_sheet.replace(" ", "_").lower()
        table_name_part = table_name.replace(" ", "_").lower()
        return (name_sheet_part + "_" + table_name_part)[:max_length]

    def map_dtype(self, dtype):
        if dtype == 'int64':
            return Integer
        elif dtype == 'float64':
            return Float
        elif dtype == 'object':
            return String(255)
        elif dtype == 'datetime64[ns]':
            return Date
        else:
            return String(255)

    def create_dynamic_model(self, table_name):
        attrs = {
            '__tablename__': f'{self.name_sheet.replace(" ", "_")}_{table_name.replace(" ", "_")}',
            'id': Column(Integer, primary_key=True, autoincrement=True),
            'created_at': Column(DateTime, default=func.now()),
            'updated_at': Column(DateTime, default=func.now(), onupdate=func.now())
        }

        for column_name, dtype in self.df.dtypes.iteritems():
            attrs[column_name] = Column(self.map_dtype(str(dtype)))

        return type('NetworkData', (self.Base,), attrs)

    def extract_table_names_in_sheet(self, name_sheet):
        try:
            df_preview = pd.read_excel(self.path, sheet_name=name_sheet, engine='openpyxl', nrows=1, header=None)
            table_names = df_preview.iloc[0].dropna().tolist()
            return table_names
        except Exception as e:
            print(f"An error occurred: {e}")
            return []

    def print_sheet_and_table_names(self):
        try:
            for sheet in self.sheet_names:
                print(f"Tables in sheet {sheet} are:")
                table_name_sheet = self.extract_table_names_in_sheet(sheet)

                for name_of_table in table_name_sheet:
                    print(f'name_of_table\n')
                print("\n\n")

        except Exception as e:
            print(f"An error occurred: {e}")

    def drop_empty_row(self):
        print("Dropping empty rows")
        self.df = self.df.dropna(how='all')

    def drop_empty_col(self):
        print("Dropping empty columns")
        self.df = self.df.dropna(axis=1, how='all')

    def fill_nan_median(self):
        print("Filling NaN values with median")
        self.df = self.df.apply(lambda x: x.fillna(x.median()) if x.dtype in ['float64', 'int64'] else x)

    def preprocess_data(self):
        for column in self.df.columns:
            if 'date' in column.lower():
                self.df[column] = pd.to_datetime(self.df[column], errors='coerce')
                self.df[column] = self.df[column].apply(lambda x: None if pd.isnull(x) else x)  # Handle NaT
            elif 'float' in str(self.df[column].dtype) or 'capacity' in column.lower():
                self.df[column] = pd.to_numeric(self.df[column], errors='coerce').fillna(0.0)


    def insert_into_DB(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()

        for index, row in self.df.iterrows():
            if 'date' in row and pd.isnull(row['date']):
                # print(f"Skipping row {index} due to invalid date")
                continue
            row_data = {column: row[column] for column in self.df.columns}
            network_data = self.NetworkData(**row_data)

            # Either updates or inserts the record
            session.merge(network_data)  

        session.commit()

    # Used to check if data is saved to the table
    def is_data_saved_db(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()

        result = session.query(self.NetworkData).all()

        for record in result:
            logger.info(f"{record.id}, {[getattr(record, column) for column in self.df.columns]}")

# Please set your file path and your Database URI here
file_path = 'IGW, MGN, GGC, NGW, Akamai Utilization_with_bugs.xlsm'
db_uri = "mysql+pymysql://root:ahmed@127.0.0.1:3306/all_tables"

# This function prints the sheets and then executes the class for each sheet
def transfer_data_db(path):
    
    # Getting the sheet names from the excel sheet
    df = pd.ExcelFile(path, engine='openpyxl')
    sheet_names = df.sheet_names

    # Printing all the sheet names
    print("Following are all the sheets:")
    for sheet in sheet_names:
        print(f'{sheet}\n')

    # Transferring data from each sheet to the database
    for sheet in sheet_names:
        ToSQL(path, sheet, db_uri)

# For transferring all the sheets
transfer_data_db(file_path)

# For transferring just one sheet for testing
# to_sql = ToSQL(file_path, "IB-OB-TB", db_uri)
