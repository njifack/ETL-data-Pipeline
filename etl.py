# import libraries

import mysql.connector
from mysql.connector import Error
import pandas as pd

# Extract Data from ncbi 
 

def extract_data(url):
    data = pd.read_html(url)  
    return data


url = 'https://www.ncbi.nlm.nih.gov/clinvar/?term=ltbr[all]'

data_tabs = extract_data(url)
data = data_tabs[0]

# Save the DataFrame as CSV
data.to_csv("clinvar_data.csv", index=False)

clinvar_data = pd.read_csv("clinvar_data.csv")


#  Transform Data

def transform_data(data):
 
    
    clinvar_data.columns = [col.lower() for col in clinvar_data.columns]  # Convert column names to lowercase
    data = clinvar_data.dropna()   
    return data

data_transform = transform_data(clinvar_data) 


# lets create a database to save the data in mysql database 

def database_creation():
    try:
    
        conn = mysql.connector.connect(user="root",host="localhost",password="password")
        cursor = conn.cursor()
        query = "CREATE DATABASE GenomicClinVarDB"
        cursor.execute(query)
        print("DATABASE CREATED SUCCESSFULLY")
    except Error as e:
        print("an error occured:",e)
    finally:
        conn.close()
        cursor.close()

database_creation()


# create a table to load the data 

import mysql.connector
from mysql.connector import Error

def create_table():
    try:
        conn = mysql.connector.connect(user="root", host="localhost", password="password", database="GenomicClinVarDB")
        cursor = conn.cursor()
        query = """
        CREATE TABLE IF NOT EXISTS clinvar(
            id VARCHAR(255),
            variation VARCHAR(255),
            gene VARCHAR(255),
            type VARCHAR(255),
            `condition` VARCHAR(255),
            `classification` VARCHAR(255)
        )
        """
        cursor.execute(query)
        print("Table created successfully.")
    except Error as e:
        print(f"An error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

create_table()


# Load Data into Mysql dtabase

import mysql.connector
from mysql.connector import Error

def load_data_to_mysql(data, table_name):
    try:
        connection = mysql.connector.connect(user="root", host="localhost", password="password", database="GenomicClinVarDB")
        cursor = connection.cursor()

        # Inserting data row by row
        for row in data.itertuples(index=False, name=None):
            query = "INSERT INTO {} VALUES ({})".format(table_name, ', '.join(['%s'] * len(row)))
            cursor.execute(query, row)

        # Commit the transaction
        connection.commit()
        print("loaded successfully")
    except Error as e:
        print(f"Error: {e}")
        connection.rollback()

    finally:
        cursor.close()
        connection.close()

# Usage
load_data_to_mysql(data_transform, "clinvar")
