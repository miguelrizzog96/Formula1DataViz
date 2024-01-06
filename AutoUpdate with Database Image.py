import os
import requests
import shutil
import gzip

def download_f1db():
    # URL of the file to download
    f1db_url = "http://ergast.com/downloads/f1db.sql.gz"

    # Local path to save the downloaded file
    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    local_gz_file_path = os.path.join(downloads_folder, "f1db.sql.gz")
    local_sql_file_path = os.path.join(downloads_folder, "f1db.sql")

    try:
        # Send a GET request to download the file
        response = requests.get(f1db_url, stream=True)

        # Check if the request was successful (HTTP status code 200)
        if response.status_code == 200:
            # Save the downloaded content to the local .gz file
            with open(local_gz_file_path, "wb") as file:
                response.raw.decode_content = True
                shutil.copyfileobj(response.raw, file)

            print("File downloaded successfully.")

            # Unzip the .gz file
            with gzip.open(local_gz_file_path, "rb") as gz_file:
                with open(local_sql_file_path, "wb") as sql_file:
                    shutil.copyfileobj(gz_file, sql_file)

            # Remove the .gz file after unzipping
            os.remove(local_gz_file_path)

            print("File unzipped and saved to Downloads folder.")
        else:
            print(f"Failed to download the file. HTTP status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the download: {e}")

if __name__ == "__main__":
    download_f1db()

import re

# Output: "This is a test; (with some- semicolons-) and more; semicolons; here."

import mysql.connector

def run_sql_file(sql_file_path):
    try:
        # Connect to the MySQL server
        
        print("Connected to the MySQL server.")

        # Create a cursor to execute SQL queries
        cursor = connection.cursor()

        # Read the SQL file content with 'utf-8-sig' encoding
        with open(sql_file_path, "r", encoding="utf-8-sig") as sql_file:
            sql_script = sql_file.read()
            # Regex pattern to find ";" inside parentheses
            pattern = r'\(([^)]*);([^)]*)\)'
            # Replacement string with "-"
            replacement = r'(\1-\2)'
            # Perform the regex replacement
            sql_script = re.sub(pattern, replacement, sql_script)

        # Execute the SQL script
        for statement in sql_script.split(';'):
            cursor.execute(statement)

        print("SQL script executed successfully.")

        # Commit the changes and close the connection
        connection.commit()
        connection.close()

    except mysql.connector.Error as e:
        print(f"An error occurred: {e}")
        if 'connection' in locals() and connection.is_connected():
            connection.rollback()
            connection.close()

if __name__ == "__main__":
    # MySQL server and database credentials
    connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='1234',
    database='f1db'
    )
    # path to SQL file
    sql_file_path = r"C:\Users\migue\Downloads\f1db.sql"

    if not os.path.exists(sql_file_path):
        print("The specified SQL file does not exist.")
    else:
        run_sql_file(sql_file_path)


import pandas as pd

def update_csv():
    #query for boxplots and lapcharts project
    q1="""
    SELECT d.*, l.name AS team 
    FROM dataset d 
    JOIN driverlineups l ON l.Driver=d.Driver AND l.year=d.year
    ORDER BY year, name, Team, Driver, lap; """
    q2=""" 
    SELECT r.year AS Season ,r.round, r.name AS Race, CONCAT(d.forename," ",d.surname) AS Driver_name,
    ln.name AS Team, lt.lap, lt.time AS Laptime , lt.milliseconds , lt.position AS Pos
    FROM drivers d
    JOIN laptimes lt ON lt.driverId=d.driverId
    JOIN races r ON lt.raceId=r.raceId
    JOIN DriverLineups ln ON CONCAT(d.forename," ",d.surname)=ln.Driver AND r.year =ln.year
    Where r.year >=2021
    ORDER BY r.year ASC, r.round ASC, lt.Lap ASC;
    """
    # MySQL server and database credentials
    connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='1234',
    database='f1db')
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()
    cursor.execute(q1)
    Boxplots = cursor.fetchall()
    #creating first DataFrame
    df1 = pd.DataFrame(Boxplots, columns=[col[0] for col in cursor.description])
    df1.columns = [col.capitalize() for col in df1.columns]
    cursor.execute(q2)
    Laptimes = cursor.fetchall()
    #Creating second Dataframe
    df2 = pd.DataFrame(Laptimes, columns=[col[0] for col in cursor.description])
    df1.columns = [col.capitalize() for col in df1.columns]
    #import data to csv
    df2.to_csv(r'G:\My Drive\04. Passion Projects\01. Formula1DataViz\04. Datasets\F1Lapcharts.csv', index=False)
    df1.to_csv(r'G:\My Drive\04. Passion Projects\01. Formula1DataViz\04. Datasets\laptimes 2023 Season for boxplots.csv', index=False)

update_csv()