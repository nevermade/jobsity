from cmath import e
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import logging
import smtplib
from email.message import EmailMessage


CHUNK_SIZE = 30

"""
This function sends an email to the recipient user from the config file so this person can know when the uploading process has finished.
"""


def send_email(emailconfig, file, result):

    email_user = emailconfig["user"]
    email_password = emailconfig["password"]
    recipient = emailconfig["recipient"]

    gmail_user = email_user
    gmail_password = email_password

    msg = EmailMessage()
    msg.set_content(
        f"""
        ----------------------------
        {'Your manual upload was succesfully completed' if result==1 else 'Your manual upload has failed' }
        """
    )

    msg["Subject"] = (
        f"Uploading {file} has finished"
        if result == 1
        else f"Uploading {file} has failed"
    )

    msg["From"] = gmail_user
    msg["To"] = recipient

    try:
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(gmail_user, gmail_password)
        server.send_message(msg)
        server.quit()

        logging.info("Email sent!")
    except e:
        logging.info(e)


"""
-This is a function to upload a csv file to Snowflake. It requires a connection to snowflake, a fully qualified table name and the name of the file. 
-I assumed that the database is created. If the table does not exist then the script will create it.
-Since it requires specific libraries I used pipenv to create the pipfile.lock and pipfile files to better distribute this code.
"""


def load_csv_to_snowflake_table(
    credentials: dict,
    destination: dict,
    filename: str,
):

    conn = snowflake.connector.connect(
        user=credentials["user"],
        password=credentials["password"],
        account=credentials["account"],
    )
    try:
        database = destination["database"]
        schema = destination["schema"]
        table = destination["table"]
    except:
        logging.info("Please provide a valid fully qualified table name")

    logging.info(
        f"The files are going to be uploaded to the following table: {database}.{schema}.{table}"
    )
    cs = conn.cursor()
    try:
        cs.execute(f"USE DATABASE {database}")
        cs.execute(f"USE SCHEMA {schema}")
        cs.execute(
            "CREATE TABLE IF NOT EXISTS trips_staging_table(region STRING, origin_coord STRING, destination_coord STRING, datetime  DATETIME, datasource STRING)"
        )

        # Here I process the file in chunks to not overload memory
        for chunk in pd.read_csv("input/" + filename, chunksize=CHUNK_SIZE):
            chunk.columns = chunk.columns.str.upper()
            write_pandas(conn, chunk, "TRIPS_STAGING_TABLE")
        logging.info(f"File {filename} was uploaded")

        return 1

    except Exception as e:
        print(e)

        return 0
    finally:
        cs.close()
