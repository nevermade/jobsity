import logging
import sys
from jobsitypkg.data_ingestion import load_csv_to_snowflake_table
from jobsitypkg.data_ingestion import send_email
from jobsitypkg.polygon_processing import proccesing_points
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s]{%(filename)s:%(lineno)-8d}%(levelname)-5s- %(message)s",
    datefmt="%D %H:%M:%S",
)


CONFIG_PARAMS = {}
FILE_NAME = ""


def initialize():
    with open("input/config.json") as json_data_file:
        global CONFIG_PARAMS
        CONFIG_PARAMS = json.load(json_data_file)

    try:
        global FILE_NAME
        FILE_NAME = sys.argv[1]
    except:
        logging.info("")


if __name__ == "__main__":
    initialize()

    result = load_csv_to_snowflake_table(
        CONFIG_PARAMS["credentials"], CONFIG_PARAMS["destination"], FILE_NAME
    )

    send_email(CONFIG_PARAMS["email_config"], FILE_NAME, result)

    if result==1:
        proccesing_points(
            CONFIG_PARAMS["credentials"], CONFIG_PARAMS["destination"], CONFIG_PARAMS["polygon"]
        )
