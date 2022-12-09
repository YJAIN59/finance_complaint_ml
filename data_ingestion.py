from spark_manager import spark_session
import os
import requests
from collections import namedtuple
import uuid
import json
from datetime import datetime
from datetime import timedelta
from config import json_data_dir, parquet_data_dir
from logger import logger

DownloadUrl = namedtuple("DownloadUrl", ["url", "file_path", "n_retry"])


def download_prep():
    # yesterday
    to_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    # Parso - day-before-yestrday
    from_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    # filename
    file_name = f"{from_date}_{to_date}.json"

    url = f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/" \
          f"?date_received_max=<todate>&date_received_min=<fromdate>" \
          f"&field=all&format=json"
    url = url.replace("<todate>", to_date).replace("<fromdate>", from_date)

    download_url = DownloadUrl(url=url, file_path=os.path.join(json_data_dir,file_name), n_retry=5)
    logger.info(f"Download url is {download_url}")
    return download_url


def download_files(download_url):
    try:
        data = requests.get(download_url.url, params={'User-agent': f'your bot {uuid.uuid4()}'})
        logger.info(f"Started writing downloaded data into json file: {download_url.file_path}")
        # saving downloaded data into hard disk
        with open(download_url.file_path, "w") as file_obj:
            finance_complaint_data = list(map(lambda x: x["_source"],
                                                filter(lambda x: "_source" in x.keys(),
                                                        json.loads(data.content)))
                                            )

            json.dump(finance_complaint_data, file_obj)
        logger.info(f"Downloaded data has been written into file: {download_url.file_path}")
    except Exception as e:
        logger.info("Failed to download hence retry again.")


def convert_files_to_parquet():
    """
    downloaded files will be converted and merged into single parquet file
    json_data_dir: downloaded json file directory
    data_dir: converted and combined file will be generated in data_dir
    output_file_name: output file name
    =======================================================================================
    returns output_file_path
    """
    try:
        output_file_name = "finance_complaint"
        parquet_file_path = os.path.join(parquet_data_dir, f"{output_file_name}")
        # logger.info(f"Parquet file will be created at: {download_url.file_path}")
        if not os.path.exists(json_data_dir):
            return parquet_file_path
        for file_name in os.listdir(json_data_dir):
            print(file_name)
            json_file_path = os.path.join(json_data_dir, file_name)
            # logger.debug(f"Converting {json_file_path} into parquet format at {parquet_file_path}")
            df = spark_session.read.json(json_file_path)
            if df.count() > 0:
                df.write.mode('append').parquet(parquet_file_path)
        print("success")
        return parquet_file_path
    except Exception as e:
        print(e.with_traceback)

# parquet_file_path = convert_files_to_parquet()