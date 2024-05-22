import os
import csv
import json
import logging
import argparse
import requests
import pandas as pd
from datetime import datetime, timezone
from collections import defaultdict
import zipfile


# Setup logging
def setup_logger(log_level):
    logger = logging.getLogger(__name__)
    log_level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(log_level)

    handler = logging.FileHandler('data_processing.log')
    handler.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger


# Parse command-line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description="Prepare user data for analysis.")
    parser.add_argument('destination_folder', type=str, help='Path to the folder where output file will be placed')
    parser.add_argument('--filename', type=str, default='output', help='Name of the output file (default: output)')
    parser.add_argument('--gender', type=str, choices=['male', 'female'], help='Filter data by gender')
    parser.add_argument('--num_rows', type=int, help='Number of rows to filter by')
    parser.add_argument('log_level', nargs='?', default='INFO', help='Log level (default: INFO)')

    return parser.parse_args()


# Download user data from RandomUser API
def download_user_data(num_rows=5000):
    url = f'https://randomuser.me/api/?results={num_rows}&format=csv'
    response = requests.get(url)
    response.raise_for_status()

    with open('users.csv', 'w', encoding='utf-8') as file:
        file.write(response.text)
    logger.info(f"Saved downloaded data to {os.path.abspath('users.csv')}")


# Process user data
def process_data(gender=None, num_rows=None):
    df = pd.read_csv('users.csv', encoding='utf-8')
    logger.info(f"Read users.csv with {len(df)} records")

    if gender:
        df = df[df['gender'] == gender]
        logger.info(f"Filtered data by gender: {gender}, remaining records: {len(df)}")

    if num_rows:
        df = df.head(num_rows)
        logger.info(f"Filtered data to first {num_rows} rows")

    df['global_index'] = range(1, len(df) + 1)

    df['current_time'] = df.apply(lambda row: datetime.now(timezone.utc).astimezone().isoformat(), axis=1)

    title_map = {'Mrs': 'missis', 'Ms': 'miss', 'Mr': 'mister', 'Madame': 'mademoiselle'}
    df['name.title'] = df['name.title'].map(title_map).fillna(df['name.title'])

    df['dob.date'] = pd.to_datetime(df['dob.date']).dt.strftime('%m/%d/%Y')
    df['registered.date'] = pd.to_datetime(df['registered.date']).dt.strftime('%m-%d-%Y, %H:%M:%S')

    df.to_csv('processed_users.csv', index=False, encoding='utf-8')
    logger.info(f"Saved processed data to {os.path.abspath('processed_users.csv')}")



# Create directory structure and save data
def create_directory_structure(destination_folder):
    df = pd.read_csv('processed_users.csv')

    data_structure = defaultdict(lambda: defaultdict(list))

    for _, row in df.iterrows():
        dob_year = int(row['dob.date'].split('/')[2])
        decade = f"{dob_year // 10 * 10}-th"
        country = row['location.country']

        user_data = row.to_dict()
        data_structure[decade][country].append(user_data)

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    for decade, countries in data_structure.items():
        decade_path = os.path.join(destination_folder, decade)
        os.makedirs(decade_path, exist_ok=True)

        for country, users in countries.items():
            country_path = os.path.join(decade_path, country)
            os.makedirs(country_path, exist_ok=True)

            max_age = max([2024 - int(user['dob.date'].split('/')[-1]) for user in users])
            avg_registered_years = round(
                sum([2024 - int(user['registered.date'].split(',')[0].split('-')[0]) for user in users]) / len(users),
                2)
            common_id_name = max(set([user['id.name'] for user in users]),
                                 key=[user['id.name'] for user in users].count)

            file_name = f"max_age_{max_age}_avg_registered_{avg_registered_years}_popular_id_{common_id_name}.csv"
            file_path = os.path.join(country_path, file_name)

            pd.DataFrame(users).to_csv(file_path, index=False)
            logger.info(f"Created file: {file_path}")


# Log folder structure
def log_folder_structure(destination_folder):
    for root, dirs, files in os.walk(destination_folder):
        level = root.replace(destination_folder, '').count(os.sep)
        indent = ' ' * 4 * (level)
        logger.info('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            logger.info('{}{}'.format(subindent, f))


# Archive destination folder
def archive_folder(destination_folder):
    zip_filename = f"{destination_folder}.zip"
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for root, _, files in os.walk(destination_folder):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.relpath(file_path, destination_folder))
    logger.info(f"Archived folder: {zip_filename}")


if __name__ == "__main__":
    args = parse_arguments()
    logger = setup_logger(args.log_level)

    try:
        logger.info("Starting data preparation script.")
        download_user_data()
        logger.info("Downloaded user data.")

        process_data(args.gender, args.num_rows)
        logger.info("Processed user data.")

        create_directory_structure(args.destination_folder)
        logger.info("Created directory structure and saved data.")

        log_folder_structure(args.destination_folder)
        logger.info("Logged folder structure.")

        archive_folder(args.destination_folder)
        logger.info("Archived destination folder.")

        logger.info("Data preparation script completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
