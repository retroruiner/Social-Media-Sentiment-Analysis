import json
import os
import uuid
import datetime
import glob
import logging

# Configure logging if not already configured elsewhere in your application
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


class JsonFileManager:
    def __init__(self, data_folder: str = None):
        # If no data folder is specified, set the default folder relative to the script
        if data_folder is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            self.data_folder = os.path.join(script_dir, "../..", "data")
        else:
            self.data_folder = data_folder

        # Ensure the target folder exists
        os.makedirs(self.data_folder, exist_ok=True)
        logging.info(f"Data folder set to: {self.data_folder}")

    def store_json(self, data: dict, filename: str, indent: int = 4) -> str:
        """
        Stores the given dictionary as a JSON file in the data folder.

        Args:
            data (dict): The data to store.
            filename (str): The name of the file.
            indent (int): The indentation level for the JSON file.

        Returns:
            str: The path to the saved file.
        """
        file_path = os.path.join(self.data_folder, filename)
        logging.info(f"Storing JSON data to file: {file_path}")

        try:
            with open(file_path, "w") as outfile:
                json.dump(data, outfile, indent=indent)
            logging.info("JSON data stored successfully.")
        except Exception as e:
            logging.error(f"Error storing JSON data to {file_path}: {e}")
            raise

        return file_path

    def generate_filename(self, keyword: str) -> str:
        """
        Generates a unique filename based on a keyword, current timestamp, and a unique identifier.

        Args:
            keyword (str): The keyword to include in the filename.

        Returns:
            str: A unique filename string.
        """
        sanitized_keyword = keyword.strip().replace(" ", "_").lower()
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = uuid.uuid4().hex[:8]
        filename = f"{sanitized_keyword}_{timestamp}_{unique_id}.json"
        logging.info(f"Generated filename: {filename}")
        return filename

    def get_existing_file_path(self, keyword: str) -> str:
        """
        Searches for an existing JSON file that matches the given keyword in the data folder.
        Returns the latest matching file if found.

        Args:
            keyword (str): The search keyword for matching filenames.

        Returns:
            str: The path to the latest matching file if found, else None.
        """
        sanitized_keyword = keyword.strip().replace(" ", "_").lower()
        search_pattern = os.path.join(self.data_folder, f"{sanitized_keyword}_*.json")
        matching_files = glob.glob(search_pattern)
        logging.info(f"Searching for files with pattern: {search_pattern}")

        if not matching_files:
            logging.info("No matching files found.")
            return None

        # Sort files by modified time (newest first)
        matching_files.sort(key=os.path.getmtime, reverse=True)
        latest_file = matching_files[0]
        logging.info(f"Found existing file: {latest_file}")
        return latest_file
