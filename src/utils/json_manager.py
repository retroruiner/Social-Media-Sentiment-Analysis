import json
import os
import datetime
import glob
import logging
import tempfile

# Configure logging if not already configured elsewhere in your application
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


class JsonFileManager:
    def __init__(self, data_folder: str = None):
        """
        Initializes the file manager with a target folder.
        Defaults to the system's temporary directory (e.g., /tmp on Render).
        """
        self.data_folder = data_folder or tempfile.gettempdir()
        os.makedirs(self.data_folder, exist_ok=True)
        logging.info(f"Data folder set to: {self.data_folder}")

    def generate_filename(self, keyword: str) -> str:
        """
        Generates a filename based on the keyword and current date (1 file/day/keyword).

        Args:
            keyword (str): The keyword for the file.

        Returns:
            str: A consistent filename for the current day.
        """
        sanitized_keyword = keyword.strip().replace(" ", "_").lower()
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        filename = f"{sanitized_keyword}_{date_str}.json"
        logging.info(f"Generated filename: {filename}")
        return filename

    def store_json(self, data: dict, filename: str, indent: int = 4) -> str:
        """
        Saves a dictionary to a JSON file in the data folder.

        Args:
            data (dict): The data to store.
            filename (str): The target file name.
            indent (int): JSON formatting indent level.

        Returns:
            str: Path to the saved file.
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

    def get_existing_file_path(self, keyword: str) -> str:
        """
        Looks for an existing file for the current day based on the keyword.

        Args:
            keyword (str): The keyword to search for.

        Returns:
            str: File path if found, else None.
        """
        filename = self.generate_filename(keyword)
        file_path = os.path.join(self.data_folder, filename)
        if os.path.exists(file_path):
            logging.info(f"Found existing file for today: {file_path}")
            return file_path

        logging.info("No existing file found for today.")
        return None

    def cleanup_old_files(self, keep_days: int = 1):
        """
        Deletes JSON files older than the specified number of days.

        Args:
            keep_days (int): Number of recent days to retain files for.
        """
        now = datetime.datetime.now()
        pattern = os.path.join(self.data_folder, "*.json")
        old_files = 0

        for file_path in glob.glob(pattern):
            modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            age_days = (now - modified_time).days
            if age_days > keep_days:
                try:
                    os.remove(file_path)
                    old_files += 1
                    logging.info(f"Deleted old file: {file_path}")
                except Exception as e:
                    logging.warning(f"Failed to delete {file_path}: {e}")

        if old_files:
            logging.info(f"Cleanup complete. Removed {old_files} old file(s).")
        else:
            logging.info("No old files to clean up.")
