import json
import os
import uuid
import datetime


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

        with open(file_path, "w") as outfile:
            json.dump(data, outfile, indent=indent)

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
        return f"{sanitized_keyword}_{timestamp}_{unique_id}.json"
