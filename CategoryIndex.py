import os
import json
from pathlib import Path

def create_category_index(sorted_data):
    category_dir = "IndexCategory"
    Path(category_dir).mkdir(parents=True, exist_ok=True)

    # Create a dictionary to hold data for each category
    category_data = {}

    for token, postings in sorted_data.items():
        category_name = token[0].lower()
        if category_name not in category_data:
            category_data[category_name] = {}
        # Add the token and its postings to the category
        category_data[category_name][token] = postings

    # Write each category's data to a separate JSON file
    for category_name, data in category_data.items():
        file_path = os.path.join(category_dir, f"{category_name}.json")
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)  # Write as formatted JSON
