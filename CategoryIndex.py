from collections import defaultdict
import json
import os
import time
from pathlib import Path

def create_category_index(sorted_data):
    category_dir = "IndexCategory"
    Path(category_dir).mkdir(parents=True, exist_ok=True)

    category_files = dict()
    for token, postings in sorted_data.items():
        category_name = token[0].lower()
        if category_name in category_files:
            category_files[category_name].write(f"{token} : {postings}\n")
        else:
            category_files[category_name] = open(os.path.join(category_dir, f'{category_name}.json'), 'w')


