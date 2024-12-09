from collections import defaultdict
from pathlib import Path
import json, os
import time
from Scoring import Scoring

class MergeIndex:
    def __init__(self):
        self.index = defaultdict(list)
        self.score = Scoring()

    def merge_index(self, main_directory):
        """
        Merges all the partial indexes from the folder containing all partial 
        indexes (currently 'IndexContent/') into one index.
        """
        df_dict = self.calculate_df()

        category_dir = "IndexCategory"
        Path(category_dir).mkdir(parents=True, exist_ok=True)

        output_batch_files = [
            os.path.join(main_directory, file)
            for file in os.listdir(main_directory)
            if file.startswith("Output") and file.endswith(".json")
        ]

        # Cache category files in memory
        category_cache = defaultdict(dict)
        for category_file in os.listdir(category_dir):
            category_name, ext = os.path.splitext(category_file)
            if ext == ".json":
                with open(os.path.join(category_dir, category_file), "r", encoding="utf-8") as f:
                    category_cache[category_name] = json.load(f)


        for batch_file in output_batch_files:
            with open(batch_file, "r", encoding="utf-8") as current_file:
                content = json.load(current_file)
                for token, posting_list in content.items():
                    category_name = token[0].lower()

                    # Merge postings
                    existing_postings = category_cache[category_name].get(token, [])
                    merged_postings = existing_postings + posting_list

                    # Update TF-IDF scores
                    DF = df_dict.get(token, 1)  # Default DF to 1 if not found
                    N = 54460  # Replace with dynamic value if possible
                    IDF = self.score.inverse_document_frequency(N, DF)
                    for posting in merged_postings:
                        posting[2] *= IDF

                    # Keep only the top 50 postings sorted by score
                    category_cache[category_name][token] = sorted(
                        merged_postings, key=lambda x: x[2], reverse=True
                    )[:50]

        # Write updated category data back to files
        for category_name, data in category_cache.items():
            file_path = os.path.join(category_dir, f"{category_name}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)


    def calculate_df(self):
        """
        Iterates through the output batch files to count the document
        frequency for each term before evaluating the query
        """
        main_directory = "IndexContent/"
        output_batch_files = [os.path.join(main_directory, file) for 
                            file in os.listdir(main_directory) 
                            if file.startswith("Output") and file.endswith(".json")]
            
        df_dict = defaultdict(int)
        for batch_file in output_batch_files:
            with open(batch_file, "r", encoding="utf-8") as current_file:
                content = json.load(current_file)
                for token, posting_list in content.items():
                    df_dict[token] += len(posting_list)

        return df_dict


if __name__ == "__main__":
    start = time.time()

    merge_index = MergeIndex()
    merge_index.merge_index("IndexContent/")


    end = time.time()

    print(f"Finished merging indexes in {end - start} seconds...")