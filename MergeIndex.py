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
        # Load all the partial indexes
        output_batch_files = [
            os.path.join(main_directory, file)
            for file in os.listdir(main_directory)
            if file.startswith("Output") and file.endswith(".json")
        ]
        # Updates the in-memory index entries with corresponding postings
        for file_path in output_batch_files:
            with open(file_path, "r", encoding="utf-8") as current_file:
                content = json.load(current_file)
                for term, posting_list in content.items():
                    self.index[term].extend(posting_list)

        # Sort the postings for each term by docID
        self.index = self._quicksort(self.index)
        self.getScoreData()
        # creates all of the json category files
        self._create_category_index(self.index)

    def getScoreData(self):
        """  docID --> {docID: url}
            termData --> {term : [docID, termFrequency, weight]}
            We want to find the tf-idf scores for these terms.
        """
        N = len(self.index) # O(1), this is big data number of pairs


        for key in self.index: #complete data
            DF = len(self.index[key]) #len of that query postings, better place to place?????
            IDF = self.score.inverse_document_frequency(N, DF)

            for posting in self.index[key]:
                # print(f"this is the data: {self.index[key]}")
                # # tf-IDF -->  1 + log(TF) * log(N / DF)
                #documentData is each posting list --> [docID, TF, TFSCORE]
                posting[2] = (posting[2] * IDF) # index 2 is the 1 + log(tf) value, * IDF which returns the complete tf-idf score

            sorted_list = sorted(self.index[key], key=lambda x: x[2], reverse=True)[:50]

            self.index[key] = sorted_list

    def _create_category_index(self, sorted_data):
        """
        Creates a directory to hold all the json files
        that are going to be created for every category
        based on the first letter of the token. 

        Each category json file holds entries to make
        group similar tokens in the same file allowing
        for faster look ups. 
        """
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

    def _quicksort(self, index):
        """
        Sorts a dictionary by its keys using the Quicksort algorithm.
        
        :param d: Dictionary to sort.
        :return: New dictionary with sorted keys.
        """
        if len(index) <= 1:
            return index  # Base case: no sorting needed for 0 or 1 item
        
        keys = list(index.keys())
        pivot = keys[0]  # Choose the first key as the pivot
        less = {k: index[k] for k in keys if k < pivot}
        equal = {k: index[k] for k in keys if k == pivot}
        greater = {k: index[k] for k in keys if k > pivot}
        
        # Recursively sort the sub-dictionaries and combine
        sorted_dict = {
            **self._quicksort(less),
            pivot: equal[pivot],
            **self._quicksort(greater),
        }
        return sorted_dict


if __name__ == "__main__":
    start = time.time()

    merge_index = MergeIndex()
    merge_index.merge_index("IndexContent/")


    end = time.time()

    print(f"Finished merging indexes in {end - start} seconds...")
