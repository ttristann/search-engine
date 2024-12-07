from collections import defaultdict
import json
import os
import time

class MergeIndex:
    def __init__(self):
        self.index = defaultdict(list)

    def merge_index(self, main_directory):
        """
        Merges all the partial indexes from the folder containing all partial indexes (currently 'IndexContent/') into one index.
        """
        # Load all the partial indexes
        output_batch_files = [
            os.path.join(main_directory, file)
            for file in os.listdir(main_directory)
            if file.startswith("Output") and file.endswith(".json")
        ]

        for file_path in output_batch_files:
            with open(file_path, "r", encoding="utf-8") as current_file:
                content = json.load(current_file)
                for term, posting_list in content.items():
                    self.index[term].extend(posting_list)

        # Sort the postings for each term by docID
        self.index = self._quicksort(self.index)

        self._write_index("IndexContent/merged_index.json")

    def _write_index(self, output_file):
        """
        Write the merged index to a file.
        """
        with open(output_file, "w") as output:
            json.dump(self.index, output, indent=2)

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
