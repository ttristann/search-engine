from collections import defaultdict
from multiprocessing import Process, Manager, Pool
import os, json
import time


"""
This class that analyzes through the newly created
partial index inside the Output_Batch text files 
inside the same directory to create a smaller index
that may contain tokens that is scattered throughout
the Output_Batch text files. 

The smaller index only contains the tokens that are
parsed only from the search query to minimize the 
consumption of memory and allow for faster search. 

The smaller index will have the format:
    smaller_index = {
        'word1': [(docId : int, freq1 : int), (docId, freq2)],
        'word2': [(docId, freq3), (docId, freq4)],
        ...
    }
"""
class QueryIndex:
    def __init__(self, query_tokens:list):
        self.query_tokens = query_tokens
        self.query_index = defaultdict(list)

    def _process_file(self, file_path):
        """
        A helper function that is called for each process 
        that are being executed in parallel using the 
        multiprocessing library. 

        It opens up Output_Batch text files to accumulate 
        tokens that are going part of the search query. 
        """
        """
        Processes a single file, extracting terms that match query_tokens
        and creating a smaller index for this file.
        """
        smaller_index = defaultdict(list)
        try:
            with open(file_path, "r", encoding="utf-8") as current_file:
                content = json.load(current_file)
                # print(f'PROCESSING FILE: {file_path} ----------------------------------\n')
                # Extract only the query tokens
                for q_token in self.query_tokens:
                    new_posting = content.get(q_token, [])
                    if not new_posting: 
                        continue # skips if there is no new posting
                    # merges and sort the current posting list with the newly loaded posting
                    smaller_index[q_token].extend(new_posting)

        except json.JSONDecodeError as e:
            print(f"The error {e} has occurred when processing {file_path}")
        except Exception as e:
            print(f"Unexpected error while processing {file_path}: {e}")
        return smaller_index
    
    def _merge_smaller_indexes(self, smaller_indexes):
        """
        Merges all smaller indexes into the main query index.
        """
        for small_index in smaller_indexes:
            for token, postings in small_index.items():
                self.query_index[token].extend(postings)
        
        # Sort postings for each token by docID
        for token in self.query_index:
            self.query_index[token] = sorted(self.query_index[token], key=lambda x: x[0])

        # print(self.query_index)


    def build_query_index(self):
        """
        Uses the multiprocessing library to create a 
        Manager object and multiple processes to make 
        searching and creating the smaller index more 
        efficient through parallelism. 
        
        Merges all of the postings of the same tokens
        located in the various partial indexes, while
        sorting them based on the docID, in ascending order. 
        
        TODO: need to incorporate a director to distribute
        the pools of workers and assign each one to one 
        specific file or output batch file. 
        """

        # files_to_process = [
        #     os.path.join(main_directory, file)
        #     for file in os.listdir(main_directory)
        #     if file.startswith("Output") and file.endswith(".json")
        # ]
        
        # with Pool() as pool:
        #     smaller_indexes = pool.map(self._process_file, files_to_process)

        # # print(f'SMALLER INDEX:\n')
        # # for index in smaller_indexes:
        # #     print(f'\tindex')
        
        # self._merge_smaller_indexes(smaller_indexes)

        ####################################
        category_folder = "IndexCategory"
        for token in self.query_tokens:
            category_name = token[0].lower()
            category_path = os.path.join(category_folder, f"{category_name}.json")
            # category_path = f"{category_folder}/{category_name}.json"
            with open(category_path, 'r') as current_file:
                content = json.load(current_file)
                new_posting = content.get(token, [])
                if not new_posting:
                    continue
                self.query_index[token].extend(new_posting)


    def get_query_index(self):
        """
        Returns the query index to be used 
        outside of the function or class. 
        """
        for token, postings in self.query_index.items():
            count = 0
            print(f'current token: {token}')
            for posting in postings:
                print(f"\t{posting}")
                count += 1
                if count == 10: break

if __name__ == "__main__":
    query_tokens = ["crista", "lope"]
    words = [
    'Apple',      # A
    'Ball',       # B
    'Cat',        # C
    'Dog',        # D
    'Elephant',   # E
    'Fish',       # F
    'Guitar',     # G
    'House',      # H
    'Igloo',      # I
    'Jungle',     # J
    'Kangaroo',   # K
    'Lion',       # L
    'Monkey',     # M
    'Nest',       # N
    'Orange',     # O
    'Penguin',    # P
    'Queen',      # Q
    'Rabbit',     # R
    'Snake',      # S
    'Tiger',      # T
    'Umbrella',   # U
    'Violin',     # V
    'Whale',      # W
    'Xylophone',  # X
    'Yacht',      # Y
    'Zebra'       # Z
]
    lst = [word.lower() for word in words]
    small_index = QueryIndex(lst)
    time_start= time.time() # start the timer for creating report
    small_index.build_query_index()
    time_end= time.time() # end the timer for creating report
    # small_index.get_query_index()
    print(f"Finished smaller index creation process in: {time_end - time_start} seconds...")