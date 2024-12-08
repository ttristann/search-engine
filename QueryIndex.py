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

    def build_query_index(self):
        """
        Iterates through the query_tokens where
        each token is analyzed to get its category
        based on its first letter and open up the 
        corresponding category json file (eg. a.json). 

        Once the json file is opened and loaded, the
        token's entry inside the file is looked up 
        and acquired to update the query_index attribute
        entry for that token. 
        """
        category_folder = "IndexCategory"
        # iterates through query_tokens
        for token in self.query_tokens:
            # identifies the category file to opem
            category_name = token[0].lower()
            # gets the path of the category file
            category_path = os.path.join(category_folder, f"{category_name}.json")
            with open(category_path, 'r') as current_file:
                content = json.load(current_file)
                # gets the entry for that token inside the file
                new_posting = content.get(token, [])
                if not new_posting: # skips token if there is no entry
                    continue
                # updates the query_index with the new posting
                self.query_index[token].extend(new_posting)


    def get_query_index(self):
        """
        Returns the query index to be used 
        outside of the function or class. 
        
        THIS IS FOR TESTING PURPOSES
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
    small_index = QueryIndex(query_tokens)
    time_start= time.time() # start the timer for creating report
    small_index.build_query_index()
    time_end= time.time() # end the timer for creating report
    small_index.get_query_index()
    print(f"Finished smaller index creation process in: {time_end - time_start} seconds...")