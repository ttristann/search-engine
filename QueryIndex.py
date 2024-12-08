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

    @staticmethod
    def _process_token_chunk(args):
        """
        Helper function that is called up to process
        a chunk of tokens from query_tokens and then
        creates a partial/smaller index that is filled with
        only entries of the chunk tokens, that is later to
        be merged with other partial indexes. 
        """

        tokens_chunk, category_folder = args
        # partial index filled with entries of the tokens_chunk
        # acquired from the different json files. 
        # This is to be returned to be merged with other indexes
        partial_index = defaultdict(list)

        for token in tokens_chunk:
            category_name = token[0].lower()
            # gets the path of the category file
            category_path = os.path.join(category_folder, f"{category_name}.json")
            try: 
                with open(category_path, 'r') as current_file:
                    content = json.load(current_file)
                    # gets the entry for that token inside the file
                    new_posting = content.get(token, [])
                    if not new_posting: # skips token if there is no entry
                        continue
                    # updates the query_index with the new posting
                    partial_index[token].extend(new_posting)

            except FileNotFoundError:
                print(f"Category file {category_path} not found.")
            except json.JSONDecodeError:
                print(f"Error decoding JSON from file {category_path}.")

        return partial_index
        

    def build_query_index(self):
        """
        Uses multiprocessing to create and use pools
        where each pool takes in a certain amount of
        tokens from the query_tokens to open up a
        certain category json file to get postings 
        of the tokens. 

        Once all of the processes have been completed 
        it merges all of the partial indexes created 
        from the processes to be assigned as the 
        main query_index filled with all of the entries
        of the query_tokens. 
        """
        num_processes = len(self.query_tokens)

        # splits the tokens into chunks for multiprocessing
        chunk_size = (len(self.query_tokens) + num_processes - 1) // num_processes
        token_chunks = [self.query_tokens[i:i + chunk_size] for i in range(0, len(self.query_tokens), chunk_size)]

        category_folder = "IndexCategory"
        
        # create and uses Pool to process the chunks at the same time
        with Pool(processes=num_processes) as pool:
            results = pool.map(self._process_token_chunk, [(chunk, category_folder) for chunk in token_chunks])

        # merge all the partial indexes together into self.query_index
        for partial_index in results:
            for token, postings in partial_index.items():
                self.query_index[token].extend(postings)

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
    query_tokens = ["tuesday", "yesterday", "today"]
    tokens = ["mach", "learn"]
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
    # small_index.get_query_index()
    print(f"Finished smaller index creation process in: {time_end - time_start} seconds...")