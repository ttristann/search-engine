from collections import defaultdict
import os, json
import heapq
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
class IndexMerge:
    def __init__(self, query_tokens:list):
        self.query_tokens = query_tokens
        self.query_index = defaultdict(list)

    def merge_index(self, main_directory):
        """
        Iterates through the Output_Batch text files
        created in the same directory to only accumulate
        tokens that are going to be used to answer the 
        search query. 
        
        Merges all of the postings of the same tokens
        located in the various partial indexes, while
        sorting them based on the docID, in descending order. 
        """
        # iterates through the project directory to find the Output_Batch text files
        for file in os.listdir(main_directory):
            if file.startswith("Output") and file.endswith(".txt"):
                file_path = os.path.join(main_directory, file)

                # opening the Output_Batch text file itself
                with open(file_path, "r", encoding="utf-8") as current_file:
                    try:
                        content = json.load(current_file)
                        
                        # iterates through the query_tokens rather than iterating through the whole
                        # partial index and then check if token is in query_token --> more efficient
                        for q_token in self.query_tokens:
                            # access the current and new postings to be combined
                            current_posting = self.query_index[q_token]
                            new_posting = content[q_token]

                            # merges and sort the current posting list with the newly loaded posting
                            merged_posting = list(heapq.merge(current_posting, new_posting, key=lambda x: x[0]))
                            self.query_index[q_token] = merged_posting

                    except json.JSONDecodeError as e:
                        print(f"The error {e} has occured when processing {file}")

        ### testing purposes
        with open("smaller_index.txt", "w") as smaller_index:
            smaller_index.flush()
            smaller_index.write(f"Main tokens: {list(self.query_index.keys())}\n")
            smaller_index.write(f"--------------------------------------------\n")
            for token in self.query_index:
                smaller_index.write(f"Current token - {token} - has the the following entries: \n")
                smaller_index.write(f"\t postings: {self.query_index[token]}")
                for posting in self.query_index[token]:
                    smaller_index.write(f"\t {posting}\n")
                smaller_index.write(f"--------------------------------------------\n")

if __name__ == "__main__":
    query_tokens = ["mach", "learn"]
    small_index = IndexMerge(query_tokens)
    time_start= time.time() # start the timer for creating report
    small_index.merge_index(".")
    time_end= time.time() # end the timer for creating report

    print(f"Finished smaller index creation process in: {time_end - time_start} seconds...")


                            


    

    