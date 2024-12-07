from collections import defaultdict
from multiprocessing import Process, Manager, Pool
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
        self.query_index = defaultdict(dict)

    # @staticmethod
    # def _process_files(file_list, query_tokens):
    #     """
    #     A helper function that is called for each process 
    #     that are being executed in parallel using the 
    #     multiprocessing library. 

    #     It opens up Output_Batch text files to accumulate 
    #     tokens that are going part of the search query. 
    #     """
    #     local_query_index = dict()

    #     # iterates only through a subset of the Output Batch files
    #     for file_path in file_list:
    #         try:
    #             with open(file_path, "r", encoding="utf-8") as current_file:
    #                 content = json.load(current_file)
    #                 # filter and accumulate tokens relevant to the query
    #                 for q_token in query_tokens:
    #                     #q_token is the query token
    #                     # accesses the current and new postings to be combined
    #                     current_posting = local_query_index.get(q_token, [])
    #                     new_posting = content.get(q_token, [])
    #                     print(f"this is the currentPosting: {current_posting}")
    #                     print(f"this is the new posting: {new_posting}")
    #                     if not new_posting:
    #                         continue # skips to the next token if current token is not present
    #                     # combined_posting = current_posting + new_posting # postings are merged into one 
    #                     local_query_index[q_token] = new_posting
    #                     # local_query_index[q_token] = sorted(combined_posting, key=lambda x: x[0])

    #         except json.JSONDecodeError as e:
    #             print(f"JSON decode error {e} in file {file_path}")
    #         except Exception as e:
    #             print(f"Unexpected error while processing {file_path}: {e}")

    #     return local_query_index


    def merge_index(self, main_directory):
        """
        Uses the multiprocessing library to create a 
        Manager object and multiple processes to make 
        searching and creating the smaller index more 
        efficient through parallelism. 
        
        Merges all of the postings of the same tokens
        located in the various partial indexes, while
        sorting them based on the docID, in ascending order. 
        """

        for file in os.listdir(main_directory):
            if file.startswith("Output") and file.endswith(".json"):
                file_path = os.path.join(main_directory, file)
                # print(file_path)
                # opening the Output_Batch text file itself
                with open(file_path, "r", encoding="utf-8") as current_file:
                    try:
                        content = json.load(current_file)
                        
                        # iterates through the query_tokens rather than iterating through the whole
                        # partial index and then check if token is in query_token --> more efficient
                        #q_token = "cristina"
                        for q_token in self.query_tokens:
                            # access the current and new postings to be combined
                            # current_posting = self.query_index.get(q_token, [])
                            new_posting = content.get(q_token, [])

                            if not new_posting: continue # skips if there is no new posting

                            # merges and sort the current posting list with the newly loaded posting
                            # print(f"This is the current posting: {current_posting}")
                            # print(f"This is the new posting: {new_posting}")
                            # combined_posting = current_posting + new_posting
                            # merged_posting = sorted(combined_posting, key=lambda x: x[0])
                            self.query_index[q_token].update(new_posting)
                        # print(self.query_index)

                    except json.JSONDecodeError as e:
                        print(f"The error {e} has occured when processing {file}")
                    except KeyError as e:
                        print(f"Token {e} missing in file {file}. Skipping.")
                    except Exception as e:
                        print(f"Unexpected error while processing {file}: {e}")

        # ## testing purposes
        # with open("smaller_index.txt", "w") as smaller_index:
        #     smaller_index.flush()
        #     smaller_index.write(f"Main tokens: {list(self.query_index.keys())}\n")
        #     smaller_index.write(f"--------------------------------------------\n")
        #     for token in self.query_index:
        #         smaller_index.write(f"Current token - {token} - has the the following entries: \n")
        #         smaller_index.write(f"\t postings: {self.query_index[token]}\n")
        #         for posting in self.query_index[token]:
        #             smaller_index.write(f"\t {posting}\n")
        #         smaller_index.write(f"--------------------------------------------\n")

    def get_query_index(self):
        """
        Returns the query index to be used 
        outside of the function or class. 
        """

        return self.query_index

if __name__ == "__main__":
    query_tokens = ["crista lopes"]
    small_index = IndexMerge(query_tokens)
    time_start= time.time() # start the timer for creating report
    small_index.merge_index("IndexContent/")
    time_end= time.time() # end the timer for creating report

    print(f"Finished smaller index creation process in: {time_end - time_start} seconds...")