import os
import json
import re
import time
import warnings # having an XMLParseAsHTMLWarning, using to catch it and identify XML file(s)
import nltk
import threading
import queue
import math
import multiprocessing

from collections import defaultdict
from bs4 import BeautifulSoup, Comment, XMLParsedAsHTMLWarning
from tokenizer import Tokenizer
from pathlib import Path
from nltk.stem import SnowballStemmer
# nltk.download('popular') # Use this to download all popular datasets for nltk, pls run once then you can comment it out


def writer_thread_worker(writer_thread_queue):
    while True:
        thread = writer_thread_queue.get()
        if thread is None: break
        write_to_disk(*thread) # unpack the arguments (*) and call the write_to_disk function
        writer_thread_queue.task_done()


def write_to_disk(main_index, output_file_path):
    """
    Writes the main index to the output file
    """
    with open(output_file_path, 'w') as output_file:
        json.dump(main_index, output_file, indent=2) # main index data is written to the output file in JSON format, easier for parsing later
        # NOTE: will need to use json.load() to read the data back in later 
    print("\n-----------------------------------------------------")
    print(f"Output successfully written to {output_file_path}")
    print("-----------------------------------------------------")


def sort_index(main_index):
    """
    Sorts the main index by 
        - frequency of the words (primary)
        - the docID (secondary)
    """
    # NOTE: x[1][1] is word frequency, x[1][0] is docID, '-' implies DESC order (highest first)
    sorted_index = {word: sorted(entries, key=lambda x: (-x[1], -x[0])) 
                    for word, entries in main_index.items()}
    return sorted_index

def process_file(main_text, docId):
    temp_index = defaultdict(list) # temporary index to store the current batch's partial index
    # calls tokenizes and normalizes the words within the main text
    current_tokenizer = Tokenizer()
    tokens_list = current_tokenizer.tokenize(main_text)
    current_tokenizer.compute_frequencies(tokens_list)
    ordered_tokens = current_tokenizer.getTokens()

    # creates the posting for the inverted index entries 
    # for the words present in the current file
    for token, frequency in ordered_tokens.items():
        stemmed_token = SnowballStemmer("english").stem(token) # stemming the token
        current_entry = (docId, frequency) # TENZIN NOTE: might not need to add frequency but let's keep for now
        temp_index[stemmed_token].append(current_entry)
    
    return temp_index
    

"""
This is file is just to experiment on how to access directories inside the DEV directory
and iterate through all the folders and json files
"""
def build_index(folder_path):
    """
    takes in a folder path to access the folder that
    contains many folders that are made of json files
    inside. 

    Curently changing content format to:
        inverted_index = {
            'word1': [(docId : int, freq1 : int), (docId, freq2)],
            'word2': [(docId, freq3), (docId, freq4)],
            ...
        }
    """
    processes = [] # list of all started processes

    main_index = defaultdict(list) # Our main inverted index
    docId = 1 # unique identifier for each document, incremented by 1 for each file
    batchSize = 10000 # number of files to process before writing to disk, could make bigger to reduce I/O overhead?? But we gotta consider memory usage (too big = bad, computer could go into coma)
    skip = False # flag to skip the current file if it has an XMLParsedAsHTMLWarning
    batchCount = 0 # counter to keep track of the batch number
    writer_thread_queue = queue.Queue() # Queue to store all started threads

    # Creating a single writer thread to write to disk
    writer_thread = threading.Thread(target=writer_thread_worker, args=(writer_thread_queue,), daemon=True)
    writer_thread.start()
    ### MULTIPROCESSING IMPLEMENTATION ???? ###
    # 1. Create a multiprocessing pool to manage the processes (instead of manually handling them)
    # 2. Initialize task list, should contain all the files to process, resets every batchSize
    # 3. Create a process for each file in the task list
    # 4. Once batchSize reached --> retrieve all current tasks -->
    #        update main_index with results --> sort and write to batch file
    # 5. Repeat until all files are processed
    # 6. Catch stragglers, AKA remaining files that didn't make it to the last batch
    # 7. Join thread for writer, ensures all files are actually written to disk
    ####################################################################

    with multiprocessing.Pool() as pool:
        tasks_per_batch = []
        # iterating through the directory/folder that contains all of the JSON files
        for json_file in Path(folder_path).rglob('*.json'): 
            with open(json_file, 'r') as current_file:
                data = json.load(current_file) # loads the json file

                # using beautiful soup to parse the content
                html_content = data.get("content")
                with warnings.catch_warnings(record=True) as w: # catch the warning
                    warnings.simplefilter("always", XMLParsedAsHTMLWarning)
                    soup_obj = BeautifulSoup(html_content, "lxml")

                    if any(issubclass(warn.category, XMLParsedAsHTMLWarning) for warn in w):
                        skip = True
                if skip:
                    # print(f"\t Skipping {data.get("url")}")
                    skip = False
                    continue

                for comment in soup_obj.find_all(string = lambda string: isinstance(string, Comment)):
                    comment.extract()

                # removes all <script> and <style> tags
                for tag_element in soup_obj.find_all(['script', 'style']):  
                    tag_element.extract()
                if(soup_obj.find('title')):
                    soup_obj.find('title').decompose() #remove title header, makes word count more accurate
                # remove title from text data, analyze code. Many words are being mashed together. Axel
                
                # gets the actual text inside the HTML file
                raw_text = soup_obj.get_text(separator=" ", strip=True)
                main_text = " ".join(re.findall(r'[a-zA-Z0-9]+', raw_text))
                # print(f"this is the main text: {main_text}")

                # calls the process_file function, tokenizing the file's text and adding it to the main index
                task = pool.apply_async(process_file, args=(main_text, docId))
                tasks_per_batch.append(task) # add task to the current batch

                docId += 1

                # Check if batch limit has been reached, T -> etner the if block, F -> continue to next file
                if len(tasks_per_batch) % batchSize == 0:
                    # retrieving the current batch of processed files
                    for task in tasks_per_batch:
                        partial_index = task.get() # get the partial index from the task
                        for word, postings in partial_index.items():
                            main_index[word] += postings
                    
                    batchCount += 1 # increment the batch count

                    # Sort and Write the current batch to disk   
                    main_index = sort_index(main_index)
                    writer_thread_queue.put((main_index, f"Output_Batch_{batchCount}.txt"))
                    
                    main_index = defaultdict(list) # reset the main index
                    tasks_per_batch = [] # reset the tasks list

        # Process the remaining files if any
        for task in tasks_per_batch:
            partial_index = task.get()
            for word, postings in partial_index.items():
                main_index[word].extend(postings)
        
        if main_index:
            batchCount += 1
            # Sort and Write remaining files to disk if any (Catch the stragglers)
            main_index = sort_index(main_index)
            writer_thread_queue.put((main_index, f"Output_Batch_{batchCount}.txt"))
            # write_to_disk(main_index, f"Output_Batch_{batchCount}.txt")
    
    writer_thread_queue.join()
    writer_thread_queue.put(None)
    writer_thread.join()

    return main_index


if __name__ == "__main__":
    # folder_path = Path('analyst/ANALYST')
    folder_path = Path('developer/DEV')
    total_files = 0 # total number of files in the directory

    time_start = time.time() # start the timer
    
    main_index = build_index(folder_path) # Get the content from the folder
    time_end = time.time() # end the timer

    print(f"Finished process in: {time_end - time_start} seconds...")
    
    # Specify the output file path
    # output_file_path = "filtered_output.txt"
    
    # Write the results to the output file
    # with open(output_file_path, 'w') as output_file:
    #     for key, value in main_index.items():
    #         output_file.write(f'word -> {key} - \n entries:\n\t{value}\n\n')
    
    # print(f"Output written to {output_file_path}")