import os
import json
import re
import time
import warnings # having an XMLParseAsHTMLWarning, using to catch it and identify XML file(s)
import nltk
import threading

from collections import defaultdict
from bs4 import BeautifulSoup, Comment, XMLParsedAsHTMLWarning
from tokenizer import Tokenizer
from pathlib import Path
from nltk.stem import SnowballStemmer
# nltk.download('popular') # Use this to download all popular datasets for nltk, pls run once then you can comment it out


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
"""
This is file is just to experiment on how to access directories inside the DEV directory
and iterate through all the folders and json files
"""
def build_index(folder_path):
    """
    takes in a folder path to access the folder that
    contains many folders that are made of json files
    inside. 

    It initializes a dictionary to store the url
    and its content accordinly and has the following format: 
        inverted_index = {
            'word1': [('url1', freq1), ('url2', freq2)],
            'word2': [('url1', freq3), ('url3', freq4)],
            ...
        }

    Curently changing content format to:
        inverted_index = {
            'word1': [(docId : int, freq1 : int), (docId, freq2)],
            'word2': [(docId, freq3), (docId, freq4)],
            ...
        }
    """
    threads = [] # list of all started threads
    main_index = defaultdict(list) # Our main inverted index
    docId = 1 # unique identifier for each document, incremented by 1 for each file
    batchSize = 1000 # number of files to process before writing to disk, could make bigger to reduce I/O overhead?? But we gotta consider memory usage (too big = bad, computer could go into coma)
    batchCount = 0 # current batch count
    skip = False # flag to skip the current file if it has an XMLParsedAsHTMLWarning

    # iterating through the directory/folder that contains all of the JSON files
    for json_file in Path(folder_path).rglob('*.json'): 
        with open(json_file, 'r') as current_file:
            data = json.load(current_file) # loads the json file
            if skip:
                # Double checking to make sure we skipped the xml file from previous iteration
                skip = False
                print(f"SKIPPED previous, Now parsing: {data.get("url")} IN {json_file}")

            # print(f"this is the file: {current_file}")
            # print(data.get("url"))

            # using beautiful soup to parse the content
            html_content = data.get("content")
            with warnings.catch_warnings(record=True) as w: # catch the warning
                warnings.simplefilter("always", XMLParsedAsHTMLWarning)
                soup_obj = BeautifulSoup(html_content, "lxml")

                if any(issubclass(warn.category, XMLParsedAsHTMLWarning) for warn in w):
                    print(f"\nXMLParsedAsHTMLWarning \n\t FOR: {data.get("url")}")
                    print(f"\t IN {json_file}")
                    skip = True
            if skip:
                print(f"\t Skipping {data.get("url")}")
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
            main_text = re.sub(r"[^A-Za-z0-9\s]+", "", raw_text)
            # print(f"this is the main text: {main_text}")

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
                main_index[stemmed_token].append(current_entry)

            batchCount += 1
            docId += 1
        
        # Check if batch limit has been reached, T -> etner the if block, F -> continue to next file
        if batchCount % batchSize == 0:
            # Sort and Write the current batch to disk
            main_index = sort_index(main_index)
            writer_thread = threading.Thread(target=write_to_disk, args=(main_index, f"Output_Batch_{batchCount}.txt"))
            # write_to_disk(main_index, f"Output_Batch_{batchCount}.txt") # e.g Output_Batch_100.txt = 1st batch, Output_Batch_200.txt = 2nd batch, etc.
            writer_thread.start()
            threads.append(writer_thread)
            main_index = defaultdict(list) # reset the main index



    if main_index:
        # Sort and Write remaining files to disk if any (Catch the stragglers)
        main_index = sort_index(main_index)
        writer_thread = threading.Thread(target=write_to_disk, args=(main_index, f"Output_batch_{batchCount}.txt"))
        writer_thread.start()
        threads.append(writer_thread)
        # write_to_disk(main_index, f"Output_Batch_{batchCount}.txt")
        main_index = defaultdict(list) # reset the main index just cuz
    
    for thread in threads:
        print(f"Thread: {thread.name} is alive: {thread.is_alive()}, JOINING")
        thread.join()

    return main_index


if __name__ == "__main__":
    # folder_path = "/Users/tristangalang/Desktop/ICS/CS121/A3 - Search Engine/DEV"
    folder_path = Path('DEV')

    # if os.path.exists(folder_path):
    #     print("exists")
    # else:
    #     print("does not exist")

    time_start = time.time()
    # Get the content from the folder
    main_index = build_index(folder_path)
    time_end = time.time()

    print(f"Finished process in: {time_end - time_start} seconds...")
    
    # Specify the output file path
    # output_file_path = "filtered_output.txt"
    
    # Write the results to the output file
    # with open(output_file_path, 'w') as output_file:
    #     for key, value in main_index.items():
    #         output_file.write(f'word -> {key} - \n entries:\n\t{value}\n\n')
    
    # print(f"Output written to {output_file_path}")