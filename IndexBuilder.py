import os
import json
import re
from collections import defaultdict
from bs4 import BeautifulSoup, Comment
from tokenizer import Tokenizer
from pathlib import Path
import nltk; nltk.download('popular')
import time


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
    """
    count = 0 # limiter? We get rid of this for actual implementation right?
    # the data structure to collect the content
    main_index = defaultdict(list)
    docId = 0
    doc_batch = []
    
    # iterating through the directory/folder that contains all of the JSON files
    for json_file in Path(folder_path).rglob('*.json'):
        if count == 3:
            break

        with open(json_file, 'r') as current_file:

            data = json.load(current_file)
            print(f"this is the file: {current_file}")
            # using beautiful soup to parse the content
            print(data.get("url"))
            html_content = data.get("content")

            soup_obj = BeautifulSoup(html_content, "lxml")

            for comment in soup_obj.find_all(text = lambda text: isinstance(text, Comment)):
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
            print(f"this is the main text: {main_text}")

            # calls tokenizes and normalizes the words within the main text
            current_tokenizer = Tokenizer()
            tokens_list = current_tokenizer.tokenize(main_text)
            current_tokenizer.compute_frequencies(tokens_list)
            ordered_tokens = current_tokenizer.getTokens()

            # creates the posting for the inverted index entries 
            # for the words present in the current file
            for token, frequency in ordered_tokens.items():
                current_entry = (data["url"], frequency)
                main_index[token].append(current_entry)

            count += 1

    return main_index


if __name__ == "__main__":
    

    # folder_path = "/Users/tristangalang/Desktop/ICS/CS121/A3 - Search Engine/DEV"
    folder_path = Path('analyst/ANALYST')

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
    output_file_path = "filtered_output.txt"
    
    # Write the results to the output file
    with open(output_file_path, 'w') as output_file:
        for key, value in main_index.items():
            output_file.write(f'word -> {key} - \n entries:\n\t{value}\n\n')
    
    print(f"Output written to {output_file_path}")