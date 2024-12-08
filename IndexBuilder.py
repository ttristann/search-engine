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
import glob

from bs4 import BeautifulSoup, Comment, XMLParsedAsHTMLWarning, MarkupResemblesLocatorWarning
from collections import defaultdict
from bs4 import BeautifulSoup, Comment, XMLParsedAsHTMLWarning, MarkupResemblesLocatorWarning
from tokenizer import Tokenizer
from pathlib import Path
from nltk.stem import PorterStemmer
from ReportCreation import report_creation
from MergeIndex import MergeIndex
from Scoring import Scoring

# nltk.download('popular') # Use this to download all popular datasets for nltk, pls run once then you can comment it out



class IndexBuilder:
    def __init__(self, filePath, batchSize=10000):
        self.docId_to_url = dict() # dictionary to store the docId to URL mapping
        self.filePath = filePath # path to the folder containing all the JSON files
        self.batchSize = batchSize # number of files to process before writing to disk, defaulted to 10,000 files per batch
        self.scoring = Scoring()
    
    def _writer_thread_worker(self, writer_thread_queue):
        """
        Given a queue of writer threads, this function will write the partial index to disk by calling the 'write_to_disk' function
        Every thread manages a separate batch of files to write to disk
        """
        while True:
            thread = writer_thread_queue.get()
            if thread is None: break
            self._write_to_disk(*thread) # unpack the arguments (*) and call the write_to_disk function
            writer_thread_queue.task_done()


    def _write_to_disk(self, partial_index, output_file_path):
        """
        Writes the main index to the output file
        """
        with open(output_file_path, 'w') as output_file:
            json.dump(partial_index, output_file, indent=2) # main index data is written to the output file in JSON format, easier for parsing later
            # NOTE: will need to use json.load() to read the data back in later 
        print("\n-----------------------------------------------------")
        print(f"Output successfully written to {output_file_path}")
        print("-----------------------------------------------------")


    def _sort_index(self, main_index):
        """
        Sorts the main index by 
            - frequency of the words (primary)
            - the docID (secondary)
        """
        # NOTE: x[1][1] is word frequency, x[1][0] is docID, '-' implies DESC order (highest first)
        sorted_index = {word: sorted(entries, key=lambda x: (-x[1], -x[0])) 
                        for word, entries in main_index.items()}
        return sorted_index

    def _process_file(self, main_text, important_words, docId, url):
        """
        Tokenizes a document's text and creates a partial index for the document.
        Additionally, creates a mapping of the given docId to it's URL for the document.
        """
        temp_index = defaultdict(list) # temporary index to store the current batch's partial index
        # calls tokenizes and normalizes the words within the main text
        current_tokenizer = Tokenizer()
        tokens_list = current_tokenizer.tokenize(main_text)
        current_tokenizer.compute_frequencies(tokens_list)
        ordered_tokens = current_tokenizer.getTokens()

        # updates the docID_dict to add the entry docId: url
        temp_docId_to_url = dict()
        temp_docId_to_url[docId] = url

        # creates the posting for the inverted index entries 
        # for the words present in the current file
        for token, frequency in ordered_tokens.items():
            stemmed_token = PorterStemmer().stem(token) # stemming the token

            # retrieves the weight of the token if it's an 
            # important word, otherwise defaults to 0
            weight = important_words.get(token, 0)
            tfScore = (self.scoring.term_frequency(frequency) + weight) #Axel: add weight after tf
            # Note: for files withmultiple types of importance (title, header, etc.), 
            # the highest weight will take precedence
            current_entry = (docId, frequency, tfScore)  # 3rd element is now tfScore
            temp_index[stemmed_token].append(current_entry)
        
        return temp_index, temp_docId_to_url

    def retrieve_important_words(self, soub_obj):
        """
        Retrieves the important words from the HTML content and assigns a given weight to them.

        Important Words Include...
            - Title | Weight: 1
            - Headers (h1,h2,h3) | Weight: 0.75
            - Bolded Text (<stong>) | Weight: 0.50
        """
        important_words = dict()

        # retrieves the title of the HTML content
        title = soub_obj.find('title')
        if title:
            title_text = title.get_text()
            important_words[title_text] = 1
        
        # retrieves the headers of the HTML content
        headers = soub_obj.find_all(['h1', 'h2', 'h3'])
        for header in headers:
            header_text = header.get_text()
            important_words[header_text] = 0.75
        
        # retrieves the bolded text of the HTML content
        bolded_content = soub_obj.find_all('strong')
        for bold_text in bolded_content:
            bold_text = bold_text.get_text()
            important_words[bold_text] = 0.50
        
        return important_words
    
    def should_skip_file(self, html_content):
         """
         Checks if the current file should be skipped

         Skip Criteria:
            - Any kind of warning raised during the parsing of the HTML content (XMLParsedAsHTMLWarning, MarkupResemblesLocatorWarning)
        
        Returns:
            - True if the file should be skipped, False otherwise
            - The parsed BeautifulSoup object
         """
         with warnings.catch_warnings(record=True) as w: # catch the warning
            # Filter out the warnings that are not XMLParsedAsHTMLWarning or MarkupResemblesLocatorWarning
            warnings.simplefilter("always", XMLParsedAsHTMLWarning)
            warnings.simplefilter("always", MarkupResemblesLocatorWarning)

            soup_obj = BeautifulSoup(html_content, "lxml")

            return any(issubclass(warn.category, (XMLParsedAsHTMLWarning, MarkupResemblesLocatorWarning)) for warn in w), soup_obj
    
    def build_index(self):
        """
        takes in a folder path to access the folder that
        contains many folders that are made of json files
        inside. 

        Curently changing content format to:
            inverted_index = {
                'word1': [(docId : int, freq1 : int, tf-idf : float), (docId, freq2, tf-idf_2)],
                'word2': [(docId, freq3, tf-idf_3), (docId, freq4, tf-idf_4)],
                ...
            }
        """
        # Create IndexContent folder if it doesn't exist
        Path("IndexContent").mkdir(parents=True, exist_ok=True)

        main_index = defaultdict(list) # Our main inverted index
        docId_to_url_builder = dict() # dictionary to store the docId to URL mapping
        writer_thread_queue = queue.Queue() # Queue to store all started threads

        docId = 1 # unique identifier for each document, incremented by 1 for each file
        batchCount = 0 # counter to keep track of the batch number

        # Creating a single writer thread to write to disk
        writer_thread = threading.Thread(target=self._writer_thread_worker, args=(writer_thread_queue,), daemon=True)
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
            for json_file in Path(self.filePath).rglob('*.json'): 
                with open(json_file, 'r') as current_file:
                    data = json.load(current_file) # loads the json file

                    # using beautiful soup to parse the content
                    html_content = data.get("content")
                    
                    # Parses the file and checks if there are any XML parsing or Markup issue - Could later update to include simhashing (?)
                    skip, soup_obj = self.should_skip_file(html_content)
                    
                    if skip:
                        continue

                    # Retrieves the important words from the HTML content
                    important_words = self.retrieve_important_words(soup_obj)

                    # removes all comments from the HTML file
                    for comment in soup_obj.find_all(string = lambda string: isinstance(string, Comment)):
                        comment.extract()

                    # removes all <script> and <style> tags
                    for tag_element in soup_obj.find_all(['script', 'style']):  
                        tag_element.extract()
                    
                    # gets the actual text inside the HTML file
                    raw_text = soup_obj.get_text(separator=" ", strip=True)
                    main_text = " ".join(re.findall(r'[a-zA-Z0-9]+', raw_text))

                    # calls the process_file function, tokenizing the file's text and adding it to the main index
                    task = pool.apply_async(self._process_file, args=(main_text, important_words, docId, data.get("url")))
                    tasks_per_batch.append(task) # add task to the current batch
                    docId += 1
                    #

                    # Check if batch limit has been reached, T -> etner the if block, F -> continue to next file
                    if len(tasks_per_batch) % self.batchSize == 0:
                        # retrieving the current batch of processed files
                        for task in tasks_per_batch:
                            partial_index, task_docId_mapping = task.get() # get the partial index from the task
                            for word, postings in partial_index.items():
                                main_index[word].extend(postings) # add the postings to the main index

                            docId_to_url_builder.update(task_docId_mapping) # update the docId_to_url dictionary with the current task's docId to URL mapping (should be one mapping per task)
                        batchCount += 1 # increment the batch count
                        
                        # Sort and Write the current batch to disk  
                        main_index = self._sort_index(main_index)
                        writer_thread_queue.put((main_index, f"IndexContent/Output_Batch_{batchCount}.json"))
                        
                        main_index = defaultdict(list) # reset the main index
                        tasks_per_batch = [] # reset the tasks list

            # Process the remaining files if any
            for task in tasks_per_batch:
                partial_index, task_docId_mapping = task.get()
                for word, postings in partial_index.items():
                    main_index[word].extend(postings)

                docId_to_url_builder.update(task_docId_mapping)

            if main_index:
                batchCount += 1
                # Sort and Write remaining files to disk if any (Catch the stragglers)
                main_index = self._sort_index(main_index)
                writer_thread_queue.put((main_index, f"IndexContent/Output_Batch_{batchCount}.json"))

        # gather all {docId : url} pairs and write to disk in 
        # ONE FILE, different from the batch files which write in batches
        writer_thread_queue.put((docId_to_url_builder, "IndexContent/docID_to_URL.json"))
        writer_thread_queue.join()
        writer_thread_queue.put(None)
        writer_thread.join()

        print("\nAll files have been processed and written to disk...")
        print(f"Total docID to URL mappings: {len(docId_to_url_builder)}")
        print("-----------------------------------------------------")

        print("\n\n-----------------------------------------------------")
        print("Starting merging process...")
        print("-----------------------------------------------------")

        merger = MergeIndex()
        merger.merge_index("IndexContent/") # merge all the partial indexes into one main index

        print("All files successfully merged and categorized lexically in 'IndexCategory/...' folder!")
        print("-----------------------------------------------------\n\n")

        self.docId_to_url = docId_to_url_builder # update the docId_to_url attribute with the final dictionary

    def get_docId_to_url(self):
        return self.docId_to_url
    
if __name__ == "__main__":
    folder_path = Path('developer/DEV') # path to the folder containing all the JSON files
    total_files = 0 # total number of files in the directory

    time_start = time.time() # start the timer for index creation
    index_builder = IndexBuilder(folder_path)
    index_builder.build_index()
    time_end = time.time() # end the timer for index

    print(f"Finished Index creation process in: {time_end - time_start} seconds...")