from pathlib import Path
import re
import time
import json
import string

from collections import defaultdict
from QueryIndex import QueryIndex
from functools import lru_cache

# from IndexBuilder import build_index
from IndexBuilder import IndexBuilder
from nltk.stem import PorterStemmer
from nltk.tokenize import RegexpTokenizer
from Scoring import Scoring
import nltk
import multiprocessing


"""
This class is utilzed for analyzing and parsing 
the search query that is inputted into the system. 
It iterates through each word in the search query
to stem and tokenize the words to be matched to
the index terms that are inside the inverted index. 

Once it matches the tokens inside the inverted index, 
it execute the AND boolean logic for each token to 
get the top 5 results of documents that has the tokens
or words inside it.
"""

class SearchQuery:
    def __init__(self, query_text, docId_dict):
        self.query_text = query_text
        self.docId_dict = docId_dict

        self.query_tokens = list()
        self.search_index = defaultdict(list)
        self.bookkeeper = defaultdict(dict)
    
    def get_search_index(self):
        return self.search_index

    def tokenize_query(self):
        """
        Goes through the query text to analyze
        each word in it to stem and tokenize it. 

        Updates the query_tokens attribute with
        a list of valid tokens that can be used
        with the inverted index. 
        """
        # initializes the stemmer
        stemmer = PorterStemmer()

        # tokenizes the string query text 
        tokenizer = RegexpTokenizer(r'\b[a-zA-Z0-9]+\b')
        tokens_list = tokenizer.tokenize(self.query_text)

        tokens_list = list(set(tokens_list)) # removes duplicates
        # updates and assigns the attribute self.query_tokens
        self.query_tokens = [stemmer.stem(token.lower()) for token in tokens_list]

    def get_query_tokens(self):
        """
        Returns the updated query_tokens to be used
        outside of the function or class. 
        """

        return self.query_tokens
    
    def build_bookkeeper(self):
        """
        Builds the bookkeeper dictionary that holds
        token offset values within the inverted index

        Structure:
        {
            file_name: {
                token: offset,
                token2: offset2,
            },
            file_name2: {
                token: offset
            },
            ...
        }
        """
        for json_file in Path('IndexCategory').rglob('*.json'):
            with open(json_file, "r") as f:
                token_list = list()
                started = False

                while True:

                    char = f.read(1) # reads one character at a time
                    
                    if not char:
                        # end of file, move to the next file
                        break
                    
                    if char == '"':
                        # indicates the start/end of a token since the token is enclosed in double quotes (All other values such as docID, tf, and weight are represented as numbers)
                        if not started:
                            # token building process has started, we start retrieving the token's offset
                            started = True
                            offset = f.tell() - 1 # gets the starting offset position of the token
                        else:
                            # token building process has ended, we add the token and its offset to the bookkeeper dictionary
                            started = False
                            token = "".join(token_list) # joins the token list to form the token
                            self.bookkeeper[json_file.name][token] = offset # update the bookkeeper to hold the token and its offset for the given file
                            token_list.clear() # clear the token list for the next token
                    elif started:
                        # token building process has started, read the token one character at a time, add each character to the token list
                        token_list.append(char)
        
        with open("bookkeeper.json", "w") as f:
            json.dump(self.bookkeeper, f, indent=4)
    
    def parse_json(self, file):
        """
        Parses the json file from the current position
        Retrieves a given token's posting list from the json file
        file should already be opened
        """
        open_bracket = 0
        char_list = list()

        while True:
            # locate the first open bracket for the list of postings
            char = file.read(1)
            # print(char)
            if not char:
                break

            if char == '[':
                # Found the first open bracket, start reading the postings
                open_bracket += 1
                char_list.append(char)
                break
        
        while open_bracket > 0:
            char = file.read(1)
            if not char:
                break
            char_list.append(char)
            if char == '[':
                open_bracket += 1
            elif char == ']':
                open_bracket -= 1
        
        return "".join(char_list)
                
        
    @lru_cache(maxsize=10) # Limits cache size to 10 loaded files at a time, least used files are removed automatically
    def create_search_index(self):
        """
        Creates a smaller index containing only the tokens from the search query.
        Returns:
            finalTop10 (dict): Mapping of document IDs to their scores.
            intersections (set): Set of document IDs present in all tokens.
        """
        finalTop10 = {}
        intersections = set()
        loadedFiles = defaultdict(lambda: defaultdict(list))
        # bookkeeper = self.get_bookkeeper()
        # print(self.bookkeeper)

        # Group tokens by their starting character
        category_tokens = defaultdict(list)
        for token in self.query_tokens:
            category_tokens[token[0]].append(token)

        # Process each group
        for char, tokens in category_tokens.items():
            file = f"IndexCategory/{char}.json"
            try:
                with open(file, "r", encoding="utf-8") as f:
                    for token in tokens:
                        # Check if token exists in the bookkeeper
                        if token not in self.bookkeeper.get(f"{char}.json", {}):
                            print(f"Sorry '{token}' does not seem to exist in the index. Continuing...")
                            continue
                        
                        # Use the token's offset found in bookkeeper to retrieve the postings
                        offset = self.bookkeeper[char + ".json"][token]
                        f.seek(offset)  # Seek to the token offset
                        postings_json = self.parse_json(f)
                        postings = json.loads(postings_json)
                        loadedFiles[char][token].extend(postings)
    
            except FileNotFoundError:
                print(f"File '{char}.json' not found in 'IndexCategory'.")
                continue
            except Exception as e:
                print(f"An unexpected error occurred while opening '{char}.json': {e}")
                continue

        # Compute finalTop10 and intersections
        for token in self.query_tokens:
            first_char = token[0]
            postings = loadedFiles[first_char].get(token, [])

            if not postings:
                print(f"No postings found for token '{token}'.")
                continue

            tempSet = set()
            for posting in postings:
                doc_id_str = str(posting[0])
                score = posting[2]
                if doc_id_str not in finalTop10:
                    finalTop10[doc_id_str] = score
                tempSet.add(doc_id_str)

            if not intersections:
                intersections = tempSet
            else:
                intersections.intersection_update(tempSet)

        return finalTop10, intersections

    def retrieve_search_results(self, finalTop10, intersections):
        """
        Retrieves the search results from the finalTop10
        dictionary and intersections set. It retrieves the
        top 5 results from the finalTop10 dictionary and 
        prints the results to the user. 
        """
        finaldict = dict()
        for docId in intersections:
            finaldict[docId] = finalTop10[docId] #finalDict only holds intersections.
        finaldict = dict(sorted(finaldict.items(), key=lambda item: item[1], reverse=True)) #only keys are intersections
        finalTop10 = dict(sorted(finalTop10.items(), key=lambda item: item[1], reverse=True)) #everything
        

        count = 1
        for key in finaldict: #first exhaust links for intersection 
            if count > 10:
                break
            print(f"\t{count}. {docId_dict[key]}\n")
            count += 1
        
        for key in finalTop10: # now fill the remainding 10 with top sorted tf-idf scores, ensure no repeats with set values
            if count > 10:
                break
            if key in finaldict: # if key is in the final dict, then we already showed it.
                continue
            else:
                print(f"\t{count}. {docId_dict[key]}\n")
                count += 1
                
    def get_bookkeeper(self):
        with open("bookkeeper.json", "r") as f:
            bookkeeper = json.load(f)
        return bookkeeper
    
    def set_bookkeeper(self, bookkeeper):
        self.bookkeeper = bookkeeper
      
if __name__ == "__main__":
    mac_path = 'DEV'
    win_path = 'developer/DEV'

    scores = Scoring()
    bigData = {}

    time_start = time.time()
    
    # Index Creation/Loading
    try:
        with open("IndexContent/docID_to_URL.json", "r") as f:
            docId_dict = json.load(f) # loads the docId_dict from the disk if we already built it previously, saves time
            with open("bookkeeper.json", "r") as b:
                bookkeeper = json.load(b)
    except FileNotFoundError:
        print("Index not found. Creating Index...")
        # instantiates an IndexBuilder object and creates the inverted index
        indexBuilder = IndexBuilder(mac_path)
        indexBuilder.build_index()
        docId_dict = indexBuilder.get_docId_to_url() # retrieves the docId_dict to be used in for searching
        bookkeeper = IndexBuilder.get_bookkeeper() # retrieves the bookkeeper dictionary
    
    time_end = time.time()
    print(f"Retrieved Index in: {time_end - time_start} seconds...\n")
    
    # search2 = SearchQuery("covid", docId_dict)
    # bk = search2.get_bookkeeper()
    ###############################################################################
    
    # Search Query Processing
    while True:
        query_text = input("What would you like to search for: ")
        print("\n")
        time_start_2 = time.time()
        
        search = SearchQuery(query_text, docId_dict) # initializes SearchQuery object
        search.set_bookkeeper(bookkeeper) # sets the bookkeeper dictionary for the search query
        search.tokenize_query()  # # stems search query words. ex: lopes --> lope
        # bookkeeper = search.get_bookkeeper() # retrieves the bookkeeper dictionary
        # for key in bookkeeper.keys():
        #     print(key)

        finalTop10, intersections = search.create_search_index() # creates a smaller index for the search query
        search.retrieve_search_results(finalTop10, intersections) # retrieves the search results

        

        time_end_2 = time.time()
        print(f"\nFinished Query Search process in: {(time_end_2 - time_start_2) * 1000} miliseconds...")
        print("\n\n")