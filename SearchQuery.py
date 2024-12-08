import re
import time
import json
from collections import defaultdict
from QueryIndex import QueryIndex
# from IndexBuilder import build_index
from IndexBuilder import IndexBuilder
from nltk.stem import PorterStemmer
from nltk.tokenize import RegexpTokenizer
from Scoring import Scoring
import nltk


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
        self.query_results = list()
        self.queryResultsFrequencies = {} #{docID: {term: frequencie}}
    
    def getQueryResults(self):
        return self.query_results
    
    def get_search_index(self):
        return self.search_index
    
    def get_query_tokens(self):
        return self.query_tokens

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
        tokenizer = RegexpTokenizer(r'\b[a-zA-Z0-9]{3,}\b')
        tokens_list = tokenizer.tokenize(self.query_text)
        # updates and assigns the attribute self.query_tokens
        self.query_tokens = [stemmer.stem(token.lower()) for token in tokens_list]

    def get_query_tokens(self):
        """
        Returns the updated query_tokens to be used
        outside of the function or class. 
        """

        return self.query_tokens
    
    def create_smaller_index(self):
        """
        Uses the imported QueryIndex class from the
        QueryIndex.py to create a smaller index that
        only contains the tokens from the search query. 
        """
        query_tokens = self.get_query_tokens()
        # instantiates an QueryIndex object 
        query_index = QueryIndex(query_tokens)
        # creates an smaller index
        query_index.build_query_index()
        # assigns/updates attribute to be used in another function
        # self.search_index = query_index.get_query_index()

        

if __name__ == "__main__":
    mac_path = 'DEV'
    # win_path = 'developer/DEV'

    time_start = time.time()
    
    # instantiates an IndexBuilder object and creates the inverted index
    indexBuilder = IndexBuilder(mac_path)
    indexBuilder.build_index()
    # docId_dict = indexBuilder.get_docId_to_url() # retrieves the docId_dict to be used in for searching
    
    time_end = time.time()

    print(f"Finished Index creation process in: {time_end - time_start} seconds...\n\n")

    with open("IndexContent/docID_to_URL.json", "r") as f:
        built_docId_dict = json.load(f) # loads the docId_dict from the disk if we already built it previously, saves time

    while True:
        query_text = input("What would you like to search for?: ")
        time_start_2 = time.time()

        search = SearchQuery(query_text, built_docId_dict) # initializes SearchQuery object
        search.tokenize_query()  # # stems search query words. ex: lopes --> lope
        finalTop10 = dict()
        intersections = set()
        loadedFiles = dict()
        
        print(f"\nHere are the results:\n")

        for token in search.get_query_tokens():
            if token[0] not in loadedFiles: #file has not been loaded, we need to load it
                file = token[0] + ".json"
                with open(f"IndexCategory/{file}", "r") as f:
                    loadedFiles[token[0]] = json.load(f) #we now have access to the folder
            
            if token not in loadedFiles[token[0]]:
                print("No there are no documents that have your query.\nPlease try another query. ")
            else:
                tempSet = set() # this set will be intersected by master set
                for posting in loadedFiles[token[0]][token]:
                    if str(posting[0]) not in loadedFiles:
                        finalTop10[str(posting[0])] = posting[2]
                        tempSet.add(str(posting[0]))

                if len(intersections) == 0:
                    intersections = tempSet
                else:
                    intersections = intersections.intersection(tempSet)

        finaldict = dict()
        for docId in intersections:
            finaldict[docId] = finalTop10[docId] #finalDict only holds intersections.
        finaldict = dict(sorted(finaldict.items(), key=lambda item: item[1], reverse=True)) #only keys are intersections
        finalTop10 = dict(sorted(finalTop10.items(), key=lambda item: item[1], reverse=True)) #everything 
        
        count = 0
        for key in finaldict: #first exhaust links for intersection 
            if count > 10:
                break
            print(f'\t{built_docId_dict[key]}')
            count += 1
        for key in finalTop10: # now fill the remainding 10 with top sorted tf-idf scores, ensure no repeats with set values
            if count > 10:
                break
            if key in finaldict: # if key is in the final dict, then we already showed it.
                continue
            else:
                print(f'\t{built_docId_dict[key]}')
                count += 1

        time_end_2 = time.time()
        print(f"\nFinished Query Search process in: {(time_end_2 - time_start_2) * 1000} milliseconds...")
        print("\n")