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
    # mac_path = 'DEV'
    # # win_path = 'developer/DEV'

    # time_start = time.time()
    
    # # instantiates an IndexBuilder object and creates the inverted index
    # # indexBuilder = IndexBuilder(mac_path)
    # # indexBuilder.build_index()
    # # docId_dict = indexBuilder.get_docId_to_url() # retrieves the docId_dict to be used in for searching
    
    # time_end = time.time()

    # print(f"Finished Index creation process in: {time_end - time_start} seconds...")
    # scores = Scoring()
    # built_docId_dict = {}
    # bigData = {}

    with open("IndexContent/docID_to_URL.json", "r") as f:
        built_docId_dict = json.load(f) # loads the docId_dict from the disk if we already built it previously, saves time
    
    # # with open("IndexContent/Output_Batch_1.json", "r") as f: #Axel wrote this, testing purposes, still need to merge
    # #     bigData = json.load(f) # loads the docId_dict from the disk if we already built it previously, saves time

    # # c = 0
    # # for i,v in built_docId_dict.items():
    # #     if c == 5:
    # #         break
    # #     print(i,v)
    # #     c+=1

    # while True:
    #     query_text = input("What would you like to search for: ")
    #     time_start_2 = time.time()

    #     search = SearchQuery(query_text, built_docId_dict) # initializes SearchQuery object
    #     search.tokenize_query()  # # stems search query words. ex: lopes --> lope
    #     # search.create_smaller_index() # 
    #     # search.match_search_query(built_docId_dict)
    #     # loadedFiles = dict()
    #     # for token in search.get_query_tokens():
    #     #     if token[0] not in loadedFiles: #file has not been loaded, we need to load it
    #     #         file = token[0] + ".json"
    #     #         with open(f"IndexCategory/{file}", "r") as f:
    #     #             loadedFiles[token[0]] = json.load(f) #we now have access to the folder
            
    #     #     if token not in loadedFiles[token[0]]:
    #     #         print("no query exists")
    #     #     else:
    #     #         count = 0
    #     #         print(f"these are the top 10 query terms for this word: {token}")
    #     #         #
    #     #         # print(f"this is the loaded Files{loadedFiles[token[0]][token]}")
    #     #         for posting in loadedFiles[token[0]][token]:
    #     #             if count > 10:
    #     #                 break
    #     #             print(built_docId_dict[str(posting[0])])
    #     #             count += 1
            




    #     # print("Here are the top 5 results: ")
    #     # search.get_top5_urls()
    #     # print(f"this is the len of built docID: {len(built_docId_dict)}: this is the bigData: {len(bigData)}")

    #     # print(search.getScoreData(built_docId_dict, bigData, scores, search.get_query_tokens()))
    #     # sortedTFIDF = {}

    #     time_end_2 = time.time()
    #     print()
    #     print(f"Finished Query Search process in: {time_end_2 - time_start_2} seconds...")
    #     print()
    while True:
        query_text = input("What would you like to search for: ")

        start_time = time.time()
        search = SearchQuery(query_text, built_docId_dict) 
        search.tokenize_query()  
        search.create_smaller_index() 
        end_time = time.time()
        print(f"Finished creating search index in {(end_time - start_time)* 1000} miliseconds...")