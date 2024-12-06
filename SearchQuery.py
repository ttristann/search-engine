import re
import time
import json
from collections import defaultdict
from IndexMerge import IndexMerge
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
    def __init__(self, query_text):
        self.query_text = query_text
        self.query_tokens = list()
        self.smaller_index = defaultdict(list)
        self.query_results = list()

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
        self.query_tokens = [stemmer.stem(token.lower()) for token in tokens_list if len(token) >= 3]

    def get_query_tokens(self):
        """
        Returns the updated query_tokens to be used
        outside of the function or class. 
        """

        return self.query_tokens
    
    def create_smaller_index(self):
        """
        Uses the imported IndexMerge class from the
        IndexMerge.py to create a smaller index that
        only contains the tokens from the search query. 
        """
        query_tokens = self.get_query_tokens()
        # instantiates an IndexMerge object 
        indexMerge = IndexMerge(query_tokens)
        # creates an smaller index
        indexMerge.merge_index('.')
        # assigns/updates attribute to be used in another function
        self.smaller_index = indexMerge.get_query_index()

    def get_smaller_index(self):
        """
        Returns the updated smaller_index to be used
        outside of the function or class.
        """

        return self.smaller_index

    def match_search_query(self, docId_dict, docId_built=False): 
        """
        Matches the search query tokens with the tokens
        inside the smaller index to get the top 5 results
        or documents based on tf-idf score that is assigned
        with each posting in the inverted index. 
        """
        ### this to make report for M2
        smaller_index = self.get_smaller_index()
        # compiles all of the postings into one list
        # postings_list = [smaller_index[key] for key, value in smaller_index.items()]
        # postings_list = [smaller_index[key][] for key, value in smaller_index.items()]
        postings_list = dict() # dictionary

        # this does not necessarily find the intersection between each.:(
        for key, value in smaller_index.items():
            for key1 in value:
                # print(f"this is the value being added: {key1}")
                if key not in postings_list:
                    postings_list[key] = set()
                    postings_list[key].add(key1)
                else:
                    postings_list[key].add(key1)

        print(f"this is the postings list: {postings_list}")
        return

        # this is to collect the sets of docID each token has
        docID_sets = [set(docID for docID, freq in posting) for posting in postings_list]
        # finds the intersectiong docID
        common_docIDs = set.intersection(*docID_sets)
        # filters the postings_list to only the entries that have the common docIDs
        filtered_lists = [
            [(docID, freq) for docID, freq in lst if docID in common_docIDs]
            for lst in postings_list
        ]
        # sorts them by freq descending
        sorted_filtered_lists = [
            sorted(lst, key=lambda x: x[1], reverse=True)
            for lst in filtered_lists
        ]
        # iterates the sorted_filtered_list to assign the url to each docID
        list_of_urls = list() # accumulate the urls 
        for posting_list in sorted_filtered_lists:
            for entry in posting_list:
                current_docID = entry[0]
                if docId_built == True:
                    current_url = docId_dict.get(str(current_docID))
                else:
                    current_url = docId_dict.get(current_docID)
                list_of_urls.append(current_url)

        self.query_results = list_of_urls

    def get_top5_urls(self):
        # prints the top 5 urls that matches to the search query
        discovered_urls = set()
        count = 0
        index = 0
        for url in self.query_results:
            if url not in discovered_urls:
                print(self.query_results[index])
                discovered_urls.add(url)
                count += 1
            index += 1
            if count >= 5: break


if __name__ == "__main__":
    mac_path = 'DEV'
    win_path = 'developer/DEV'

    time_start = time.time()

    docId_dict = IndexBuilder("developer/DEV")
    docId_dict.build_index()
    time_end = time.time()

    print(f"Finished Index creation process in: {time_end - time_start} seconds...")
    scores = Scoring()
    built_docId_dict = {}

    with open("docID_to_URL.txt", "r") as f:
        built_docId_dict = json.load(f) # loads the docId_dict from the disk if we already built it previously, saves time
    
    # c = 0
    # for i,v in built_docId_dict.items():
    #     if c == 5:
    #         break
    #     print(i,v)
    #     c+=1

    while True:
        query_text = input("What would you like to search for: ")
        time_start_2 = time.time()
        search = SearchQuery(query_text) # initializes SearchQuery object
        search.tokenize_query()  # # stems search query words. ex: lopes --> lope
        search.create_smaller_index() # 
        search.match_search_query(docId_dict, True) # Set to true when we already built the docId_dict
        print("Here are the top 5 results: ")
        search.get_top5_urls()
        # print(search.get_smaller_index())
        sortedTFIDF = {}

        # print(search.get_smaller_index().keys())
        # {"cristina": [["docID", "tf]"]}
        
        for key, value in search.get_smaller_index().items():
            # print(len(search.get_smaller_index()[key])) # this is DF(Document Frequency)
            # print("this is the size of smaller_index: ", search.get_smaller_index()[key].values())
            for pair in value:

                sortedTFIDF[pair[0]] = scores.tf_idf(pair[1], len(docId_dict), len(search.get_smaller_index()[key]))

            
        sortedTFIDF = dict(sorted(sortedTFIDF.items(), key=lambda item: item[1], reverse=True))
        # for key, value in sortedTFIDF.items():
        #     print(f"{key}: {value}")

        time_end_2 = time.time()
        print(f"Finished Query Search process in: {time_end_2 - time_start_2} seconds...")

