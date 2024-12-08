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
        self.search_index = query_index.get_query_index()

    def match_search_query(self, docId_dict, docId_built=False): 
        """
        Matches the search query tokens with the tokens
        inside the smaller index to get the top 5 results
        or documents based on tf-idf score that is assigned
        with each posting in the inverted index. 

        TO DO: NEED A MORE EFFICIENT WAY OF MATCHING
        """
        ### this to make report for M2
        smaller_index = self.get_search_index()
        print(f"this is the smaller index: {smaller_index}")
        # compiles all of the postings into one list
        # postings_list = [smaller_index[key] for key, value in smaller_index.items()]
        # postings_list = [smaller_index[key][] for key, value in smaller_index.items()]
        postings_list = dict() # dictionary
        sort_order = dict()
        # this does not necessarily find the intersection between each.:(
        # for key, value in smaller_index.items():
        #     for key1 in value:
        #         # print(f"this is the value being added: {key1}")
        #         if key not in postings_list:
        #             postings_list[key] = set()
        #             postings_list[key].add(key1)
        #         else:
        #             postings_list[key].add(key1)

        # print(f"this is the postings list: {postings_list}")
        return

        # this is to collect the sets of docID each token has
        # docID_sets = [set(docID for docID, freq in posting) for posting in postings_list]

        # finds the intersectiong docID
        # common_docIDs = set.intersection(*docID_sets)
        print(f"this is the common DOCIDS: {postings_list}")
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


    def getScoreData(self, docID, termData, scores, tokens):
        """  docID --> {docID: url}
            termData --> {term : [docID, termFrequency, weight]}
            We want to find the tf-idf scores for these terms.
        """
        N = len(docID) # O(1)
        tfIDF_scores = dict() # key is term, value is dict--> {docID: tfIDF}. This way we can retrieve data fast
        
        # tokens --> 
        # print(f"these are the tokens: {tokens}") # tokens --> list ["crista", "lope"]
        
        for token in tokens:
            if token not in termData: #if there is no key in dictionary, then no words that match it are found
                continue
            print(termData[token])
            DF = len(termData[token]) #the number of lists for that term is the number of documents that word is in
            IDF = scores.inverse_document_frequency(N, DF) # these calulations will only be done once, IDF only changes in between terms
            # termScores = dict()

            for documentData in termData[token]:
                    # tf-IDF -->  1 + log(TF) * log(N / DF)
                    # documentData = [docID, termFrequency, weight]
                    # termScores[token][documentData[0]] = scores.tf_idf(documentData[1], IDF)   #tfIDF_scores = {"cristina": {docID: tf-IDF, docID: tf-IDF}}
                    if documentData[0] in tfIDF_scores:
                        tfIDF_scores[documentData[0]] += (scores.tf_idf(documentData[1], IDF) + documentData[2]) #Add score instead of replacing value
                    else:
                        tfIDF_scores[documentData[0]] = (scores.tf_idf(documentData[1], IDF) + documentData[2]) #create dictionary pair --> term: tf-idf
            # tfIDF_scores.update(termScores)

        sorted_dict = dict(sorted(tfIDF_scores.items(), key=lambda item: item[1], reverse=True))
        print("\n\n")
        return sorted_dict





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
            if count >= 10: break
        return discovered_urls

if __name__ == "__main__":
    mac_path = 'DEV'
    # win_path = 'developer/DEV'

    time_start = time.time()
    
    # instantiates an IndexBuilder object and creates the inverted index
    # indexBuilder = IndexBuilder(mac_path)
    # indexBuilder.build_index()
    # docId_dict = indexBuilder.get_docId_to_url() # retrieves the docId_dict to be used in for searching
    
    time_end = time.time()

    print(f"Finished Index creation process in: {time_end - time_start} seconds...")
    scores = Scoring()
    built_docId_dict = {}
    bigData = {}

    with open("IndexContent/docID_to_URL.json", "r") as f:
        built_docId_dict = json.load(f) # loads the docId_dict from the disk if we already built it previously, saves time
    
    # with open("IndexContent/Output_Batch_1.json", "r") as f: #Axel wrote this, testing purposes, still need to merge
    #     bigData = json.load(f) # loads the docId_dict from the disk if we already built it previously, saves time

    # c = 0
    # for i,v in built_docId_dict.items():
    #     if c == 5:
    #         break
    #     print(i,v)
    #     c+=1

    while True:
        query_text = input("What would you like to search for: ")
        time_start_2 = time.time()

        search = SearchQuery(query_text, built_docId_dict) # initializes SearchQuery object
        search.tokenize_query()  # # stems search query words. ex: lopes --> lope
        # search.create_smaller_index() # 
        # search.match_search_query(built_docId_dict)
        loadedFiles = dict()
        for token in search.get_query_tokens():
            if token[0] not in loadedFiles: #file has not been loaded, we need to load it
                file = token[0] + ".json"
                with open(f"IndexCategory/{file}", "r") as f:
                    loadedFiles[token[0]] = json.load(f) #we now have access to the folder
            
            if token not in loadedFiles[token[0]]:
                print("no query exists")
            else:
                count = 0
                print(f"these are the top 10 query terms for this word: {token}")
                #
                # print(f"this is the loaded Files{loadedFiles[token[0]][token]}")
                for posting in loadedFiles[token[0]][token]:
                    if count > 10:
                        break
                    print(built_docId_dict[str(posting[0])])
                    count += 1
            




        # print("Here are the top 5 results: ")
        # search.get_top5_urls()
        # print(f"this is the len of built docID: {len(built_docId_dict)}: this is the bigData: {len(bigData)}")

        print(search.getScoreData(built_docId_dict, bigData, scores, search.get_query_tokens()))
        sortedTFIDF = {}

        time_end_2 = time.time()
        print(f"Finished Query Search process in: {time_end_2 - time_start_2} seconds...")