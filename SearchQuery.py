import re
from collections import defaultdict
from IndexMerge import IndexMerge
from IndexBuilder import docId_dict, build_index
from nltk.stem import SnowballStemmer


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
        self.query_results = set()

    def tokenize_query(self):
        """
        Goes through the query text to analyze
        each word in it to stem and tokenize it. 

        Updates the query_tokens attribute with
        a list of valid tokens that can be used
        with the inverted index. 
        """
        # initializes the stemmer
        stemmer = SnowballStemmer("english")

        # tokenizes the string query text 
        tokens_list = re.findall(r'\b[a-zA-Z0-9]+\b', self.query_text)
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

    def match_search_query(self): 
        """
        Matches the search query tokens with the tokens
        inside the smaller index to get the top 5 results
        or documents based on tf-idf score that is assigned
        with each posting in the inverted index. 

        THIS IS A BASIC IMPLEMENTATION RIGHT NOW, ONLY GETS
        THE TOP 5 URLS FOR EACH TOKEN

        TODO: implement a way to incorporate boolean AND ogic
        across the different tokens
        """
        # # in order for the docID dict to be compiled IndexBuilder has be to executed first
        # # print(len(docId_dict))
        # smaller_index = self.get_smaller_index()
        # for token in smaller_index:
        #     count = 0 # to keep track how many of the top 5 urls have been added
        #     postings = smaller_index.get(token, [])
        #     # iterates the postings for current token
        #     for posting in postings:
        #         current_docID = posting[0] 
        #         # print(f"current_docid: {current_docID}")
        #         current_url = docId_dict.get(current_docID)
        #         # print(f"current_url: {current_url}")
        #         # appends the url to the top 5
        #         self.query_results[token].append(current_url)
        #         count += 1
        #         if count >= 5: break
                
        # # for testing purposes
        # for key in self.query_results:
        #     print(f"token: {key}\n")
        #     for entry in self.query_results[key]:
        #         print(f"\t {entry}")

        #     print("---------------------")

        ### this to make report for M2
        smaller_index = self.get_smaller_index()
        # compiles all of the postings into one list
        postings_list = [smaller_index[key] for key in smaller_index]
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
        set_of_urls = set() # accumulate the urls 
        for posting_list in sorted_filtered_lists:
            for entry in posting_list:
                current_docID = entry[0]
                current_url = docId_dict.get(current_docID)
                set_of_urls.add(current_url)

        self.query_results = set_of_urls

    def get_top5_urls(self):
        count = 0
        for url in self.query_results:
            print(url)
            count += 1
            if count >= 5: break


        






if __name__ == "__main__":
    query_text = "master of software engineering"
    build_index("DEV")
    search = SearchQuery(query_text)
    search.tokenize_query()
    search.create_smaller_index()
    search.match_search_query()
    search.get_top5_urls()
