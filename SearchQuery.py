import re
from collections import defaultdict
from IndexMerge import IndexMerge
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
        self.smaller_index = defaultdict(dict)

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

    def match_search_query(self): 
        """
        Matches the search query tokens with the tokens
        inside the smaller index to get the top 5 results
        or documents based on tf-idf score that is assigned
        with each posting in the inverted index. 

        THIS IS A BASIC IMPLEMENTATION RIGHT NOW, ONLY GETS
        THE TOP 5 URLS FOR EACH TOKEN

        TODO: implement a way to incorporate boolean logic
        """
        pass 




if __name__ == "__main__":
    query_text = "How to train your Dragon"
    search = SearchQuery(query_text)
    search.tokenize_query()