import re
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

    def tokenize_query(self):
        tokens_list = re.findall(r'\b[a-zA-Z0-9]+\b', self.query_text)
        tokens_list = [token.lower() for token in tokens_list if len(token) >= 3]

        print(tokens_list)

if __name__ == "__main__":
    query_text = "How to train your Dragon"
    search = SearchQuery(query_text)
    search.tokenize_query()