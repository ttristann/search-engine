import re
import time
from collections import defaultdict
from IndexMerge import IndexMerge
from IndexBuilder import build_index
from nltk.stem import SnowballStemmer
from Scoring import Scoring
from multiprocessing import Process, Manager, Pool


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

    def get_smaller_index(self):
        return self.smaller_index

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
    
    @staticmethod
    def intersect_postings(postings_pair):
        """
        Performs intersection and merging of two postings lists.
        Args:
            postings_pair (tuple): A tuple containing two postings lists to intersect.
        Returns:
            list: Merged and sorted posting list after intersection.
        """
        list1, list2 = postings_pair

        # Convert postings to sets of docIDs for intersection
        set1 = set(docID for docID, freq in list1)
        set2 = set(docID for docID, freq in list2)
        common_docIDs = set1 & set2

        # Filter and merge the postings lists based on common_docIDs
        merged_posting = [
            (docID, freq1 + freq2)
            for (docID, freq1) in list1 if docID in common_docIDs
            for (_, freq2) in list2 if docID == _
        ]

        # Sort merged postings by docID (ascending)
        return sorted(merged_posting, key=lambda x: x[0])
    


    def match_search_query(self, docId_dict):
        """
        Matches the search query tokens with the tokens
        inside the smaller index to get the top 5 results
        or documents based on tf-idf score that is assigned
        with each posting in the inverted index.
        
        This optimized version uses multiprocessing to
        perform intersections in batches.
        """
        # Step 1: Retrieve the smaller index
        smaller_index = self.get_smaller_index()

        # Step 2: Compile all postings lists
        postings_list = [
            smaller_index[key] for key in smaller_index
        ]

        # Step 3: Sort postings lists by size (smallest first)
        postings_list.sort(key=len)

        # Step 4: Perform pairwise intersections using multiprocessing
        with Pool(processes=4) as pool:  # Adjust the number of processes based on your system
            while len(postings_list) > 1:
                # Create pairs of postings lists
                pairs = [(postings_list[i], postings_list[i + 1]) for i in range(0, len(postings_list) - 1, 2)]

                # Perform parallel intersection
                results = pool.map(self.intersect_postings, pairs)

                # Handle odd postings list (carry over the last unpaired list)
                if len(postings_list) % 2 == 1:
                    results.append(postings_list[-1])

                postings_list = results
                postings_list.sort(key=len)

        # Step 5: Extract the final intersection result
        if postings_list:
            final_posting = postings_list[0]
        else:
            final_posting = []

        # Step 6: Map docIDs to URLs and sort by tf-idf score
        sorted_results = sorted(final_posting, key=lambda x: x[1], reverse=True)
        list_of_urls = [docId_dict.get(docID) for docID, _ in sorted_results]

        # Limit to top 5 results
        self.query_results = list_of_urls[:5]


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


if __name__ == "__main__":
    time_start = time.time()
    docId_dict = build_index("DEV")
    time_end = time.time()
    print(f"Finished Index creation process in: {time_end - time_start} seconds...")
    scores = Scoring()
    
    while True:
        query_text = input("What would you like to search for: ")
        time_start_2 = time.time()
        search = SearchQuery(query_text) # initializes SearchQuery object
        search.tokenize_query()  # # stems search query words. ex: lopes --> lope
        search.create_smaller_index() # 
        search.match_search_query(docId_dict)
        print("Here are the top 5 results: ")
        search.get_top5_urls()
        sortedTFIDF = {}

        # for key, value in search.get_smaller_index().items():
        #     for pair in value:
        #         # smaller index formatted in --> dictionary{query: list[[docID: count of word in doc]]}
        #         print(f"DOCID: {pair[0]}, TF: {scores.term_frequency(pair[1])}", end=" ")
        #         print(f"IDF: {scores.inverse_document_frequency(len(docId_dict), len(search.get_smaller_index()[key]))}", end=" ")
        #         print(
        #             f"TF-IDF: {scores.term_frequency(pair[1]) * scores.inverse_document_frequency(len(docId_dict), len(search.get_smaller_index()[key]))}")
                
                
        #         sortedTFIDF[pair[0]] = scores.term_frequency(pair[1]) * scores.inverse_document_frequency(len(docId_dict), len(search.get_smaller_index()[key]))
        # sortedTFIDF = dict(sorted(sortedTFIDF.items(), key=lambda item: item[1], reverse=True))
        # for key, value in sortedTFIDF.items():
        #     print(f"{key}: {value}")

        time_end_2 = time.time()
        print(f"Finished Query Search process in: {time_end_2 - time_start_2} seconds...")
