from flask import Flask, request, render_template, jsonify
from SearchQuery import SearchQuery  # Import your existing class
import time
import json

app = Flask(__name__)

# Global variables to store the docId to URL dictionary and the bookkeeper dictionary
docId_dict, bk = {}, {}

def load_data(path):
    try:
        with open("IndexContent/docID_to_URL.json", "r") as f:
            docId_dict = json.load(f) # loads the docId_dict from the disk if we already built it previously, saves time
        with open("IndexCategory/bookkeeper.json", "r") as f:
            bk = json.load(f)
    except FileNotFoundError:
        print("Index not found. Creating Index...")

        # instantiates an IndexBuilder object and creates the inverted index
        docId_dict, bk = SearchQuery.initializeSearchData(path)

    return docId_dict, bk

@app.route('/')
def home():
    return render_template('index.html') # search input form

@app.route('/search', methods=['POST'])
def search_query():
    # Retrieve the global variables that have been procssed in main
    global docId_dict, bk
    
    # Retrieving the query from the form
    print("\n--------------------------")
    print("retrieving query...")
    query_text = request.form['query']
    print("query retrieved...")
    print("--------------------------")

    # Processing the search query
    print("\n--------------------------")
    print("processing search query...")
    search_query_start = time.time()

    search = SearchQuery(query_text, docId_dict) # initializes SearchQuery object
    search.set_bookkeeper(bk) # sets the bookkeeper dictionary for the search query
    search.tokenize_query()  # # stems search query words. ex: lopes --> lope

    finalTop10, intersections = search.create_search_index() # creates a smaller index for the search query
    sorted_finalDict, sorted_finalTop10 = search.retrieve_search_results(finalTop10, intersections) # sorts and retrieves the search results

    # Listing out the top 10 unique search results
    search_results = []
    count = 1
    for key in sorted_finalDict: #first exhaust links for intersection
        url = docId_dict[key]
        if count > 10:
            break
        search_results.append(url)
        count += 1

    for key in sorted_finalTop10: # now fill the remainding 10 with top sorted tf-idf scores, ensure no repeats with set values
        url = docId_dict[key]
        if count > 10:
            break
        if key in sorted_finalDict: # if key is in the final dict, then we already showed it.
            continue
        else:
            search_results.append(url)
            count += 1
    search_query_end = time.time()

    query_retrieval_time = f"{((search_query_end - search_query_start) * 1000):.2f}"

    print(f"search query processed in {query_retrieval_time} milliseconds...")
    print("Rendering search results...")
    print("--------------------------")

    return render_template('search_results.html', search_results=search_results, query=query_text, time_elapsed=query_retrieval_time)

if __name__ == '__main__':
    win_path = 'developer/DEV'
    mac_path = 'DEV'

    # Loading in the index data, dociId to URL converter, and bookkeeper into memory
    print("\n--------------------------")
    print("loading data...")
    data_retrieveal_start = time.time()
    docId_dict, bk = load_data(win_path)
    data_retrieval_end = time.time()
    print(f"data loaded in {(data_retrieval_end - data_retrieveal_start) * 1000} milliseconds...")
    print("--------------------------")

    app.run(debug=True) # runs the Flask app
