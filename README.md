# Search Engine(M1) - Inverted Index

## TODO:
- Identify important words -- Axel
- Check unique docId count --> docId_to_url dictionary has 54643, unique docID count has 53985
- SIMHASH implementation
- tf-idf, maybe wih the cosine implementation
- GUI implementation
- figure out way to implement the proper way of finding intersections (starting with smallest set) -- tristan
- MAIN PRIORITY:
    To make the search portion way more faster than as of right now (~4.13 seconds for crista lopes),
    need to implement a multiprocessing functionality when creating the smaller index that only has
    the tokens that has been parsed from the search query. 
    
    Proposed process:
    - break up the search query terms 
    - have a "director" machine assign different processes to different query terms
    - each process will create an even smaller index that has the subquery terms
    - once all processes have been finished, the director will combine them
    - assign combined index to the main attribute small_index


## Setup
1. Clone repository ```git clone https://github.com/ttristann/search-engine.git```
2. Navigate into project directory ```cd search-engine```
3. Install Python dependencies ```pip install -r requirements.txt```


## Run inverted index builder
1. Navigate to ```IndexBuilder.py```
2. Run the file either...
    - On your IDE (hitting the ```run``` button or similar)
    - On your terminal, bash, etc. 
    ```python InvertedIndexBuilder.py```