# Search Engine(M1) - Inverted Index

## TODO:
- Identify important words -- Axel
- Check unique docId count --> docId_to_url dictionary has 54643, unique docID count has 53985
- SIMHASH implementation
- tf-idf implementation
- GUI implementation
- Convert IndexBuilder.py into a class object, have a getter for retrieving the index after creation so we don't have to build it over and over again in SearchQuery.py
- MAIN PRIORITY:
    make indexmerge way faster
    - instead of having each thread loop all over the six output batch files all over again
    - director assigns to have each thread just assigned to one file
    - then director merges it together and have it ranked by tf-idf


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