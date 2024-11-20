# Search Engine(M1) - Inverted Index

## TODO:
- Need to merge a smaller index in batches (if needed) -- Tristan
- Need to sort the smaller index properly, look at the smaller_index.txt to see how it is being currently sorted -- Tristan
- Parse and tokenize the search query text -- Whole group 
- Identify important words -- Axel
- Possibly implement simhash algo to avoid near-duplicates, need to talk about this one tho
- Also possibly implement threading and/or multiprocessing to speed up index


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