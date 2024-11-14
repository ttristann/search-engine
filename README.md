# Search Engine(M1) - Inverted Index

## TODO:
- try to iterate through the whole dev file to see how much space and time efficient the sample code is
- as new entries are added onto the index, keep sorting them based on the frequency in descending order
- Check and Remove Duplicate tokens
- Need to merge output batch files into one file, can make a separate class to do so
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