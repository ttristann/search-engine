# Search Engine(M1) - Inverted Index

## TODO:
- MAIN PROIRITY - MAKE SCRIPTS TO MAKE THE REPORT
- Make it more efficient, try out multi-processing instead of multi-threading - current standing 110 min/500 mb
- Check and Remove Duplicate url with the same content (use simhash for next milestone)
- Need to merge output batch files into one file, can make a separate class to do so
- Try to import project to openlab to check the if program is more efficient there compared to local devices

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