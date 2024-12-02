# Search Engine(M1) - Inverted Index

## TODO:
- Identify important words -- Axel
- Check unique docId count --> docId_to_url dictionary has 54643, unique docID count has 53985
- SIMHASH implementation
- tf-idf, maybe wih the cosine implementation
- GUI implementation
- figure out way to implement the proper way of finding intersections (starting with smallest set) -- tristan
- MAIN PRIORITY:
    KEEP THINKING OF WAYS TO MAKE IT FASTER (RN 2.64 for "crista lopes")


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