from collections import defaultdict
from simHashing import Simhashing
"""
Tokenizer class that takes in a string of text 
that has been filtered through by Beautiful Soup
and tokenizes and normalizes the text into 
individual significant tokens. 

Conditions to be considered as a token:
    - alphanumeric
    - at least the length of 3
"""

class Tokenizer:
    def __init__(self):
        self.tokens = defaultdict(int) # keeps track of all the tokens and their frequencies

    def tokenize(self, main_text):
        """
        Iterates through the main text to build tokens
        by adding alphanumeric characters to a token, 
        then checks if the resulting token meets the 
        condition to be a valid one. 
        """
        

        final_string = " "
        current_token = []
        for char in main_text:
            if ('A' <= char <='Z') or ('a' <= char <= 'z') or ('0' <= char <= '9'): # checking for alphanumeric value
                    current_token.append(char.lower()) # normalizes it
            else:
                if current_token and len(current_token) >= 3:
                    combined = ''.join(current_token)
                    yield combined
                current_token = [] # resets it to make a new token

        if current_token and len(current_token) >= 3: # accounts for the last token to be yielded
            combined = ''.join(current_token)
            yield combined

    def modified_tokenize(self, main_text, word):
        """
        Iterates through the main text to build tokens
        by adding alphanumeric characters to a token, 
        then checks if the resulting token meets the 
        condition to be a valid one. 
        """
        
        current_word_Count = dict()
        unique_words = set()
        current_word_Count[word] = 0
        final_string = " "
        current_token = []
        for char in main_text:
            if ('A' <= char <='Z') or ('a' <= char <= 'z') or ('0' <= char <= '9'): # checking for alphanumeric value
                    current_token.append(char.lower()) # normalizes it
            else:
                if current_token and len(current_token) >= 3:
                    combined = ''.join(current_token)
                    if combined == word:
                        current_word_Count[word] += 1
                    unique_words.add(combined)
                        
                current_token = [] # resets it to make a new token

        if current_token and len(current_token) >= 3: # accounts for the last token to be yielded
            combined = ''.join(current_token)
            if combined == word:
                current_word_Count[word] += 1
            unique_words.add(combined)
        return [current_word_Count, unique_words]

    def compute_frequencies(self, tokens):
        """
        Updates the tokens attribute to keep track 
        of the frequencies of the tokens. 
        
        Organizes the tokens in descending order
        starting with the most frequent token.
        """
        for token in tokens:
            self.tokens[token] += 1

        # sorting the tokens in descending order
        self.tokens = dict(sorted(self.tokens.items(), key=lambda item: item[1], reverse=True))

    def getTokens(self):
        return self.tokens

    


         
    