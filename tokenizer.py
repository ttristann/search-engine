from collections import defaultdict
"""
Tokenizer class that takes in a string of text 
that has been filtered through by Beautiful Soup
and tokenizes and normalizes the text into 
individual significant tokens. 

Conditions to be considered as a token:
    - not a stop word
    - alphanumeric
    - at least the length of 3
"""

class Tokenizer:
    def __init__(self):
        self.stop_words = {
            'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 
            'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 
            'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 
            'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from',
            'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", 
            "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 
            'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't",
            'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 
            'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 
            'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", 
            "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 
            'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 'they', 
            "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 
            'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were',
            "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', 
            "who's", 'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", 
            "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves'                    
        }
        self.tokens = defaultdict(int)

    def tokenize(self, main_text):
        """
        Iterates through the main text to build tokens
        by adding alphanumeric characters to a token, 
        then checks if the resulting token meets the 
        condition to be a valid one. 
        """
        current_token = []
        for char in main_text:
            if ('A' <= char <='Z') or ('a' <= char <= 'z') or ('0' <= char <= '9'): # checking for alphanumeric value
                    current_token.append(char.lower()) # normalizes it
            else:
                if current_token and len(current_token) >= 3:
                    combined = ''.join(current_token)
                    # if "Institutions" in combined:
                    #     print(f"This is the wordddd:      {combined}\n\n\n\n")
                    print(f"this is the word: {combined}")
                    if combined not in self.stop_words:
                         yield combined
                current_token = [] # resets it to make a new token
            
            # for char in stringDoc:
            #     # we will check via ascii number. If it is a (number or char): (then keep), else: (ignore)
            #     if((65 <= ord(char) <= 90) or (97<=ord(char)<=122) or (48<=ord(char)<=57)):
            #         word += char.lower()

            #     elif word != "": # ensures we are not adding an empty string to the tokens list
            #         tokens.append(word)
            #         word = ""

        if current_token and len(current_token) >= 3: # accounts for the last token to be yielded
            combined = ''.join(current_token)
            if combined not in self.stop_words:
                yield combined

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

    


         
    