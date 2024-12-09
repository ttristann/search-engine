import math

class Scoring:

    def __init__(self):
        self.term_counts = {} #{term, {DocID, count}} ????

    def term_frequency(self, tf): #as you process
        return 1 + math.log10(tf)
    
    def inverse_document_frequency(self, N, DF): # afterward creating index
        return math.log10(N / DF)

    def tf_idf(self, tf, IDF): # afterward creating index
        return self.term_frequency(tf) * IDF #this is assuming 
    