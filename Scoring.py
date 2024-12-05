import math

class Scoring:

    def __init__(self):
        self.term_counts = {} #{term, {DocID, count}} ????

        
    # Term Frequency(TF) = (Number of times term t appears in text / total # of terms in text)
    # Inverse Document Frequency(IDF) = log((Total # of texts in corpus) / # of texts containing term)
    # tf-idf = term_frequency(TFt) * inverse_document_frequency(N / DF)

    def term_frequency(self, tf): #as you process
        return 1 + math.log10(tf)
    
    def inverse_document_frequency(self, N, DF): # afterward creating index
        return math.log10(N / DF)
    
    def tf_idf(self, tf, N, DF): # afterward creating index
        # 1 + log(TF) * log(N / DF)
        return self.term_frequency(tf) * self.inverse_document_frequency(N, DF)