
class Scoring:


    def __init__(self):
        self.term_counts = {} #{term, {DocID, count}} ????


        

    # Term Frequency(TF) = (Number of times term t appears in text / total # of terms in text)
    # Inverse Document Frequency(IDF) = log((Total # of texts in corpus) / # of texts containing term)
    # tf-idf = term_frequency(t, text) * inverse_document_frequency(t, corpus)

    def term_frequency(self, t, text): #as you process
        pass
    
    def inverse_document_frequency(self, t, corpus): # afterward creating index
        pass
    
    def tf_idf(self, text, corpus): # afterward creating index
        pass