from simhash import Simhash
import hashlib

class Simhashing:
    
    def __init__(self):
        self.url_hashValues = set()
        self.similarities = set()
    
    # the closer to 0 the more similar they are
    def computeHash(self, url, text):
        hashValue = Simhash(text).value

        print("this is inside the compute Hash function")
        for HashValue in self.url_hashValues:
            if self.hamming_distance(hashValue, HashValue) < 5:
                self.url_hashValues.add(hashValue)
                self.similarities.add(url) # similar document has already been found, ignore
                return


        self.url_hashValues.add(hashValue) # if no similar match is found then add it to unique hashValues set

        print(self.url_hashValues)
        print(self.similarities)
        print("\n")

        return


    def hamming_distance(self, val1, val2):

        return bin(val1 ^ val2).count('1')



