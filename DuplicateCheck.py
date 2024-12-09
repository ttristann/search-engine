import hashlib
import re

class DuplicateCheck:
    
    def __init__(self):
        self.unique_links = set()
    
    def is_duplicate(self, url):
        # Remove fragment from URL
        url = url.split("#")[0]

        if url in self.unique_links:
            return True
        
    def is_low_contextual_value(soup_text, soup_tags):
        # tokenizes all of the text inside a html 
        text_tokens = re.findall(r'\b\w+\b', soup_text)

        # compares the lengths to determine if low context
        # if rate < 95% for text-to-HTML, it is low context
        total_length = len(text_tokens) + len(soup_tags)
        if total_length == 0:
            return True  # handle cases with no content by considering them low context

        text_ratio = len(text_tokens) / total_length
        html_ratio = len(soup_tags) / total_length

        # return true if the HTML or text ratio indicates low context
        return not (text_ratio > .95 or html_ratio > .95)

    def simhash(self, text):
        pass
