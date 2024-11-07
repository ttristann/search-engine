import os
import json
import re
from collections import defaultdict
from bs4 import BeautifulSoup, Comment


"""
This is file is just to experiment on how to access directories inside the DEV directory
and iterate through all the folders and json files
"""

def getting_content(folder_path):
    """
    takes in a folder path to access the folder that
    contains many folders that are made of json files
    inside. 

    It initializes a dictionary to store the url
    and its content accordinly
    """
    allowed_tags = [
    "h1", "h2", "h3", "h4", "h5", "h6",  # Heading tags
    "p",                                 # Paragraph
    "span",                              # Span
    "a",                                 # Anchor
    "ul", "ol", "li",                    # List tags
    "div",                               # Div
    "header", "footer", "section", "article",  # Semantic structural tags
    "b", "strong", "i", "em",            # Bold and italic emphasis
    "blockquote",                        # Blockquote
    "code",                              # Code snippet
    "table", "tr", "td", "th",           # Table tags
    "figcaption",                        # Figure caption
    "details", "summary"                 # Collapsible content
    ]

    main_index = defaultdict()
    count = 0
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".json") and count <= 1:
                json_path = os.path.join(root, file)
                with open(json_path, 'r') as current_file:
                    data = json.load(current_file)
                    # main_index[data["url"]] = data["content"]

                    # using beautiful soup to parse the content
                    html_content = data.get("content")
                    soup_obj = BeautifulSoup(html_content, "html.parser")

                    for comment in soup_obj.find_all(text = lambda text: isinstance(text, Comment)):
                        comment.extract()

                    # removes all <script> and <style> tags
                    for tag_element in soup_obj.find_all(['script', 'style']):  
                        tag_element.extract()
                    

                    # gets the actual text inside the HTML file
                    raw_text = soup_obj.get_text(separator=" ", strip=True)
                    main_text = re.sub(r"[^A-Za-z0-9\s]+", "", raw_text)
                    

                    # for tag in soup.find_all(True):  # True finds all tags
                    #     if tag.name not in allowed_tags:
                    #         tag.decompose()  # Remove the tag and its content

                    # # Get all text and strip extra whitespace
                    # significant_text = soup.get_text(separator=" ", strip=True)
                    # print(f"SIGNIFICANT TEXT: {significant_text}")
                    main_index[data["url"]] = main_text
                    
                    count += 1

    return main_index


if __name__ == "__main__":
    

    folder_path = "/Users/tristangalang/Desktop/ICS/CS121/A3 - Search Engine/DEV"
    main_index = getting_content(folder_path)
    
    # Specify the output file path
    output_file_path = "filtered_output.txt"
    
    # Write the results to the output file
    with open(output_file_path, 'w') as output_file:
        for key, value in main_index.items():
            output_file.write(f'URL - {key} - has the content:\n\t{value}\n\n')
    
    print(f"Output written to {output_file_path}")