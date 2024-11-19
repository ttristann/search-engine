import os
import json
"""
This class/function(s) should be iterating through all of the
entries inside each of the batch_output text files and merging
them to make the report. 

It should be a "dictionary" that is being updated with words
present in the corpus and then their associated values will
be updated in descending order based on the frequency count,
instead of making new entries for each word in the batch files.
"""
def report_creation(main_directory):
    """
    Iterates through the directory where the output batch 
    text files are created in and then parses each file
    to keep track of the unique tokens and the total 
    documents/urls that have been processed to create the
    inverted index. 
    """

    # initializers to be compiled when iterating
    unique_tokens = set()
    unique_docID = set()
    total_file_size = 0
    # iterating through the project directory
    for file in os.listdir(main_directory):
        if file.startswith("Output") and file.endswith(".txt"):
            file_path = os.path.join(main_directory, file)
            total_file_size += os.path.getsize(file_path)

            # opens the output batch text file 
            with open(file_path, "r", encoding="utf-8") as current_file:
                try:
                    content = json.load(current_file)

                    # iterate through each entry dictionary like data structure in the text file
                    for word, entries in content.items():
                        unique_tokens.add(word)

                        # iterate through the posting list to get the docID
                        for entry in entries:
                            current_docID = entry[0]
                            unique_docID.add(current_docID)

                except json.JSONDecodeError as e:
                    print(f"The error {e} has occured when processing {file}")

    total_file_sizeMB = total_file_size / 1000

    print(f"unique tokens: {len(unique_tokens)}")
    print(f"unique docID: {len(unique_docID)}")
    print(f"Total File Size: ~{total_file_sizeMB:.2f} MB")

    with open("report.txt", "w") as report:
        report.flush()
        report.write(f"Unique Tokens: {len(unique_tokens)}\n")
        report.write("----------------------------------------\n")
        report.write(f"Total Indexed Documents: {len(unique_docID)}\n")
        report.write("----------------------------------------\n")
        report.write(f"Total File Size: {total_file_sizeMB:.2f} KB")

if __name__ == "__main__":
    dir_path = "."
    report_creation(dir_path)
