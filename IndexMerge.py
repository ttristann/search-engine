import os
import json

main_directory = "."

for file in os.listdir(main_directory):
    if file.startswith("batch") and file.endswith(".json"):
        file_path = os.path.join(main_directory, file)

        with open(file_path, 'r', encoding="utf-8") as current_file:
            try:
                content = json.load(current_file)  # Load JSON from batch file

                for term in content:
                    file_name = term + ".json"  # Fix the file name generation
                    term_file_path = os.path.join(main_directory, file_name)

                    if os.path.exists(term_file_path):
                        with open(term_file_path, 'r', encoding="utf-8") as json_file:
                            j_content = json.load(json_file)  # Corrected line
                    else:
                        j_content = {}  # If file doesn't exist, initialize as empty

                    current_posting = j_content.get(term, [])
                    new_posting = content.get(term, [])
                    combined_posting = current_posting + new_posting

                    # Update the JSON content
                    j_content[term] = combined_posting

                    # Save the updated JSON back to the file
                    with open(term_file_path, 'w', encoding="utf-8") as json_file:
                        json.dump(j_content, json_file, indent=4)

            except json.JSONDecodeError as e:
                print(f"The error {e} has occurred when processing {file_path}")
            except Exception as e:
                print(f"Unexpected error while processing {file_path}: {e}")
