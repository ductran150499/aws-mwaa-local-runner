# import json

# def read_json_lines(data_filePath, save_json_file_name):
#     data = []
#     with open(data_filePath, 'r', encoding='utf-8') as file:
#         for line in file:
#             data.append(json.loads(line.strip()))
    
#     with open(save_json_file_name, 'w', encoding='utf-8') as f:
#             json.dump(data, f, ensure_ascii=False, indent=4)


import json
import os
from tqdm import tqdm 

def readAndConsolidateJSONLines(folder_path, save_json_file_name, backup_file_name):
    data = []
    last_processed = {"file": None, "line": None}

    if os.path.exists(backup_file_name):
        with open(backup_file_name, 'r', encoding='utf-8') as backup_file:
            last_processed = json.load(backup_file)

    total_files = 16 
    total_lines = 0

    for i in range(total_files):
        file_path = os.path.join(folder_path, f"reviews_data_{i}.json")
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                total_lines += sum(1 for line in file)
        except FileNotFoundError:
            continue

    current_line = 0 
    pbar = tqdm(total=total_lines, desc="Processing JSON Lines")

    for i in range(total_files):
        data_file_path = os.path.join(folder_path, f"reviews_data_{i}.json")

        try:
            with open(data_file_path, 'r', encoding='utf-8') as file:
                line_num = 0
                for line in file:
                    line_num += 1
                    current_line += 1

                    if (data_file_path == last_processed["file"]) and (line_num <= last_processed["line"]):
                        continue
                    
                    data.append(json.loads(line.strip()))

                    last_processed = {"file": data_file_path, "line": line_num}
                    with open(backup_file_name, 'w', encoding='utf-8') as backup_file:
                        json.dump(last_processed, backup_file, ensure_ascii=False, indent=4)

                    pbar.update(1)

        except Exception as e:
            print(f"Error reading {data_file_path} at line {line_num}: {e}")
            break 

    pbar.close()  

    with open(save_json_file_name, 'w', encoding='utf-8') as output_file:
        json.dump(data, output_file, ensure_ascii=False, indent=4)


folder_path = "./data"  
readAndConsolidateJSONLines(folder_path, "consolidated_data.json", "backup_file.json")
