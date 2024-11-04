import json

def read_json_lines(data_filePath, save_json_file_name):
    data = []
    with open(data_filePath, 'r', encoding='utf-8') as file:
        for line in file:
            data.append(json.loads(line.strip()))
    
    with open(save_json_file_name, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

