# extract.py
import pandas as pd

def extract():
    # Tạo dữ liệu mẫu cho DataFrame
    data = {
        'customer_id': [1, 2, 3],
        'amount_spent': [100.0, 200.0, 150.0]
    }
    df = pd.DataFrame(data)
    return df.to_dict(orient='records')  # Trả về dạng list of dict

if __name__ == "__main__":
    customers = extract()
    print(customers)
