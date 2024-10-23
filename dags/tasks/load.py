# load.py
import pandas as pd
from sqlalchemy import create_engine, text

def load(total_spent):
    # Kết nối đến cơ sở dữ liệu PostgreSQL
    engine = create_engine('postgresql://postgres:036899Nl##@192.168.1.95:5432/testdb')
    
    
    # Tạo bảng nếu nó chưa tồn tại
    with engine.connect() as connection:
        create_table_query = text("""
        CREATE TABLE IF NOT EXISTS results (
            total_spent FLOAT
        );
        """)
        connection.execute(create_table_query)

    # Chuyển đổi kết quả thành DataFrame
    result_df = pd.DataFrame({'total_spent': [total_spent]})
    
    # Lưu dữ liệu vào bảng Results
    result_df.to_sql('results', engine, if_exists='replace', index=False)
    print("Dữ liệu đã được load vào cơ sở dữ liệu thành công.")

if __name__ == "__main__":
    import transform
    import extract

    # Extract dữ liệu
    customers = extract.extract()
    
    # Transform dữ liệu
    total_spent = transform.transform(customers)
    
    # Load dữ liệu
    load(total_spent)

