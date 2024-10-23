# transform.py
def transform(customers):
    total_spent = sum(customer['amount_spent'] for customer in customers)
    return total_spent

if __name__ == "__main__":
    import extract

    customers = extract.extract()
    total_spent = transform(customers)
    print(f"Tổng chi tiêu của khách hàng: {total_spent}")
