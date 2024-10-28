import unittest
from unittest.mock import MagicMock
from dags.ggmaps.transform import remove_duplicates

class TestRemoveDuplicates(unittest.TestCase):

    def test_remove_duplicates(self):
        # Giả lập Task Instance
        ti = MagicMock()
        # Dữ liệu đầu vào có trùng lặp
        ti.xcom_pull.return_value = [
            ['ID_1', 'Type_1', 'Address_1', 'Place A'],
            ['ID_2', 'Type_2', 'Address_2', 'Place B'],
            ['ID_1', 'Type_1', 'Address_1', 'Place A'],  # Duplicate
            ['ID_3', 'Type_3', 'Address_3', 'Place C'],
            ['ID_2', 'Type_2', 'Address_2', 'Place B'],  # Duplicate
        ]

        # Gọi hàm remove_duplicates
        unique_entries = remove_duplicates(ti)

        # Kết quả mong đợi
        expected_unique_entries = [
            ['ID_1', 'Type_1', 'Address_1', 'Place A'],
            ['ID_2', 'Type_2', 'Address_2', 'Place B'],
            ['ID_3', 'Type_3', 'Address_3', 'Place C'],
        ]

        # Kiểm tra kết quả
        self.assertEqual(unique_entries, expected_unique_entries)

if __name__ == '__main__':
    unittest.main()