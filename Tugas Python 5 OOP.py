import pandas as pd
import numpy as np

#Task 1

class MarketingDataETL:
    def __init__(self, file_name):
        self.file_name = file_name

    def extract(self):
        # Extract data from CSV file
        return pd.read_csv(self.fie_name)
    
class MarketingDataETL:
    def __init__(self, file_path):
        self.file_path = file_path

    def transform(self):
        try:
            data = pd.read_csv(self.file_path)
        except FileNotFoundError:
            print("File not found. Please provide the correct file path.")
            return None
       # Contoh transformasi sederhana: mengubah kolom 'tanggal' menjadi format datetime
        if 'tanggal' in data.columns:
            data['tanggal'] = pd.to_datetime(data['tanggal'], errors='coerce')

        # Menyimpan data yang telah ditransformasi ke dalam file baru
        transformed_file_path = "transformed_marketing_data.csv"
        data.to_csv(transformed_file_path, index=False)
        print(f"Transformed data saved to {transformed_file_path}")
        return transformed_file_path

# Contoh penggunaan
if __name__ == "__main__":
    etl = MarketingDataETL("marketing_data.csv")
    transformed_file_path = etl.transform()
    
def store(self, output_file):
        if self.data is not None:
            try:
                # Menyimpan data yang telah ditransformasi ke dalam struktur DataFrame
                self.data.to_csv(output_file, index=False)
                print("Data berhasil disimpan di", output_file)
                self.analyze_target_group()  # Memanggil method analyze_target_group
            except Exception as e:
                print("Terjadi kesalahan saat menyimpan data:", str(e))
        else:
            print("Data belum diekstrak dan ditransformasi. Lakukan ekstraksi dan transformasi terlebih dahulu.")

#Task 2
class TargetedMarketingETL(MarketingDataETL):
    def __init__(self, file_name, segment_column, segment_threshold):
        super().__init__(file_name)
        self.segment_column = segment_column
        self.segment_threshold = segment_threshold

    def segment_customers(self, data):
        # Segment customers based on given criteria
        data[self.segment_column + '_segment'] = data[self.segment_column] > self.segment_threshold
        return data
    
def transform(self, data):
        # Transform data and segment customers
        data = super().transform(data)
        data = self.segment_customers(data)
        return data
    
data = [
    {'customer_id': 1, 'name': 'John', 'purchase_history': 1500},
    {'customer_id': 2, 'name': 'Jane', 'purchase_history': 700},
    {'customer_id': 3, 'name': 'Doe', 'purchase_history': 300}
]

etl_process = SegmentedTargetedMarketingETL(data)
etl_process.transform()

for row in etl_process.data:
    print(row)
    
# Usage
etl = TargetedMarketingETL('marketing_data.csv', 'total_spending', 100)
data = etl.extract()
transformed_data = etl.transform(data)
etl.store(transformed_data)