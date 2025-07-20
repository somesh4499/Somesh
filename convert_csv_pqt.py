import sys
import argparse
from pyspark.sql import SparkSession

class CsvToParquetConverter:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.spark = SparkSession.builder.appName("CsvToParquet").getOrCreate()

    def convert(self):
        # Read CSV
        df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(self.input_path)
        # Write as Parquet
        df.write.mode("overwrite").parquet(self.output_path)

    def stop(self):
        self.spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Convert CSV to Parquet using PySpark")
    parser.add_argument("--input_path", required=True, help="Path to input CSV file or directory")
    parser.add_argument("--output_path", required=True, help="Path to output Parquet directory")
    args = parser.parse_args()

    converter = CsvToParquetConverter(args.input_path, args.output_path)
    converter.convert()
    converter.stop()

if __name__ == "__main__":
    main() #added commnt