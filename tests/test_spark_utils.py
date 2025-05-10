import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.spark_utils import get_product_category_pairs


class TestSparkUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestApp") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_get_product_category_pairs(self):
        # Создаем тестовые данные
        products_data = [(1, "Product A"), (2, "Product B"), (3, "Product C")]
        categories_data = [(101, "Category X"), (102, "Category Y")]
        relations_data = [(1, 101), (1, 102), (2, 101)]

        products_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False)
        ])

        categories_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False)
        ])

        relations_schema = StructType([
            StructField("product_id", IntegerType(), nullable=False),
            StructField("category_id", IntegerType(), nullable=False)
        ])

        products_df = self.spark.createDataFrame(data=products_data, schema=products_schema)
        categories_df = self.spark.createDataFrame(data=categories_data, schema=categories_schema)
        relations_df = self.spark.createDataFrame(data=relations_data, schema=relations_schema)

        # Вызываем функцию
        result_df = get_product_category_pairs(products_df, categories_df, relations_df)

        # Ожидаемый результат
        expected_data = [
            ("Product A", "Category X"),
            ("Product A", "Category Y"),
            ("Product B", "Category X"),
            ("Product C", None)  # Продукт без категорий
        ]

        expected_df = self.spark.createDataFrame(expected_data, ["product_name", "category_name"])

        # Проверяем результат
        self.assertTrue(result_df.collect() == expected_df.collect())


if __name__ == "__main__":
    unittest.main()