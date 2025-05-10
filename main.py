from pyspark.sql import SparkSession
from src.spark_utils import get_product_category_pairs
import os
import sys

# Вывод текущих переменных окружения
print("JAVA_HOME:", os.getenv("JAVA_HOME"))
print("PATH:", os.getenv("PATH"))

# Отладка PySpark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--verbose pyspark-shell'


def main():
    # Инициализация SparkSession
    spark = SparkSession.builder \
        .appName("ProductCategoryPairs") \
        .getOrCreate()

    # Пример данных
    products_data = [(1, "Product A"), (2, "Product B"), (3, "Product C")]
    categories_data = [(101, "Category X"), (102, "Category Y")]
    relations_data = [(1, 101), (1, 102), (2, 101)]

    products_df = spark.createDataFrame(products_data, ["id", "name"])
    categories_df = spark.createDataFrame(categories_data, ["id", "name"])
    relations_df = spark.createDataFrame(relations_data, ["product_id", "category_id"])

    # Получение результата
    result_df = get_product_category_pairs(products_df, categories_df, relations_df)

    # Вывод результата
    result_df.show()

    # Остановка SparkSession
    spark.stop()


if __name__ == "__main__":
    main()