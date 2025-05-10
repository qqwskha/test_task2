from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


def get_product_category_pairs(products_df: DataFrame, categories_df: DataFrame, relations_df: DataFrame) -> DataFrame:
    """
    Возвращает датафрейм с парами «Имя продукта – Имя категории»
    и именами всех продуктов, у которых нет категорий.

    :param products_df: Датафрейм продуктов (id, name)
    :param categories_df: Датафрейм категорий (id, name)
    :param relations_df: Датафрейм связей (product_id, category_id)
    :return: Датафрейм с результатом
    """
    # Соединяем продукты и связи
    products_with_relations = products_df.join(
        relations_df,
        products_df["id"] == relations_df["product_id"],
        "left"
    ).select(products_df["name"].alias("product_name"), relations_df["category_id"])

    # Соединяем с категориями
    product_category_pairs = products_with_relations.join(
        categories_df,
        products_with_relations["category_id"] == categories_df["id"],
        "left"
    ).select(
        col("product_name"),
        col("name").alias("category_name")
    )

    # Находим продукты без категорий
    products_without_categories = products_df.join(
        relations_df,
        products_df["id"] == relations_df["product_id"],
        "left_anti"
    ).select(
        col("name").alias("product_name"),
        col("name").alias("category_name")  # Категории нет, поэтому null
    ).withColumn("category_name", col("category_name").cast("string"))  # Приводим к строке

    # Объединяем результаты
    result_df = product_category_pairs.unionByName(products_without_categories)

    return result_df