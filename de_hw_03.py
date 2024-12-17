# %%
# 1. Імпорт бібліотек та створення SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round, expr

# Створення SparkSession
spark = SparkSession.builder \
    .appName("Product Purchase Analysis") \
    .getOrCreate()

# %%
# 2. Завантаження файлів 

# Шлях до файлів
file_path = "C:/Users/Elena/Desktop/MasterIT/DE/"

# Завантаження CSV-файлів
users_df = spark.read.csv(f"{file_path}users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv(f"{file_path}purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv(f"{file_path}products.csv", header=True, inferSchema=True)

# Перевірка завантажених даних
print("Users Data:")
users_df.show()

print("Purchases Data:")
purchases_df.show()

print("Products Data:")
products_df.show()

# %%
# 3. Очистка даних (видалення рядків з пропущеними значеннями)

users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# %%
# 4. Об'єднання таблиць для отримання всіх необхідних даних

# Об'єднання purchases_df з users_df по user_id
purchases_users_df = purchases_df.join(users_df, on="user_id", how="inner")

# Об'єднання з products_df по product_id
full_df = purchases_users_df.join(products_df, on="product_id", how="inner")

# Перевірка результату об'єднання
full_df.show()

# %%
# 5. Загальна сума покупок за категорією продуктів

total_sales_by_category = full_df.groupBy("category") \
    .agg(sum(col("quantity") * col("price")).alias("total_sales"))

total_sales_by_category.show()


# %%
# 6. Сума покупок для вікової категорії 18-25 за категорією продуктів

sales_18_25_by_category = full_df.filter((col("age") >= 18) & (col("age") <= 25)) \
    .groupBy("category") \
    .agg(sum(col("quantity") * col("price")).alias("total_sales_18_25"))

sales_18_25_by_category.show()


# %%
# 7. Частка покупок за категорією товарів для вікової категорії 18-25

# Загальна сума покупок для вікової категорії 18-25
total_18_25_sales = sales_18_25_by_category.agg(sum("total_sales_18_25").alias("total"))

# Обчислення відсоткової частки для кожної категорії
sales_percentage_18_25 = sales_18_25_by_category.crossJoin(total_18_25_sales) \
    .withColumn("percentage", round((col("total_sales_18_25") / col("total")) * 100, 2)) \
    .select("category", "percentage")

sales_percentage_18_25.show()

# %%
# 8. Три категорії з найвищим відсотком витрат

top_3_categories = sales_percentage_18_25.orderBy(col("percentage").desc()).limit(3)

top_3_categories.show()

# %%
# Зупинка SparkSession
spark.stop()
print("Spark-сесію зупинено.")
