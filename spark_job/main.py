from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

jdbc_url = "jdbc:postgresql://postgres:5432/petstore_analytics"
jdbc_props = {
    "user": "lab",
    "password": "lab123",
    "driver": "org.postgresql.Driver",
}

clickhouse_url = "jdbc:clickhouse://clickhouse:8123/analytics"
clickhouse_props = {
    "user": "default",
    "password": "",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def get_postgres_connection():
    import psycopg2

    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="petstore_analytics",
        user="lab",
        password="lab123",
    )


def create_tables(spark):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS fact_sales")
    cursor.execute("DROP TABLE IF EXISTS dim_date")
    cursor.execute("DROP TABLE IF EXISTS dim_supplier")
    cursor.execute("DROP TABLE IF EXISTS dim_store")
    cursor.execute("DROP TABLE IF EXISTS dim_product")
    cursor.execute("DROP TABLE IF EXISTS dim_product_brand")
    cursor.execute("DROP TABLE IF EXISTS dim_product_category")
    cursor.execute("DROP TABLE IF EXISTS dim_seller")
    cursor.execute("DROP TABLE IF EXISTS dim_customer")
    cursor.execute("DROP TABLE IF EXISTS dim_pet")
    cursor.execute("DROP TABLE IF EXISTS dim_pet_category")
    cursor.execute("DROP TABLE IF EXISTS dim_country")
    cursor.execute("DROP TABLE IF EXISTS mock_data")

    cursor.execute("""
        CREATE TABLE dim_country (
            country_key SERIAL PRIMARY KEY,
            country_name VARCHAR(100) UNIQUE NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE dim_pet_category (
            pet_category_key SERIAL PRIMARY KEY,
            category_name VARCHAR(50) UNIQUE NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE dim_pet (
            pet_key SERIAL PRIMARY KEY,
            pet_type VARCHAR(50),
            pet_name VARCHAR(100),
            breed VARCHAR(100),
            pet_category_key INTEGER REFERENCES dim_pet_category(pet_category_key),
            UNIQUE(pet_type, pet_name, breed)
        )
    """)
    cursor.execute("""
        CREATE TABLE dim_customer (
            customer_key SERIAL PRIMARY KEY,
            source_id INTEGER UNIQUE,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            age INTEGER,
            email VARCHAR(255),
            postal_code VARCHAR(20),
            country_key INTEGER REFERENCES dim_country(country_key),
            pet_key INTEGER REFERENCES dim_pet(pet_key)
        )
    """)
    cursor.execute("""
        CREATE TABLE dim_seller (
            seller_key SERIAL PRIMARY KEY,
            source_id INTEGER UNIQUE,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255),
            postal_code VARCHAR(20),
            country_key INTEGER REFERENCES dim_country(country_key)
        )
    """)
    cursor.execute("""
        CREATE TABLE dim_product_category (
            category_key SERIAL PRIMARY KEY,
            category_name VARCHAR(100) UNIQUE NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE dim_product_brand (
            brand_key SERIAL PRIMARY KEY,
            brand_name VARCHAR(100) UNIQUE NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE dim_product (
            product_key SERIAL PRIMARY KEY,
            source_id INTEGER UNIQUE,
            product_name VARCHAR(255),
            category_key INTEGER REFERENCES dim_product_category(category_key),
            brand_key INTEGER REFERENCES dim_product_brand(brand_key),
            price NUMERIC(10,2),
            weight NUMERIC(8,2),
            color VARCHAR(50),
            size VARCHAR(20),
            material VARCHAR(100),
            description TEXT,
            rating NUMERIC(3,2),
            reviews INTEGER,
            release_date VARCHAR(50),
            expiry_date VARCHAR(50)
        )
    """)
    cursor.execute("""
        CREATE TABLE dim_store (
            store_key SERIAL PRIMARY KEY,
            store_name VARCHAR(255) UNIQUE NOT NULL,
            location VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(100),
            country_key INTEGER REFERENCES dim_country(country_key),
            phone VARCHAR(50),
            email VARCHAR(255)
        )
    """)
    cursor.execute("""
        CREATE TABLE dim_supplier (
            supplier_key SERIAL PRIMARY KEY,
            supplier_name VARCHAR(255) UNIQUE NOT NULL,
            contact VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(50),
            address TEXT,
            city VARCHAR(100),
            country_key INTEGER REFERENCES dim_country(country_key)
        )
    """)
    cursor.execute("""
        CREATE TABLE dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date VARCHAR(50) NOT NULL,
            day INTEGER,
            month INTEGER,
            year INTEGER,
            quarter INTEGER,
            day_of_week INTEGER,
            day_name VARCHAR(20),
            month_name VARCHAR(20),
            is_weekend BOOLEAN
        )
    """)
    cursor.execute("""
        CREATE TABLE fact_sales (
            sale_key BIGSERIAL PRIMARY KEY,
            source_id INTEGER,
            date_key INTEGER REFERENCES dim_date(date_key),
            customer_key INTEGER REFERENCES dim_customer(customer_key),
            seller_key INTEGER REFERENCES dim_seller(seller_key),
            product_key INTEGER REFERENCES dim_product(product_key),
            store_key INTEGER REFERENCES dim_store(store_key),
            supplier_key INTEGER REFERENCES dim_supplier(supplier_key),
            quantity INTEGER NOT NULL,
            unit_price NUMERIC(10,2),
            total_price NUMERIC(12,2),
            sale_date_original VARCHAR(50)
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created!")


def load_mock_data(spark):
    print("Loading CSV data with Spark...")

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("customer_first_name", StringType(), True),
            StructField("customer_last_name", StringType(), True),
            StructField("customer_age", IntegerType(), True),
            StructField("customer_email", StringType(), True),
            StructField("customer_country", StringType(), True),
            StructField("customer_postal_code", StringType(), True),
            StructField("customer_pet_type", StringType(), True),
            StructField("customer_pet_name", StringType(), True),
            StructField("customer_pet_breed", StringType(), True),
            StructField("seller_first_name", StringType(), True),
            StructField("seller_last_name", StringType(), True),
            StructField("seller_email", StringType(), True),
            StructField("seller_country", StringType(), True),
            StructField("seller_postal_code", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("product_price", FloatType(), True),
            StructField("product_quantity", IntegerType(), True),
            StructField("sale_date", StringType(), True),
            StructField("sale_customer_id", IntegerType(), True),
            StructField("sale_seller_id", IntegerType(), True),
            StructField("sale_product_id", IntegerType(), True),
            StructField("sale_quantity", IntegerType(), True),
            StructField("sale_total_price", FloatType(), True),
            StructField("store_name", StringType(), True),
            StructField("store_location", StringType(), True),
            StructField("store_city", StringType(), True),
            StructField("store_state", StringType(), True),
            StructField("store_country", StringType(), True),
            StructField("store_phone", StringType(), True),
            StructField("store_email", StringType(), True),
            StructField("pet_category", StringType(), True),
            StructField("product_weight", FloatType(), True),
            StructField("product_color", StringType(), True),
            StructField("product_size", StringType(), True),
            StructField("product_brand", StringType(), True),
            StructField("product_material", StringType(), True),
            StructField("product_description", StringType(), True),
            StructField("product_rating", FloatType(), True),
            StructField("product_reviews", IntegerType(), True),
            StructField("product_release_date", StringType(), True),
            StructField("product_expiry_date", StringType(), True),
            StructField("supplier_name", StringType(), True),
            StructField("supplier_contact", StringType(), True),
            StructField("supplier_email", StringType(), True),
            StructField("supplier_phone", StringType(), True),
            StructField("supplier_address", StringType(), True),
            StructField("supplier_city", StringType(), True),
            StructField("supplier_country", StringType(), True),
        ]
    )

    df_list = []
    for i in range(1, 11):
        csv_path = f"/import_data/MOCK_DATA_{i}.csv"
        try:
            df = spark.read.csv(csv_path, header=True, schema=schema, nullValue="")
            df_list.append(df)
            print(f"Loaded {csv_path}")
        except Exception as e:
            print(f"Error loading {csv_path}: {e}")

    if df_list:
        combined_df = df_list[0]
        for df in df_list[1:]:
            combined_df = combined_df.union(df)

        combined_df = combined_df.filter(
            col("id").isNotNull()
            & (col("id") != 0)
            & (col("customer_first_name") != "customer_first_name")
        )

        print(f"Total rows: {combined_df.count()}")

        combined_df.write.jdbc(
            url=jdbc_url,
            table="mock_data",
            mode="overwrite",
            properties=jdbc_props,
        )
        print("Mock data loaded to PostgreSQL via Spark!")
    else:
        print("No CSV files found")


def write_df_to_postgres(df, table_name, mode="overwrite"):
    if df is None or df.head(1) == []:
        print(f"Warning: No data to write to {table_name}")
        return
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=jdbc_props,
    )


def write_df_to_clickhouse(df, table_name, mode="overwrite"):
    if df is None or df.head(1) == []:
        print(f"Warning: No data to write to ClickHouse {table_name}")
        return

    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host="clickhouse", port=8123, username="default", password=""
    )

    if mode == "overwrite":
        client.command(f"TRUNCATE TABLE analytics.{table_name}")

    pandas_df = df.toPandas()
    pandas_df = pandas_df.fillna(0)

    client.insert_df(f"analytics.{table_name}", pandas_df)
    print(f"Written to ClickHouse: {table_name} ({len(pandas_df)} rows)")

    client.close()


def insert_dim_country(spark, df):
    countries = (
        df.select("customer_country")
        .distinct()
        .union(df.select("seller_country").distinct())
        .union(df.select("store_country").distinct())
        .union(df.select("supplier_country").distinct())
        .withColumnRenamed("customer_country", "country_name")
        .na.drop(subset=["country_name"])
    ).distinct()

    write_df_to_postgres(countries, "dim_country", mode="append")

    country_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_country")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    country_map = {
        row.country_name: row.country_key
        for row in country_df.select("country_name", "country_key").collect()
    }
    return country_map


def insert_dim_pet_category(spark, df):
    pet_categories = (
        (df.select("pet_category").distinct().na.drop(subset=["pet_category"]))
        .distinct()
        .withColumnRenamed("pet_category", "category_name")
    )

    write_df_to_postgres(pet_categories, "dim_pet_category", mode="append")

    pet_cat_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_pet_category")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    pet_cat_map = {
        row.category_name: row.pet_category_key
        for row in pet_cat_df.select("category_name", "pet_category_key").collect()
    }
    return pet_cat_map


def insert_dim_pet(spark, df, pet_cat_map):
    pets = (
        df.select(
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
            "pet_category",
        )
        .groupBy("customer_pet_type", "customer_pet_name", "customer_pet_breed")
        .agg(
            first("pet_category").alias("pet_category"),
        )
        .na.drop(subset=["customer_pet_type"])
    )

    pets_df = pets.select(
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
    )

    write_df_to_postgres(
        pets_df.withColumnRenamed("customer_pet_type", "pet_type")
        .withColumnRenamed("customer_pet_name", "pet_name")
        .withColumnRenamed("customer_pet_breed", "breed"),
        "dim_pet",
        mode="append",
    )

    pet_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_pet")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    pet_map = {
        (row.pet_type, row.pet_name, row.breed): row.pet_key
        for row in pet_df.select("pet_key", "pet_type", "pet_name", "breed").collect()
    }
    return pet_map


def insert_dim_customer(spark, df, country_map, pet_map):
    customers = (
        df.select(
            "id",
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_postal_code",
        )
        .groupBy("id")
        .agg(
            first("customer_first_name").alias("customer_first_name"),
            first("customer_last_name").alias("customer_last_name"),
            first("customer_age").alias("customer_age"),
            first("customer_email").alias("customer_email"),
            first("customer_postal_code").alias("customer_postal_code"),
        )
    )

    customers_df = (
        customers.withColumnRenamed("id", "source_id")
        .withColumnRenamed("customer_first_name", "first_name")
        .withColumnRenamed("customer_last_name", "last_name")
        .withColumnRenamed("customer_age", "age")
        .withColumnRenamed("customer_email", "email")
        .withColumnRenamed("customer_postal_code", "postal_code")
    )

    write_df_to_postgres(customers_df, "dim_customer", mode="append")

    customer_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_customer")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    customer_map = {
        row.source_id: row.customer_key
        for row in customer_df.select("customer_key", "source_id").collect()
    }
    return customer_map


def insert_dim_seller(spark, df, country_map):
    sellers = (
        df.select(
            "sale_seller_id",
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_postal_code",
        )
        .groupBy("sale_seller_id")
        .agg(
            first("seller_first_name").alias("seller_first_name"),
            first("seller_last_name").alias("seller_last_name"),
            first("seller_email").alias("seller_email"),
            first("seller_postal_code").alias("seller_postal_code"),
        )
    )

    sellers_df = (
        sellers.withColumnRenamed("sale_seller_id", "source_id")
        .withColumnRenamed("seller_first_name", "first_name")
        .withColumnRenamed("seller_last_name", "last_name")
        .withColumnRenamed("seller_email", "email")
        .withColumnRenamed("seller_postal_code", "postal_code")
    )

    write_df_to_postgres(sellers_df, "dim_seller", mode="append")

    seller_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_seller")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    seller_map = {
        row.source_id: row.seller_key
        for row in seller_df.select("seller_key", "source_id").collect()
    }
    return seller_map


def insert_dim_product_category(spark, df):
    categories = (
        (df.select("product_category").distinct().na.drop(subset=["product_category"]))
        .distinct()
        .withColumnRenamed("product_category", "category_name")
    )

    write_df_to_postgres(categories, "dim_product_category", mode="append")

    cat_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_product_category")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    cat_map = {
        row.category_name: row.category_key
        for row in cat_df.select("category_key", "category_name").collect()
    }
    return cat_map


def insert_dim_product_brand(spark, df):
    brands = (
        (df.select("product_brand").distinct().na.drop(subset=["product_brand"]))
        .distinct()
        .withColumnRenamed("product_brand", "brand_name")
    )

    write_df_to_postgres(brands, "dim_product_brand", mode="append")

    brand_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_product_brand")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    brand_map = {
        row.brand_name: row.brand_key
        for row in brand_df.select("brand_key", "brand_name").collect()
    }
    return brand_map


def insert_dim_product(spark, df, cat_map, brand_map):
    products = (
        df.select(
            "sale_product_id",
            "product_name",
            "product_category",
            "product_brand",
            "product_price",
            "product_weight",
            "product_color",
            "product_size",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date",
        )
        .groupBy("sale_product_id")
        .agg(
            first("product_name").alias("product_name"),
            first("product_category").alias("product_category"),
            first("product_brand").alias("product_brand"),
            first("product_price").alias("product_price"),
            first("product_weight").alias("product_weight"),
            first("product_color").alias("product_color"),
            first("product_size").alias("product_size"),
            first("product_material").alias("product_material"),
            first("product_description").alias("product_description"),
            first("product_rating").alias("product_rating"),
            first("product_reviews").alias("product_reviews"),
            first("product_release_date").alias("product_release_date"),
            first("product_expiry_date").alias("product_expiry_date"),
        )
    )

    cat_df = spark.createDataFrame(
        [(k, v) for k, v in cat_map.items()], ["category_name", "category_key"]
    )
    brand_df = spark.createDataFrame(
        [(k, v) for k, v in brand_map.items()], ["brand_name", "brand_key"]
    )

    products_with_cat = products.join(
        cat_df, products.product_category == cat_df.category_name, "left"
    )
    products_with_all = products_with_cat.join(
        brand_df, products_with_cat.product_brand == brand_df.brand_name, "left"
    )

    products_df = (
        products_with_all.withColumnRenamed("sale_product_id", "source_id")
        .withColumnRenamed("product_name", "product_name")
        .withColumnRenamed("product_price", "price")
        .withColumnRenamed("product_weight", "weight")
        .withColumnRenamed("product_color", "color")
        .withColumnRenamed("product_size", "size")
        .withColumnRenamed("product_material", "material")
        .withColumnRenamed("product_description", "description")
        .withColumnRenamed("product_rating", "rating")
        .withColumnRenamed("product_reviews", "reviews")
        .withColumnRenamed("product_release_date", "release_date")
        .withColumnRenamed("product_expiry_date", "expiry_date")
    )[
        [
            "source_id",
            "product_name",
            "category_key",
            "brand_key",
            "price",
            "weight",
            "color",
            "size",
            "material",
            "description",
            "rating",
            "reviews",
            "release_date",
            "expiry_date",
        ]
    ]

    write_df_to_postgres(products_df, "dim_product", mode="append")

    product_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_product")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    product_map = {
        row.source_id: row.product_key
        for row in product_df.select("product_key", "source_id").collect()
    }
    return product_map


def insert_dim_store(spark, df, country_map):
    stores = (
        df.select(
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_phone",
            "store_email",
        )
        .groupBy("store_name")
        .agg(
            first("store_location").alias("store_location"),
            first("store_city").alias("store_city"),
            first("store_state").alias("store_state"),
            first("store_phone").alias("store_phone"),
            first("store_email").alias("store_email"),
        )
        .na.drop(subset=["store_name"])
    )

    stores_df = (
        stores.withColumnRenamed("store_location", "location")
        .withColumnRenamed("store_city", "city")
        .withColumnRenamed("store_state", "state")
        .withColumnRenamed("store_phone", "phone")
        .withColumnRenamed("store_email", "email")
    )

    write_df_to_postgres(stores_df, "dim_store", mode="append")

    store_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_store")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    store_map = {
        row.store_name: row.store_key
        for row in store_df.select("store_key", "store_name").collect()
    }
    return store_map


def insert_dim_supplier(spark, df, country_map):
    suppliers = (
        df.select(
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
        )
        .groupBy("supplier_name")
        .agg(
            first("supplier_contact").alias("supplier_contact"),
            first("supplier_email").alias("supplier_email"),
            first("supplier_phone").alias("supplier_phone"),
            first("supplier_address").alias("supplier_address"),
            first("supplier_city").alias("supplier_city"),
        )
        .na.drop(subset=["supplier_name"])
    )

    suppliers_df = (
        suppliers.withColumnRenamed("supplier_contact", "contact")
        .withColumnRenamed("supplier_email", "email")
        .withColumnRenamed("supplier_phone", "phone")
        .withColumnRenamed("supplier_address", "address")
        .withColumnRenamed("supplier_city", "city")
    )

    write_df_to_postgres(suppliers_df, "dim_supplier", mode="append")

    supplier_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_supplier")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    supplier_map = {
        row.supplier_name: row.supplier_key
        for row in supplier_df.select("supplier_key", "supplier_name").collect()
    }
    return supplier_map


def insert_dim_date(spark, df):
    dates = df.select("sale_date").distinct().na.drop(subset=["sale_date"])

    from datetime import datetime
    from pyspark.sql.functions import udf
    from pyspark.sql.types import ArrayType, StringType

    def parse_date(date_str):
        try:
            if "/" in str(date_str):
                parts = str(date_str).split("/")
                if len(parts) == 3:
                    parsed = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                else:
                    parsed = date_str
            else:
                parsed = date_str

            dt = datetime.strptime(parsed, "%Y-%m-%d")
            return [
                str(int(dt.strftime("%Y%m%d"))),
                parsed,
                str(dt.day),
                str(dt.month),
                str(dt.year),
                str((dt.month - 1) // 3 + 1),
                str(dt.weekday()),
                dt.strftime("%A"),
                dt.strftime("%B"),
                str(dt.weekday() >= 5),
            ]
        except:
            return None

    parse_date_udf = udf(parse_date, ArrayType(StringType()))

    dates_parsed = dates.withColumn("parsed", parse_date_udf(col("sale_date")))

    dates_df = dates_parsed.select(
        col("parsed")[0].cast("int").alias("date_key"),
        col("parsed")[1].alias("full_date"),
        col("parsed")[2].cast("int").alias("day"),
        col("parsed")[3].cast("int").alias("month"),
        col("parsed")[4].cast("int").alias("year"),
        col("parsed")[5].cast("int").alias("quarter"),
        col("parsed")[6].cast("int").alias("day_of_week"),
        col("parsed")[7].alias("day_name"),
        col("parsed")[8].alias("month_name"),
        col("parsed")[9].cast("Boolean").alias("is_weekend"),
    ).filter(col("date_key").isNotNull())

    write_df_to_postgres(dates_df, "dim_date", mode="append")


def insert_fact_sales(
    spark, df, customer_map, seller_map, product_map, store_map, supplier_map
):
    customer_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_customer")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    seller_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_seller")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    product_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_product")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    store_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_store")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    supplier_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_supplier")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    date_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_date")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    def parse_date_udf(date_str):
        from datetime import datetime

        try:
            if "/" in str(date_str):
                parts = str(date_str).split("/")
                if len(parts) == 3:
                    return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
            return str(date_str)
        except:
            return None

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    parse_date_str_udf = udf(parse_date_udf, StringType())

    sales_data = df.select(
        col("id").alias("source_id"),
        col("sale_date"),
        col("sale_quantity").alias("quantity"),
        col("product_price").alias("unit_price"),
        col("sale_total_price").alias("total_price"),
        col("sale_seller_id"),
        col("sale_product_id"),
        col("store_name"),
        col("supplier_name"),
    ).withColumn("parsed_date", parse_date_str_udf(col("sale_date")))

    sales_with_customer = sales_data.join(
        customer_df, sales_data.source_id == customer_df.source_id, "inner"
    ).select(
        sales_data.source_id,
        sales_data.parsed_date,
        sales_data.quantity,
        sales_data.unit_price,
        sales_data.total_price,
        sales_data.sale_seller_id,
        sales_data.sale_product_id,
        sales_data.store_name,
        sales_data.supplier_name,
        customer_df.customer_key,
    )

    sales_with_seller = sales_with_customer.join(
        seller_df, sales_with_customer.sale_seller_id == seller_df.source_id, "inner"
    ).select(
        sales_with_customer.source_id,
        sales_with_customer.parsed_date,
        sales_with_customer.quantity,
        sales_with_customer.unit_price,
        sales_with_customer.total_price,
        sales_with_customer.sale_product_id,
        sales_with_customer.store_name,
        sales_with_customer.supplier_name,
        sales_with_customer.customer_key,
        seller_df.seller_key,
    )

    sales_with_product = sales_with_seller.join(
        product_df, sales_with_seller.sale_product_id == product_df.source_id, "inner"
    ).select(
        sales_with_seller.source_id,
        sales_with_seller.parsed_date,
        sales_with_seller.quantity,
        sales_with_seller.unit_price,
        sales_with_seller.total_price,
        sales_with_seller.store_name,
        sales_with_seller.supplier_name,
        sales_with_seller.customer_key,
        sales_with_seller.seller_key,
        product_df.product_key,
    )

    sales_with_store = sales_with_product.join(
        store_df, sales_with_product.store_name == store_df.store_name, "inner"
    ).select(
        sales_with_product.source_id,
        sales_with_product.parsed_date,
        sales_with_product.quantity,
        sales_with_product.unit_price,
        sales_with_product.total_price,
        sales_with_product.supplier_name,
        sales_with_product.customer_key,
        sales_with_product.seller_key,
        sales_with_product.product_key,
        store_df.store_key,
    )

    sales_with_supplier = sales_with_store.join(
        supplier_df,
        sales_with_store.supplier_name == supplier_df.supplier_name,
        "inner",
    ).select(
        sales_with_store.source_id,
        sales_with_store.parsed_date,
        sales_with_store.quantity,
        sales_with_store.unit_price,
        sales_with_store.total_price,
        sales_with_store.customer_key,
        sales_with_store.seller_key,
        sales_with_store.product_key,
        sales_with_store.store_key,
        supplier_df.supplier_key,
    )

    sales_with_date = sales_with_supplier.join(
        date_df, sales_with_supplier.parsed_date == date_df.full_date, "inner"
    ).select(
        sales_with_supplier.source_id,
        date_df.date_key,
        sales_with_supplier.customer_key,
        sales_with_supplier.seller_key,
        sales_with_supplier.product_key,
        sales_with_supplier.store_key,
        sales_with_supplier.supplier_key,
        sales_with_supplier.quantity,
        sales_with_supplier.unit_price,
        sales_with_supplier.total_price,
    )

    fact_sales_df = sales_with_date.select(
        col("source_id"),
        col("date_key"),
        col("customer_key"),
        col("seller_key"),
        col("product_key"),
        col("store_key"),
        col("supplier_key"),
        col("quantity"),
        col("unit_price"),
        col("total_price"),
    )

    write_df_to_postgres(fact_sales_df, "fact_sales", mode="append")
    print(f"Fact sales inserted: {fact_sales_df.count()} rows")


def load_to_star_schema(spark):
    print("Step 1: Creating tables via Spark...")
    create_tables(spark)

    print("Step 2: Loading CSV data via Spark...")
    load_mock_data(spark)

    print("Step 3: Loading data from PostgreSQL...")
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "mock_data")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    print("Step 4: Inserting dimension: country")
    country_map = insert_dim_country(spark, df)

    print("Step 5: Inserting dimension: pet_category")
    pet_cat_map = insert_dim_pet_category(spark, df)

    print("Step 6: Inserting dimension: pet")
    pet_map = insert_dim_pet(spark, df, pet_cat_map)

    print("Step 7: Inserting dimension: customer")
    customer_map = insert_dim_customer(spark, df, country_map, pet_map)

    print("Step 8: Inserting dimension: seller")
    seller_map = insert_dim_seller(spark, df, country_map)

    print("Step 9: Inserting dimension: product_category")
    cat_map = insert_dim_product_category(spark, df)

    print("Step 10: Inserting dimension: product_brand")
    brand_map = insert_dim_product_brand(spark, df)

    print("Step 11: Inserting dimension: product")
    product_map = insert_dim_product(spark, df, cat_map, brand_map)

    print("Step 12: Inserting dimension: store")
    store_map = insert_dim_store(spark, df, country_map)

    print("Step 13: Inserting dimension: supplier")
    supplier_map = insert_dim_supplier(spark, df, country_map)

    print("Step 14: Inserting dimension: date")
    insert_dim_date(spark, df)

    print("Step 15: Inserting fact: sales")
    insert_fact_sales(
        spark, df, customer_map, seller_map, product_map, store_map, supplier_map
    )

    print("Star schema loading complete!")


def create_clickhouse_tables():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host="clickhouse", port=8123, username="default", password=""
    )

    client.command("CREATE DATABASE IF NOT EXISTS analytics")

    client.command("""
    CREATE TABLE IF NOT EXISTS analytics.sales_by_product (
        product_name String,
        category_name String,
        brand_name String,
        total_revenue Nullable(Float64),
        total_quantity Nullable(Int64),
        avg_rating Nullable(Float64),
        review_count Nullable(Int64)
    ) ENGINE = MergeTree()
    ORDER BY product_name
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS analytics.sales_by_customer (
        customer_key Nullable(Int32),
        first_name String,
        last_name String,
        country_name String,
        total_purchases Nullable(Float64),
        avg_order_value Nullable(Float64),
        order_count Nullable(Int32)
    ) ENGINE = MergeTree()
    ORDER BY customer_key
    """)
    client.command("""
    CREATE TABLE IF NOT EXISTS analytics.sales_by_time (
        year_month String,
        year Nullable(Int32),
        month Nullable(Int32),
        total_revenue Nullable(Float64),
        order_count Nullable(Int32),
        avg_order_value Nullable(Float64)
    ) ENGINE = MergeTree()
    ORDER BY (year, month)
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS analytics.sales_by_store (
        store_name String,
        city String,
        country_name String,
        total_revenue Nullable(Float64),
        order_count Nullable(Int32),
        avg_order_value Nullable(Float64)
    ) ENGINE = MergeTree()
    ORDER BY store_name
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS analytics.sales_by_supplier (
        supplier_name String,
        country_name String,
        total_revenue Nullable(Float64),
        product_count Nullable(Int32),
        avg_price Nullable(Float64)
    ) ENGINE = MergeTree()
    ORDER BY supplier_name
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS analytics.product_quality (
        product_name String,
        category_name String,
        rating Nullable(Float64),
        review_count Nullable(Int64),
        total_quantity_sold Nullable(Int64),
        total_revenue Nullable(Float64)
    ) ENGINE = MergeTree()
    ORDER BY product_name
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS analytics.sales_by_time (
        year_month String,
        year Int32,
        month Int32,
        total_revenue Float64,
        order_count Int32,
        avg_order_value Float64
    ) ENGINE = MergeTree()
    ORDER BY (year, month)
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS analytics.sales_by_store (
        store_name String,
        city String,
        country_name String,
        total_revenue Float64,
        order_count Int32,
        avg_order_value Float64
    ) ENGINE = MergeTree()
    ORDER BY store_name
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS analytics.sales_by_supplier (
        supplier_name String,
        country_name String,
        total_revenue Float64,
        product_count Int32,
        avg_price Float64
    ) ENGINE = MergeTree()
    ORDER BY supplier_name
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS analytics.product_quality (
        product_name String,
        category_name String,
        rating Float64,
        review_count Int64,
        total_quantity_sold Int64,
        total_revenue Float64
    ) ENGINE = MergeTree()
    ORDER BY rating
    """)

    client.close()
    print("ClickHouse tables created!")


def generate_reports(spark):
    print("Generating reports in ClickHouse...")

    fact_sales_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "fact_sales")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    dim_product_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_product")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    dim_product_category_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_product_category")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    dim_product_brand_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_product_brand")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    dim_customer_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_customer")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    dim_country_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_country")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    dim_date_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_date")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    dim_store_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_store")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    dim_supplier_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_supplier")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    print("Generating sales_by_product...")
    sales_by_product = (
        fact_sales_df.join(
            dim_product_df, fact_sales_df.product_key == dim_product_df.product_key
        )
        .join(
            dim_product_category_df,
            dim_product_df.category_key == dim_product_category_df.category_key,
        )
        .join(
            dim_product_brand_df,
            dim_product_df.brand_key == dim_product_brand_df.brand_key,
        )
        .groupBy(
            dim_product_df.product_name,
            dim_product_category_df.category_name,
            dim_product_brand_df.brand_name,
        )
        .agg(
            sum(fact_sales_df.total_price).alias("total_revenue"),
            sum(fact_sales_df.quantity).alias("total_quantity"),
            avg(dim_product_df.rating).alias("avg_rating"),
            max(dim_product_df.reviews).alias("review_count"),
        )
        .orderBy(desc("total_quantity"))
        .limit(10)
        .select(
            dim_product_df.product_name,
            dim_product_category_df.category_name,
            dim_product_brand_df.brand_name,
            col("total_revenue").cast("double"),
            col("total_quantity").cast("long"),
            col("avg_rating").cast("double"),
            col("review_count").cast("long"),
        )
    )
    write_df_to_clickhouse(sales_by_product, "sales_by_product", mode="overwrite")

    print("Generating sales_by_customer...")
    sales_by_customer = (
        fact_sales_df.join(
            dim_customer_df, fact_sales_df.customer_key == dim_customer_df.customer_key
        )
        .join(dim_country_df, dim_customer_df.country_key == dim_country_df.country_key)
        .groupBy(
            dim_customer_df.customer_key,
            dim_customer_df.first_name,
            dim_customer_df.last_name,
            dim_country_df.country_name,
        )
        .agg(
            sum(fact_sales_df.total_price).alias("total_purchases"),
            avg(fact_sales_df.total_price).alias("avg_order_value"),
            count(fact_sales_df.sale_key).alias("order_count"),
        )
        .orderBy(desc("total_purchases"))
        .limit(10)
        .select(
            dim_customer_df.customer_key.cast("int"),
            dim_customer_df.first_name,
            dim_customer_df.last_name,
            dim_country_df.country_name,
            col("total_purchases").cast("double"),
            col("avg_order_value").cast("double"),
            col("order_count").cast("int"),
        )
    )
    write_df_to_clickhouse(sales_by_customer, "sales_by_customer", mode="overwrite")

    print("Generating sales_by_time...")
    sales_by_time = (
        fact_sales_df.join(dim_date_df, fact_sales_df.date_key == dim_date_df.date_key)
        .groupBy(dim_date_df.month_name, dim_date_df.year, dim_date_df.month)
        .agg(
            sum(fact_sales_df.total_price).alias("total_revenue"),
            count(fact_sales_df.sale_key).alias("order_count"),
            avg(fact_sales_df.total_price).alias("avg_order_value"),
        )
        .withColumn(
            "year_month",
            concat(dim_date_df.month_name, lit(" "), dim_date_df.year.cast("string")),
        )
        .orderBy(dim_date_df.year, dim_date_df.month)
        .select(
            col("year_month"),
            dim_date_df.year.cast("int"),
            dim_date_df.month.cast("int"),
            col("total_revenue").cast("double"),
            col("order_count").cast("int"),
            col("avg_order_value").cast("double"),
        )
    )
    write_df_to_clickhouse(sales_by_time, "sales_by_time", mode="overwrite")

    print("Generating sales_by_store...")
    sales_by_store = (
        fact_sales_df.join(
            dim_store_df, fact_sales_df.store_key == dim_store_df.store_key
        )
        .join(dim_country_df, dim_store_df.country_key == dim_country_df.country_key)
        .groupBy(
            dim_store_df.store_name, dim_store_df.city, dim_country_df.country_name
        )
        .agg(
            sum(fact_sales_df.total_price).alias("total_revenue"),
            count(fact_sales_df.sale_key).alias("order_count"),
            avg(fact_sales_df.total_price).alias("avg_order_value"),
        )
        .orderBy(desc("total_revenue"))
        .limit(5)
        .select(
            dim_store_df.store_name,
            dim_store_df.city,
            dim_country_df.country_name,
            col("total_revenue").cast("double"),
            col("order_count").cast("int"),
            col("avg_order_value").cast("double"),
        )
    )
    write_df_to_clickhouse(sales_by_store, "sales_by_store", mode="overwrite")

    print("Generating sales_by_supplier...")
    sales_by_supplier = (
        fact_sales_df.join(
            dim_supplier_df, fact_sales_df.supplier_key == dim_supplier_df.supplier_key
        )
        .join(dim_product_df, fact_sales_df.product_key == dim_product_df.product_key)
        .join(dim_country_df, dim_supplier_df.country_key == dim_country_df.country_key)
        .groupBy(dim_supplier_df.supplier_name, dim_country_df.country_name)
        .agg(
            sum(fact_sales_df.total_price).alias("total_revenue"),
            count(dim_product_df.product_key).alias("product_count"),
            avg(dim_product_df.price).alias("avg_price"),
        )
        .orderBy(desc("total_revenue"))
        .limit(5)
        .select(
            dim_supplier_df.supplier_name,
            dim_country_df.country_name,
            col("total_revenue").cast("double"),
            col("product_count").cast("long"),
            col("avg_price").cast("double"),
        )
    )
    write_df_to_clickhouse(sales_by_supplier, "sales_by_supplier", mode="overwrite")

    print("Generating product_quality...")
    product_quality = (
        fact_sales_df.join(
            dim_product_df, fact_sales_df.product_key == dim_product_df.product_key
        )
        .join(
            dim_product_category_df,
            dim_product_df.category_key == dim_product_category_df.category_key,
        )
        .groupBy(
            dim_product_df.product_name,
            dim_product_category_df.category_name,
            dim_product_df.rating,
            dim_product_df.reviews,
        )
        .agg(
            sum(fact_sales_df.quantity).alias("total_quantity_sold"),
            sum(fact_sales_df.total_price).alias("total_revenue"),
        )
        .orderBy(desc(dim_product_df.rating))
        .select(
            dim_product_df.product_name,
            dim_product_category_df.category_name,
            dim_product_df.rating.cast("double"),
            dim_product_df.reviews.cast("long").alias("review_count"),
            col("total_quantity_sold").cast("long"),
            col("total_revenue").cast("double"),
        )
    )
    write_df_to_clickhouse(product_quality, "product_quality", mode="overwrite")

    print("Reports generated successfully!")


def main():
    spark = (
        SparkSession.builder.appName("PetStore ETL")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.4.6",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Step 1: Loading data to star schema in PostgreSQL...")
        load_to_star_schema(spark)

        print("\nStep 2: Creating ClickHouse tables...")
        create_clickhouse_tables()

        print("\nStep 3: Generating reports in ClickHouse...")
        generate_reports(spark)

        print("\n=== ETL Complete! ===")
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
