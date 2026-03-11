from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2


def get_postgres_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="petstore_analytics",
        user="lab",
        password="lab123",
    )


def create_star_schema_tables():
    conn = get_postgres_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM fact_sales")
    cursor.execute("DELETE FROM dim_date")
    cursor.execute("DELETE FROM dim_supplier")
    cursor.execute("DELETE FROM dim_store")
    cursor.execute("DELETE FROM dim_product")
    cursor.execute("DELETE FROM dim_product_brand")
    cursor.execute("DELETE FROM dim_product_category")
    cursor.execute("DELETE FROM dim_seller")
    cursor.execute("DELETE FROM dim_customer")
    cursor.execute("DELETE FROM dim_pet")
    cursor.execute("DELETE FROM dim_pet_category")
    cursor.execute("DELETE FROM dim_country")

    conn.commit()
    cursor.close()
    conn.close()


def insert_dim_country(spark, df):
    countries = (
        df.select("customer_country")
        .distinct()
        .union(df.select("seller_country").distinct())
        .union(df.select("store_country").distinct())
        .union(df.select("supplier_country").distinct())
        .withColumnRenamed("customer_country", "country_name")
        .na.drop(subset=["country_name"])
    )

    countries = countries.distinct().collect()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    country_map = {}
    for row in countries:
        country_name = row.country_name
        if country_name:
            cursor.execute(
                "INSERT INTO dim_country (country_name) VALUES (%s) ON CONFLICT (country_name) DO NOTHING RETURNING country_key, country_name",
                (country_name,),
            )
            result = cursor.fetchone()
            if result:
                country_map[country_name] = result[0]

    conn.commit()
    cursor.close()
    conn.close()
    return country_map


def insert_dim_pet_category(spark, df):
    pet_categories = (
        df.select("pet_category").distinct().na.drop(subset=["pet_category"])
    )
    pet_categories = pet_categories.distinct().collect()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    pet_cat_map = {}
    for row in pet_categories:
        category_name = row.pet_category
        if category_name:
            cursor.execute(
                "INSERT INTO dim_pet_category (category_name) VALUES (%s) ON CONFLICT (category_name) DO NOTHING RETURNING pet_category_key, category_name",
                (category_name,),
            )
            result = cursor.fetchone()
            if result:
                pet_cat_map[category_name] = result[0]

    conn.commit()
    cursor.close()
    conn.close()
    return pet_cat_map


def insert_dim_pet(spark, df, pet_cat_map):
    pets = df.select(
        "customer_pet_type", "customer_pet_name", "customer_pet_breed", "pet_category"
    ).distinct()
    pets = pets.na.drop(subset=["customer_pet_type"])
    pets = pets.collect()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    pet_map = {}
    for row in pets:
        pet_type = row.customer_pet_type
        pet_name = row.customer_pet_name
        breed = row.customer_pet_breed
        pet_category = row.pet_category

        pet_category_key = pet_cat_map.get(pet_category) if pet_category else None

        cursor.execute(
            "INSERT INTO dim_pet (pet_type, pet_name, breed, pet_category_key) VALUES (%s, %s, %s, %s) ON CONFLICT (pet_type, pet_name, breed) DO NOTHING RETURNING pet_key, pet_type, pet_name, breed",
            (pet_type, pet_name, breed, pet_category_key),
        )
        result = cursor.fetchone()
        if result:
            key = (pet_type, pet_name, breed)
            pet_map[key] = result[0]

    conn.commit()
    cursor.close()
    conn.close()
    return pet_map


def insert_dim_customer(spark, df, country_map, pet_map):
    customers = df.select(
        "id",
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "customer_postal_code",
        "customer_country",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
    ).distinct()

    customers = customers.collect()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    customer_map = {}
    for row in customers:
        source_id = row.id
        first_name = row.customer_first_name
        last_name = row.customer_last_name
        age = row.customer_age
        email = row.customer_email
        postal_code = row.customer_postal_code
        country_name = row.customer_country
        pet_type = row.customer_pet_type
        pet_name = row.customer_pet_name
        breed = row.customer_pet_breed

        country_key = country_map.get(country_name)
        pet_key = pet_map.get((pet_type, pet_name, breed))

        cursor.execute(
            """
            INSERT INTO dim_customer (source_id, first_name, last_name, age, email, postal_code, country_key, pet_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_id) DO UPDATE SET first_name = EXCLUDED.first_name
            RETURNING customer_key, source_id
        """,
            (
                source_id,
                first_name,
                last_name,
                age,
                email,
                postal_code,
                country_key,
                pet_key,
            ),
        )
        result = cursor.fetchone()
        if result:
            customer_map[result[1]] = result[0]

    conn.commit()
    cursor.close()
    conn.close()
    return customer_map


def insert_dim_seller(spark, df, country_map):
    sellers = df.select(
        "sale_seller_id",
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_postal_code",
        "seller_country",
    ).distinct()

    sellers = sellers.collect()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    seller_map = {}
    for row in sellers:
        source_id = row.sale_seller_id
        first_name = row.seller_first_name
        last_name = row.seller_last_name
        email = row.seller_email
        postal_code = row.seller_postal_code
        country_name = row.seller_country

        country_key = country_map.get(country_name)

        cursor.execute(
            """
            INSERT INTO dim_seller (source_id, first_name, last_name, email, postal_code, country_key)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_id) DO UPDATE SET first_name = EXCLUDED.first_name
            RETURNING seller_key, source_id
        """,
            (source_id, first_name, last_name, email, postal_code, country_key),
        )
        result = cursor.fetchone()
        if result:
            seller_map[result[1]] = result[0]

    conn.commit()
    cursor.close()
    conn.close()
    return seller_map


def insert_dim_product_category(spark, df):
    categories = (
        df.select("product_category").distinct().na.drop(subset=["product_category"])
    )
    categories = categories.distinct().collect()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    cat_map = {}
    for row in categories:
        category_name = row.product_category
        if category_name:
            cursor.execute(
                "INSERT INTO dim_product_category (category_name) VALUES (%s) ON CONFLICT (category_name) DO NOTHING RETURNING category_key, category_name",
                (category_name,),
            )
            result = cursor.fetchone()
            if result:
                cat_map[category_name] = result[0]

    conn.commit()
    cursor.close()
    conn.close()
    return cat_map


def insert_dim_product_brand(spark, df):
    brands = df.select("product_brand").distinct().na.drop(subset=["product_brand"])
    brands = brands.distinct().collect()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    brand_map = {}
    for row in brands:
        brand_name = row.product_brand
        if brand_name:
            cursor.execute(
                "INSERT INTO dim_product_brand (brand_name) VALUES (%s) ON CONFLICT (brand_name) DO NOTHING RETURNING brand_key, brand_name",
                (brand_name,),
            )
            result = cursor.fetchone()
            if result:
                brand_map[brand_name] = result[0]

    conn.commit()
    cursor.close()
    conn.close()
    return brand_map


def insert_dim_product(spark, df, cat_map, brand_map):
    products = df.select(
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
    ).distinct()

    products = products.collect()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    product_map = {}
    for row in products:
        source_id = row.sale_product_id
        product_name = row.product_name
        category_name = row.product_category
        brand_name = row.product_brand
        price = row.product_price
        weight = row.product_weight
        color = row.product_color
        size = row.product_size
        material = row.product_material
        description = row.product_description
        rating = row.product_rating
        reviews = row.product_reviews
        release_date = row.product_release_date
        expiry_date = row.product_expiry_date

        category_key = cat_map.get(category_name)
        brand_key = brand_map.get(brand_name)

        cursor.execute(
            """
            INSERT INTO dim_product (source_id, product_name, category_key, brand_key, price, weight, color, size, material, description, rating, reviews, release_date, expiry_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_id) DO UPDATE SET product_name = EXCLUDED.product_name
            RETURNING product_key, source_id
        """,
            (
                source_id,
                product_name,
                category_key,
                brand_key,
                price,
                weight,
                color,
                size,
                material,
                description,
                rating,
                reviews,
                release_date,
                expiry_date,
            ),
        )
        result = cursor.fetchone()
        if result:
            product_map[result[1]] = result[0]

    conn.commit()
    cursor.close()
    conn.close()
    return product_map


def insert_dim_store(spark, df, country_map):
    stores = df.select(
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
    ).distinct()

    stores = stores.collect()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    store_map = {}
    for row in stores:
        store_name = row.store_name
        location = row.store_location
        city = row.store_city
        state = row.store_state
        country_name = row.store_country
        phone = row.store_phone
        email = row.store_email

        country_key = country_map.get(country_name)

        if store_name:
            cursor.execute(
                """
                INSERT INTO dim_store (store_name, location, city, state, country_key, phone, email)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (store_name) DO UPDATE SET location = EXCLUDED.location
                RETURNING store_key, store_name
            """,
                (store_name, location, city, state, country_key, phone, email),
            )
            result = cursor.fetchone()
            if result:
                store_map[result[1]] = result[0]

    conn.commit()
    cursor.close()
    conn.close()
    return store_map


def insert_dim_supplier(spark, df, country_map):
    suppliers = df.select(
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    ).distinct()

    suppliers = suppliers.collect()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    supplier_map = {}
    for row in suppliers:
        supplier_name = row.supplier_name
        contact = row.supplier_contact
        email = row.supplier_email
        phone = row.supplier_phone
        address = row.supplier_address
        city = row.supplier_city
        country_name = row.supplier_country

        country_key = country_map.get(country_name)

        if supplier_name:
            cursor.execute(
                """
                INSERT INTO dim_supplier (supplier_name, contact, email, phone, address, city, country_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (supplier_name) DO UPDATE SET contact = EXCLUDED.contact
                RETURNING supplier_key, supplier_name
            """,
                (supplier_name, contact, email, phone, address, city, country_key),
            )

            result = cursor.fetchone()
            if result:
                supplier_map[result[1]] = result[0]

    conn.commit()
    cursor.close()
    conn.close()
    return supplier_map


def insert_dim_date(spark, df):
    dates = df.select("sale_date").distinct().na.drop(subset=["sale_date"])
    dates = dates.collect()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    for row in dates:
        sale_date = row.sale_date
        try:
            parsed_date = sale_date
            if "/" in sale_date:
                parts = sale_date.split("/")
                if len(parts) == 3:
                    parsed_date = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"

            from datetime import datetime

            dt = datetime.strptime(parsed_date, "%Y-%m-%d")
            date_key = int(dt.strftime("%Y%m%d"))
            day = dt.day
            month = dt.month
            year = dt.year
            quarter = (month - 1) // 3 + 1
            day_of_week = dt.weekday()
            day_name = dt.strftime("%A")
            month_name = dt.strftime("%B")
            is_weekend = day_of_week >= 5

            cursor.execute(
                """
                INSERT INTO dim_date (date_key, full_date, day, month, year, quarter, day_of_week, day_name, month_name, is_weekend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_key) DO NOTHING
            """,
                (
                    date_key,
                    parsed_date,
                    day,
                    month,
                    year,
                    quarter,
                    day_of_week,
                    day_name,
                    month_name,
                    is_weekend,
                ),
            )
        except:
            pass

    conn.commit()
    cursor.close()
    conn.close()


def insert_fact_sales(
    spark, df, customer_map, seller_map, product_map, store_map, supplier_map
):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT date_key, full_date FROM dim_date")
    date_map = {str(row[1]): row[0] for row in cursor.fetchall()}

    sales = df.collect()
    inserted_count = 0
    error_count = 0

    for row in sales:
        source_id = row.id
        sale_date = row.sale_date
        customer_id = row.id  # use id, not sale_customer_id
        seller_id = row.sale_seller_id
        product_id = row.sale_product_id
        store_name = row.store_name
        supplier_name = row.supplier_name
        quantity = row.sale_quantity
        unit_price = row.product_price
        total_price = row.sale_total_price

        try:
            parsed_date = sale_date
            if sale_date and "/" in str(sale_date):
                parts = str(sale_date).split("/")
                if len(parts) == 3:
                    parsed_date = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"

            date_key = date_map.get(parsed_date)
            customer_key = customer_map.get(customer_id)
            seller_key = seller_map.get(seller_id)
            product_key = product_map.get(product_id)
            store_key = store_map.get(store_name)
            supplier_key = supplier_map.get(supplier_name)

            if date_key and customer_key and seller_key and product_key and store_key:
                cursor.execute(
                    """
                    INSERT INTO fact_sales (source_id, date_key, customer_key, seller_key, product_key, store_key, supplier_key, quantity, unit_price, total_price, sale_date_original)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        source_id,
                        date_key,
                        customer_key,
                        seller_key,
                        product_key,
                        store_key,
                        supplier_key,
                        quantity,
                        unit_price,
                        total_price,
                        parsed_date,
                    ),
                )
                inserted_count += 1
            else:
                error_count += 1
                if error_count <= 5:
                    print(
                        f"Missing keys for sale {source_id}: date={date_key}, customer={customer_key}, seller={seller_id}, product={product_id}, store={store_name}, supplier={supplier_name}"
                    )
        except Exception as e:
            error_count += 1
            if error_count <= 5:
                print(f"Error inserting sale {source_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Fact sales: inserted {inserted_count}, errors {error_count}")


def load_to_star_schema(spark):
    print("Loading data from PostgreSQL...")
    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/petstore_analytics")
        .option("dbtable", "mock_data")
        .option("user", "lab")
        .option("password", "lab123")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    print("Creating star schema tables...")
    create_star_schema_tables()

    print("Inserting dimension: country")
    country_map = insert_dim_country(spark, df)

    print("Inserting dimension: pet_category")
    pet_cat_map = insert_dim_pet_category(spark, df)

    print("Inserting dimension: pet")
    pet_map = insert_dim_pet(spark, df, pet_cat_map)

    print("Inserting dimension: customer")
    customer_map = insert_dim_customer(spark, df, country_map, pet_map)

    print("Inserting dimension: seller")
    seller_map = insert_dim_seller(spark, df, country_map)

    print("Inserting dimension: product_category")
    cat_map = insert_dim_product_category(spark, df)

    print("Inserting dimension: product_brand")
    brand_map = insert_dim_product_brand(spark, df)

    print("Inserting dimension: product")
    product_map = insert_dim_product(spark, df, cat_map, brand_map)

    print("Inserting dimension: store")
    store_map = insert_dim_store(spark, df, country_map)

    print("Inserting dimension: supplier")
    supplier_map = insert_dim_supplier(spark, df, country_map)

    print("Inserting dimension: date")
    insert_dim_date(spark, df)

    print("Inserting fact: sales")
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
        total_revenue Float64,
        total_quantity Int64,
        avg_rating Float64,
        review_count Int64
    ) ENGINE = MergeTree()
    ORDER BY product_name
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS analytics.sales_by_customer (
        customer_key Int32,
        first_name String,
        last_name String,
        country_name String,
        total_purchases Float64,
        avg_order_value Float64,
        order_count Int32
    ) ENGINE = MergeTree()
    ORDER BY customer_key
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

    conn = get_postgres_connection()
    cursor = conn.cursor()

    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host="clickhouse", port=8123, username="default", password=""
    )

    cursor.execute("""
        SELECT 
            p.product_name,
            pc.category_name,
            pb.brand_name,
            SUM(fs.total_price) as total_revenue,
            SUM(fs.quantity) as total_quantity,
            AVG(p.rating) as avg_rating,
            MAX(p.reviews) as review_count
        FROM fact_sales fs
        JOIN dim_product p ON fs.product_key = p.product_key
        JOIN dim_product_category pc ON p.category_key = pc.category_key
        JOIN dim_product_brand pb ON p.brand_key = pb.brand_key
        GROUP BY p.product_name, pc.category_name, pb.brand_name
        ORDER BY total_quantity DESC
        LIMIT 10
    """)

    product_data = cursor.fetchall()
    client.command("TRUNCATE TABLE analytics.sales_by_product")

    for row in product_data:
        values = ",".join(
            [
                f"'{str(v).replace(chr(39), chr(39) + chr(39))}'"
                if v is not None
                else "NULL"
                for v in row
            ]
        )
        client.command(
            f"INSERT INTO analytics.sales_by_product (product_name, category_name, brand_name, total_revenue, total_quantity, avg_rating, review_count) VALUES ({values})"
        )

    cursor.execute("""
        SELECT 
            c.customer_key,
            c.first_name,
            c.last_name,
            co.country_name,
            SUM(fs.total_price) as total_purchases,
            AVG(fs.total_price) as avg_order_value,
            COUNT(*) as order_count
        FROM fact_sales fs
        JOIN dim_customer c ON fs.customer_key = c.customer_key
        JOIN dim_country co ON c.country_key = co.country_key
        GROUP BY c.customer_key, c.first_name, c.last_name, co.country_name
        ORDER BY total_purchases DESC
        LIMIT 10
    """)

    customer_data = cursor.fetchall()
    client.command("TRUNCATE TABLE analytics.sales_by_customer")

    for row in customer_data:
        values = ",".join(
            [
                f"'{str(v).replace(chr(39), chr(39) + chr(39))}'"
                if v is not None
                else "NULL"
                for v in row
            ]
        )
        client.command(
            f"INSERT INTO analytics.sales_by_customer (customer_key, first_name, last_name, country_name, total_purchases, avg_order_value, order_count) VALUES ({values})"
        )

    cursor.execute("""
        SELECT 
            d.month_name || ' ' || d.year as year_month,
            d.year,
            d.month,
            SUM(fs.total_price) as total_revenue,
            COUNT(*) as order_count,
            AVG(fs.total_price) as avg_order_value
        FROM fact_sales fs
        JOIN dim_date d ON fs.date_key = d.date_key
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
    """)

    time_data = cursor.fetchall()
    client.command("TRUNCATE TABLE analytics.sales_by_time")

    for row in time_data:
        values = ",".join(
            [
                f"'{str(v).replace(chr(39), chr(39) + chr(39))}'"
                if v is not None
                else "NULL"
                for v in row
            ]
        )
        client.command(
            f"INSERT INTO analytics.sales_by_time (year_month, year, month, total_revenue, order_count, avg_order_value) VALUES ({values})"
        )

    cursor.execute("""
        SELECT 
            s.store_name,
            s.city,
            co.country_name,
            SUM(fs.total_price) as total_revenue,
            COUNT(*) as order_count,
            AVG(fs.total_price) as avg_order_value
        FROM fact_sales fs
        JOIN dim_store s ON fs.store_key = s.store_key
        JOIN dim_country co ON s.country_key = co.country_key
        GROUP BY s.store_name, s.city, co.country_name
        ORDER BY total_revenue DESC
        LIMIT 5
    """)

    store_data = cursor.fetchall()
    client.command("TRUNCATE TABLE analytics.sales_by_store")

    for row in store_data:
        values = ",".join(
            [
                f"'{str(v).replace(chr(39), chr(39) + chr(39))}'"
                if v is not None
                else "NULL"
                for v in row
            ]
        )
        client.command(
            f"INSERT INTO analytics.sales_by_store (store_name, city, country_name, total_revenue, order_count, avg_order_value) VALUES ({values})"
        )

    cursor.execute("""
        SELECT 
            sup.supplier_name,
            co.country_name,
            SUM(fs.total_price) as total_revenue,
            COUNT(DISTINCT p.product_key) as product_count,
            AVG(p.price) as avg_price
        FROM fact_sales fs
        JOIN dim_supplier sup ON fs.supplier_key = sup.supplier_key
        JOIN dim_product p ON fs.product_key = p.product_key
        JOIN dim_country co ON sup.country_key = co.country_key
        GROUP BY sup.supplier_name, co.country_name
        ORDER BY total_revenue DESC
        LIMIT 5
    """)

    supplier_data = cursor.fetchall()
    client.command("TRUNCATE TABLE analytics.sales_by_supplier")

    for row in supplier_data:
        values = ",".join(
            [
                f"'{str(v).replace(chr(39), chr(39) + chr(39))}'"
                if v is not None
                else "NULL"
                for v in row
            ]
        )
        client.command(
            f"INSERT INTO analytics.sales_by_supplier (supplier_name, country_name, total_revenue, product_count, avg_price) VALUES ({values})"
        )

    cursor.execute("""
        SELECT 
            p.product_name,
            pc.category_name,
            p.rating,
            p.reviews as review_count,
            SUM(fs.quantity) as total_quantity_sold,
            SUM(fs.total_price) as total_revenue
        FROM fact_sales fs
        JOIN dim_product p ON fs.product_key = p.product_key
        JOIN dim_product_category pc ON p.category_key = pc.category_key
        GROUP BY p.product_name, pc.category_name, p.rating, p.reviews
        ORDER BY p.rating DESC
    """)

    quality_data = cursor.fetchall()
    client.command("TRUNCATE TABLE analytics.product_quality")

    for row in quality_data:
        values = ",".join(
            [
                f"'{str(v).replace(chr(39), chr(39) + chr(39))}'"
                if v is not None
                else "NULL"
                for v in row
            ]
        )
        client.command(
            f"INSERT INTO analytics.product_quality (product_name, category_name, rating, review_count, total_quantity_sold, total_revenue) VALUES ({values})"
        )

    client.close()
    cursor.close()
    conn.close()

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
