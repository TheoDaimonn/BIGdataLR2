
CREATE TABLE dim_country (
    country_key SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL
);

-- Измерения: Питомцы
CREATE TABLE dim_pet_category (
    pet_category_key SERIAL PRIMARY KEY,
    category_name VARCHAR(50) UNIQUE NOT NULL  
);

CREATE TABLE dim_pet (
    pet_key SERIAL PRIMARY KEY,
    pet_type VARCHAR(50),          
    pet_name VARCHAR(100),
    breed VARCHAR(100),            
    pet_category_key INTEGER REFERENCES dim_pet_category(pet_category_key),
    UNIQUE(pet_type, pet_name, breed) 
);

-- Покупатель
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
);

-- Продавец
CREATE TABLE dim_seller (
    seller_key SERIAL PRIMARY KEY,
    source_id INTEGER UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    postal_code VARCHAR(20),
    country_key INTEGER REFERENCES dim_country(country_key)
);

-- Товар
CREATE TABLE dim_product_category (
    category_key SERIAL PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE NOT NULL  -- Food, Cage, Accessory...
);

CREATE TABLE dim_product_brand (
    brand_key SERIAL PRIMARY KEY,
    brand_name VARCHAR(100) UNIQUE NOT NULL     
);

CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    source_id INTEGER UNIQUE,      
    product_name VARCHAR(255),
    category_key INTEGER REFERENCES dim_product_category(category_key),
    brand_key INTEGER REFERENCES dim_product_brand(brand_key),
    price NUMERIC(10,2),
    weight NUMERIC(8,2),
    color VARCHAR(50),
    size VARCHAR(20),               -- Small, Medium, Large
    material VARCHAR(100),
    description TEXT,
    rating NUMERIC(3,2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE
);

-- Магазин
CREATE TABLE dim_store (
    store_key SERIAL PRIMARY KEY,
    store_name VARCHAR(255) UNIQUE NOT NULL,
    location VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country_key INTEGER REFERENCES dim_country(country_key),
    phone VARCHAR(50),
    email VARCHAR(255)
);

-- Поставщик
CREATE TABLE dim_supplier (
    supplier_key SERIAL PRIMARY KEY,
    supplier_name VARCHAR(255) UNIQUE NOT NULL,
    contact VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    country_key INTEGER REFERENCES dim_country(country_key)
);

-- Измерение даты
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,  
    full_date DATE NOT NULL,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    quarter INTEGER,
    day_of_week INTEGER,          
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    is_weekend BOOLEAN
);

-- ФАКТ: Продажи
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
    
    sale_date_original DATE
);

CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_key);