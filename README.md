# BigDataSpark - ETL с Apache Spark

## Реализовано

ETL-пайплайн: PostgreSQL (источник + звезда) -> Spark -> ClickHouse

**Примечание: долгая сборка**
## Таблицы PostgreSQL (звезда)

**Измерения:**
- dim_country - страны
- dim_pet_category, dim_pet - питомцы
- dim_customer - клиенты
- dim_seller - продавцы
- dim_product_category, dim_product_brand, dim_product - товары
- dim_store - магазины
- dim_supplier - поставщики
- dim_date - дата

**Факты:**
- fact_sales - продажи

## Отчеты ClickHouse (6 таблиц)

- sales_by_product  - Топ-10 продуктов, выручка по категориям
- sales_by_customer - Топ-10 клиентов, распределение по странам
- sales_by_time    - Месячные/годовые тренды
- sales_by_store   - Топ-5 магазинов, распределение по городам/странам
- sales_by_supplier - Топ-5 поставщиков
- product_quality  - Продукты по рейтингу

## Запуск

```bash
docker-compose up --build
```

## Просмотр результатов

**PostgreSQL:**
```bash
docker exec -it petstore_dw psql -U lab -d petstore_analytics -c "SELECT COUNT(*) FROM fact_sales;"
```

**ClickHouse:**
```bash
docker exec -it spark_clickhouse clickhouse-client --query "SELECT * FROM analytics.sales_by_product;"
docker exec -it spark_clickhouse clickhouse-client --query "SELECT * FROM analytics.sales_by_customer;"
docker exec -it spark_clickhouse clickhouse-client --query "SELECT * FROM analytics.sales_by_time;"
docker exec -it spark_clickhouse clickhouse-client --query "SELECT * FROM analytics.sales_by_store;"
docker exec -it spark_clickhouse clickhouse-client --query "SELECT * FROM analytics.sales_by_supplier;"
docker exec -it spark_clickhouse clickhouse-client --query "SELECT * FROM analytics.product_quality;"
```

## Структура

```
data/           - CSV файлы с данными
sql/            - DDL скрипты
spark_job/      - main.py (ETL скрипт)
docker-compose.yml
```
