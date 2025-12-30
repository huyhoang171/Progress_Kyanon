
# my_demo_dbt

Short description
-----------------

`my_demo_dbt` is a small example dbt project demonstrating a simple ELT workflow: load sample data from `seeds/`, perform light transformations in `models/staging/`, and build analytical tables (marts) in `models/marts/` (dimension & fact). The project is configured to run locally with DuckDB (`dev.duckdb`).

Goals
-----

- Demonstrate a basic dbt project structure and workflow.
- Practice `dbt seed`, `dbt run`, `dbt test`, and `dbt docs` locally.
- Validate data quality with tests (unique, not_null, relationships).

Key structure
-------------

- `dbt_project.yml`: dbt project configuration.
- `dev.duckdb`: local DuckDB file for development/testing.
- `seeds/`: sample CSV data loaded with `dbt seed`:
	- `raw_customers.csv`
	- `raw_orders.csv`
	- `raw_products.csv`
- `models/staging/`: staging models (e.g. `stg_orders.sql`) that clean and normalize seed data.
- `models/marts/`: marts (dimension & fact models): `dim_customers.sql`, `fact_sales.sql`.
- `models/marts/marts.yml`: model metadata and test declarations (unique, not_null, relationships).
- `tests/`: project-level or custom tests (e.g. `assert_total_amount_is_positive.sql`).
- `target/`: dbt artifacts and run output (compiled SQL, `run_results.json`, etc.).

Component details
-----------------

- Seeds: raw input data used to build models. Run `dbt seed` to load them into the local DuckDB.
- Staging: `models/staging/stg_orders.sql` normalizes and prepares order data for downstream models.
- Marts:
	- `dim_customers`: customer dimension; tests include `unique` and `not_null` on `customer_sk`.
	- `fact_sales`: sales fact table; tests include `unique`/`not_null` on `order_id`, `not_null` and `relationships` for `customer_sk` (references `dim_customers.customer_sk`), and `not_null` for `total_amount`.
- Tests: declared tests in `marts.yml` run with `dbt test`. Additional business tests live in `tests/`.

Quick start (local)
-------------------

Install dbt and the DuckDB adapter if you don't have them yet:

```bash
pip install --upgrade pip
pip install dbt-core dbt-duckdb
```

From the project directory:

```bash
cd my_demo_dbt
dbt seed        # load seed CSVs into DuckDB
dbt run         # build models
dbt test        # run declared tests
```

Generate and serve documentation:

```bash
dbt docs generate
dbt docs serve
```



