# open-food-facts-import
This script imports data from the Open Food Facts Dataset (Data URL: https://www.kaggle.com/openfoodfacts/world-food-facts) to a PostgreSQL database.

# Run the Skript
In order to run the script, you have to download and extract the dataset available at https://www.kaggle.com/openfoodfacts/world-food-facts.
The script uses the `en.openfoodfacts.org.products.tsv` file from the dataset.

Create a database on your PostgreSQL server and  define the *database connection information* in `db_config.json`.

Afterwards you can run the `loader.py` to import the data to your Postgres database
```
python3 loader.py path/to/your/open-food-facts-products-file.tsv
```
