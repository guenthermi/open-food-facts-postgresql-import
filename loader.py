
import sys
import re
import json
import math
import psycopg2
import pandas as pd

HELP_TEXT = ('USAGE: \033[1mloader.py\033[0m input_file_path\n' +
             '\tinput_file_path: path to tsv file with open food facts products')

SCHEMA_FILE = 'schema_simple_ingredients.json'
DB_CONFIG_FILE = 'db_config.json'

SIMPLE_INGREDIENTS = True

RE_BRACKETS = re.compile('\([^(]*\)')
RE_CLEAR = re.compile('[*:&\[\]\(\)%]')


def connect_to_database(db_config_file):
    f = open(db_config_file, 'r')
    db_config = json.load(f)
    f.close()
    try:
        con = psycopg2.connect(
            "dbname='" + db_config['db_name'] + "' user='" +
            db_config['username'] + "' host='" + db_config['host'] +
            "' password='" + db_config['password'] + "'")
        return con, con.cursor(), db_config['batch_size']
    except:
        print('ERROR: Can not connect to database')
    return


def disable_triggers(schema_info, con, cur):
    for table_name in schema_info.keys():
        cur.execute('ALTER TABLE ' + table_name + ' DISABLE trigger ALL;')
        con.commit()
    return


def enable_triggers(schema_info, con, cur):
    for table_name in schema_info.keys():
        cur.execute('ALTER TABLE ' + table_name + ' ENABLE trigger ALL;')
        con.commit()
    return


def create_schema(schema_file, con, cur):
    query_new_tmpl = 'CREATE TABLE %s %s'
    query_drop_tmpl = 'DROP TABLE IF EXISTS %s'
    f = open(SCHEMA_FILE, 'r')
    schema_data = json.load(f)
    f.close()
    cur.execute(query_drop_tmpl % (', '.join(schema_data.keys()),))
    con.commit()
    for (table_name, columns) in schema_data.items():
        cur.execute(query_new_tmpl % (table_name, columns))
    con.commit()
    return schema_data


def get_db_value(raw_value):
    if type(raw_value) == float:
        if math.isnan(raw_value):
            return None
    return raw_value


def parse_list(values, sep=', ', lower=True, ONLY_CLEAR=True):
    tmp = ''.join(RE_BRACKETS.split(
        values.replace(') ', '), ').replace(' (', '(')))
    if lower:
        tmp = tmp.lower()
    if ONLY_CLEAR:
        if (RE_CLEAR.search(tmp)):
            return []
    return [s.lstrip().rstrip() for s in tmp.replace('.', '').split(sep)]


def get_db_literal(value):
    if value == None:
        return None
    else:
        return str(value)


def process_buffers(buffers, con, cur, batch_size):
    for buffer, query in buffers.values():
        if len(buffer) >= batch_size:
            cur.executemany(query, buffer)
            con.commit()
            buffer.clear()
    return


def flush_buffers(buffers, con, cur, batch_size):
    for buffer, query in buffers.values():
        cur.executemany(query, buffer)
        con.commit()
        buffer.clear()
    return


def insert_data_bucket(bucket, attrs, buffers, buffername, con, cur, batch_size):
    for elem in bucket:
        buffers[buffername][0].append(
            [tuple([get_db_literal(elem[attr]) for attr in attrs])])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)
    return


def insert_product_data_simple_ingredients(product_data, batch_size, con, cur):
    insert_query_tmpl = "INSERT INTO products (id, product_name, ingredients," \
                        " creator, category, energy_100g, sugars_100g, " \
                        "salt_100g, fat_100g, proteins_100g, " \
                        "carbohydrates_100g) VALUES %s"
    buffers = {'products': (list(), insert_query_tmpl)}
    insert_data_bucket(product_data, [
                       'id', 'product_name', 'ingredients', 'creator',
                       'category', 'energy_100g', 'sugars_100g', 'salt_100g',
                       'fat_100g', 'proteins_100g', 'carbohydrates_100g'],
                       buffers, 'products', con, cur, batch_size)
    return


def insert_product_data(product_data, batch_size, con, cur):
    insert_query_tmpl = "INSERT INTO products (id, product_name, creator, " \
                        "category, energy_100g, sugars_100g, salt_100g, " \
                        "fat_100g, proteins_100g, carbohydrates_100g) VALUES %s"
    buffers = {'products': (list(), insert_query_tmpl)}
    insert_data_bucket(product_data, [
                       'id', 'product_name', 'creator', 'category',
                       'energy_100g', 'sugars_100g', 'salt_100g', 'fat_100g',
                       'proteins_100g', 'carbohydrates_100g'], buffers,
                       'products', con, cur, batch_size)
    return


def insert_ingredients_data(ingredient_data, products_ingredients_data,
                            batch_size, con, cur):
    insert_ingredient_query_tmpl = "INSERT INTO ingredients (id, name) VALUES %s"
    insert_product_ingredient_query_tmpl = "INSERT INTO products_ingredients" \
                                           " (product_id, ingredient_id) " \
                                           "VALUES %s"
    buffers = {
        'ingredients': (list(), insert_ingredient_query_tmpl),
        'products_ingredients': (list(), insert_product_ingredient_query_tmpl)
    }
    insert_data_bucket(ingredient_data, [
                       'id', 'name'], buffers, 'ingredients', con, cur,
                       batch_size)
    insert_data_bucket(products_ingredients_data, [
                       'product_id', 'ingredient_id'], buffers,
                       'products_ingredients', con, cur, batch_size)
    return


def insert_countries_data(countries_data, products_countries_data, batch_size,
                          con, cur):
    insert_country_query_tmpl = "INSERT INTO countries (id, name) VALUES %s"
    insert_product_country_query_tmpl = "INSERT INTO products_countries (" \
                                        "product_id, country_id) VALUES %s"
    buffers = {
        'countries': (list(), insert_country_query_tmpl),
        'products_countries': (list(), insert_product_country_query_tmpl)
    }
    insert_data_bucket(
        countries_data, ['id', 'name'], buffers, 'countries', con, cur,
        batch_size)
    insert_data_bucket(products_countries_data, [
                       'product_id', 'country_id'], buffers,
                       'products_countries', con, cur, batch_size)
    return


def insert_brands_data(brands_data, products_brands_data, batch_size, con, cur):
    insert_brand_query_tmpl = "INSERT INTO brands (id, name) VALUES %s"
    insert_product_brand_query_tmpl = "INSERT INTO products_brands (" \
                                      "product_id, brand_id) VALUES %s"
    buffers = {
        'brands': (list(), insert_brand_query_tmpl),
        'products_brands': (list(), insert_product_brand_query_tmpl)
    }
    insert_data_bucket(brands_data, ['id', 'name'],
                       buffers, 'brands', con, cur, batch_size)
    insert_data_bucket(products_brands_data, [
                       'product_id', 'brand_id'], buffers, 'products_brands',
                       con, cur, batch_size)
    return


def insert_categories(categories_data, batch_size, con, cur):
    insert_category_query_tmpl = "INSERT INTO categories (id, name) VALUES %s"
    buffers = {
        'categories': (list(), insert_category_query_tmpl)
    }
    insert_data_bucket(categories_data, [
                       'id', 'name'], buffers, 'categories', con, cur, batch_size)
    return


def load_data(filename, max_size=1000000, ONLY_ENGLISH=True):
    RELEVANT_COLUMNS = ['creator', 'product_name', 'generic_name', 'packaging',
                        'packaging_tags', 'brands', 'brands_tags', 'categories',
                        'categories_tags', 'categories_en', 'origins',
                        'origins_tags', 'manufacturing_places',
                        'manufacturing_places_tags', 'labels', 'labels_tags',
                        'labels_en', 'cities', 'cities_tags', 'countries',
                        'countries_en', 'countries_tags', 'purchase_places',
                        'stores', 'ingredients_text', 'allergens',
                        'allergens_en', 'traces', 'traces_tags', 'traces_en',
                        'energy_100g', 'sugars_100g', 'salt_100g', 'fat_100g',
                        'proteins_100g', 'carbohydrates_100g',
                        'main_category_en']
    # some data seems to be redundand
    # (e.g. packaging and packaging_tags is often similar)
    dataset = pd.read_csv(filename, sep='\t')
    reduced = dataset[RELEVANT_COLUMNS]
    product_data = list()
    ingredients_data = set()
    product_ingredients_data = list()
    countries_data = set()
    product_brands_data = list()
    brands_data = set()
    product_countries_data = list()
    category_data = dict()
    count = 0
    for line in reduced.iterrows():

        countries_list_raw = get_db_value(line[1]['countries_en'])
        countries_list = (parse_list(countries_list_raw, sep=',', lower=False)
                          if countries_list_raw != None else [])
        if not 'United States' in countries_list:
            continue

        product_values = dict()
        product_values['id'] = len(product_data)
        product_values['product_name'] = get_db_value(line[1]['product_name'])
        product_values['creator'] = get_db_value(line[1]['creator'])
        product_values['energy_100g'] = get_db_value(line[1]['energy_100g'])
        product_values['sugars_100g'] = get_db_value(line[1]['sugars_100g'])
        product_values['salt_100g'] = get_db_value(line[1]['salt_100g'])
        product_values['fat_100g'] = get_db_value(line[1]['fat_100g'])
        product_values['carbohydrates_100g'] = get_db_value(
            line[1]['carbohydrates_100g'])
        product_values['proteins_100g'] = get_db_value(
            line[1]['proteins_100g'])
        if SIMPLE_INGREDIENTS:
            product_values['ingredients'] = get_db_value(
                line[1]['ingredients_text'])
        product_data.append(product_values)
        if not SIMPLE_INGREDIENTS:
            ingredients_list_raw = get_db_value(line[1]['ingredients_text'])
            ingredients_list = parse_list(ingredients_list_raw) if (
                ingredients_list_raw != None) else []
            ingredients_data.update(ingredients_list)
            product_ingredients_values = dict()
            product_ingredients_values['product_id'] = len(product_data) - 1
            product_ingredients_values['ingredients_list'] = ingredients_list
            product_ingredients_data.append(product_ingredients_values)

        countries_data.update(countries_list)
        product_countries_values = dict()
        product_countries_values['product_id'] = len(product_data) - 1
        product_countries_values['countries_list'] = countries_list
        product_countries_data.append(product_countries_values)

        brands_list_raw = get_db_value(line[1]['brands'])
        brands_list = parse_list(brands_list_raw, sep=',',
                                 lower=False) if brands_list_raw != None else []
        brands_data.update(brands_list)
        product_brands_values = dict()
        product_brands_values['product_id'] = len(product_data) - 1
        product_brands_values['brands_list'] = brands_list
        product_brands_data.append(product_brands_values)

        category_value = get_db_value(line[1]['main_category_en'])
        if category_value != None:
            if category_value not in category_data:
                category_data[category_value] = len(category_data)
            product_values['category'] = category_data[category_value]
        else:
            product_values['category'] = None
        count += 1
        if count > max_size:
            break
    # post process
    countries_data_list = list()
    countries_id_lookup = dict()
    product_countries_data_flat = list()
    for country in countries_data:
        countries_data_list.append(
            {'name': country, 'id': len(countries_data_list)})
        countries_id_lookup[country] = len(countries_data_list) - 1
    for elem in product_countries_data:
        for country in elem['countries_list']:
            product_countries_data_flat.append(
                {'product_id': elem['product_id'], 'country_id':
                 countries_id_lookup[country]})

    ingredients_data_list = list()
    ingredients_id_lookup = dict()
    product_ingredients_data_flat = list()
    if not SIMPLE_INGREDIENTS:
        for elem in ingredients_data:
            ingredients_data_list.append(
                {'name': elem, 'id': len(ingredients_data_list)})
            ingredients_id_lookup[elem] = len(ingredients_data_list) - 1
        for elem in product_ingredients_data:
            for ingredient in elem['ingredients_list']:
                product_ingredients_data_flat.append(
                    {'product_id': elem['product_id'], 'ingredient_id':
                     ingredients_id_lookup[ingredient]})

    brands_data_list = list()
    brands_id_lookup = dict()
    product_brands_data_flat = list()
    for brand in brands_data:
        brands_data_list.append({'name': brand, 'id': len(brands_data_list)})
        brands_id_lookup[brand] = len(brands_data_list) - 1
    for elem in product_brands_data:
        for brand in elem['brands_list']:
            product_brands_data_flat.append(
                {'product_id': elem['product_id'], 'brand_id':
                 brands_id_lookup[brand]})

    category_data_list = list()
    for key in category_data:
        id = category_data[key]
        category_data_list.append({'id': id, 'name': key})
    return (product_data, ingredients_data_list, product_ingredients_data_flat,
            countries_data_list, product_countries_data_flat, brands_data_list,
            product_brands_data_flat, category_data_list)


def main(argc, argv):

    if argc != 2:
        print(HELP_TEXT)
        return

    dataset_file_path = argv[1]

    db_config_file = DB_CONFIG_FILE
    con, cur, batch_size = connect_to_database(db_config_file)
    schema_data = create_schema(SCHEMA_FILE, con, cur)
    (products, ingredients, products_ingredients, countries, products_countries,
     brands, products_brands, categories) = load_data(dataset_file_path,
                                                      max_size=float('inf'))
    disable_triggers(schema_data, con, cur)
    if SIMPLE_INGREDIENTS:
        insert_product_data_simple_ingredients(products, batch_size, con, cur)
    else:
        insert_product_data(products, batch_size, con, cur)
        insert_ingredients_data(
            ingredients, products_ingredients, batch_size, con, cur)
    insert_countries_data(countries, products_countries, batch_size, con, cur)
    insert_brands_data(brands, products_brands, batch_size, con, cur)
    insert_categories(categories, batch_size, con, cur)
    enable_triggers(schema_data, con, cur)
    return


if __name__ == "__main__":
    main(len(sys.argv), sys.argv)
