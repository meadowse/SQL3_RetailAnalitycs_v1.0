--DROP DATABASE IF EXISTS RetailAnalitycs;
--CREATE DATABASE RetailAnalitycs;
DROP TABLE IF EXISTS personal_information Cascade;
CREATE TABLE IF NOT EXISTS personal_information (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR NOT NULL CHECK (customer_name ~ '^[A-ZА-Я][a-zа-я -]*$'),
    customer_surname VARCHAR NOT NULL CHECK (customer_surname ~ '^[A-ZА-Я][a-zа-я -]*$'),
    customer_primary_email VARCHAR NOT NULL UNIQUE CHECK (
        customer_primary_email ~ '^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$'
    ),
    customer_primary_phone VARCHAR NOT NULL UNIQUE CHECK (customer_primary_phone ~ '^[+]7[0-9]{10}$')
);
-- test
--insert into personal_information
--	(customer_name, customer_surname, customer_primary_email, customer_primary_phone)
--values ('Alex', 'Cqoch', 'a1@deonop.com', '+79226339281');
DROP TABLE IF EXISTS cards CASCADE;
CREATE TABLE IF NOT EXISTS cards (
    customer_card_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES personal_information(customer_id) ON
    UPDATE
        CASCADE ON DELETE CASCADE
);
-- Для формата даты, чтобы не возникало ошибок при импорте
SET
    datestyle = 'ISO,DMY';
DROP TABLE IF EXISTS transactions CASCADE;
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_card_id INT REFERENCES cards(customer_card_id) ON
    UPDATE
        CASCADE ON DELETE CASCADE,
        transaction_summ NUMERIC NOT NULL CHECK (transaction_summ >= 0),
        transaction_datetime TIMESTAMP NOT NULL,
        transaction_store_id INT NOT NULL
);
DROP TABLE IF EXISTS sku_group CASCADE;
CREATE TABLE IF NOT EXISTS sku_group (
    group_id SERIAL PRIMARY KEY,
    group_name VARCHAR NOT NULL UNIQUE CHECK (group_name ~ '^[[:graph:]]*$')
);
-- test
--insert into sku_group (group_name) values ('123abc');
DROP TABLE IF EXISTS product_grid CASCADE;
CREATE TABLE IF NOT EXISTS product_grid (
    sku_id SERIAL PRIMARY KEY,
    sku_name VARCHAR NOT NULL CHECK (sku_name ~ '^[[:print:]]*$'),
    group_id INT REFERENCES sku_group(group_id) ON
    UPDATE
        CASCADE ON DELETE CASCADE
);
DROP TABLE IF EXISTS checks CASCADE;
CREATE TABLE IF NOT EXISTS checks (
    transaction_id INT REFERENCES transactions(transaction_id) ON
    UPDATE
        CASCADE ON DELETE CASCADE,
        sku_id INT REFERENCES product_grid(sku_id) ON
    UPDATE
        CASCADE ON DELETE CASCADE,
        sku_amount NUMERIC NOT NULL CHECK (sku_amount >= 0),
        sku_summ NUMERIC NOT NULL CHECK (sku_summ >= 0),
        sku_summ_paid NUMERIC NOT NULL CHECK (sku_summ_paid >= 0),
        sku_discount NUMERIC NOT NULL CHECK (sku_discount >= 0)
);
DROP TABLE IF EXISTS stores CASCADE;
CREATE TABLE IF NOT EXISTS stores (
    transaction_store_id INT,
    sku_id BIGINT REFERENCES product_grid(sku_id) ON
    UPDATE
        CASCADE ON DELETE CASCADE,
        sku_purchase_price NUMERIC NOT NULL CHECK (sku_purchase_price >= 0),
        sku_retail_price NUMERIC NOT NULL CHECK (sku_retail_price >= 0)
);
DROP TABLE IF EXISTS date_of_analysis_formation CASCADE;
CREATE TABLE IF NOT EXISTS date_of_analysis_formation (analysis_formation TIMESTAMP);
-- Import
-- run the script 'prepare.sh' before
SET
    import_path.const TO '/tmp/import/';
DROP PROCEDURE IF EXISTS import_from_file;
CREATE
OR REPLACE PROCEDURE import_from_file(
    IN table_name VARCHAR,
    IN file_name VARCHAR,
    IN delimiter VARCHAR DEFAULT 'E''\t'''
) AS $$ BEGIN EXECUTE format(
    'COPY %s FROM %L WITH DELIMITER %s',
    $1,
    current_setting('import_path.const') || $2,
    $3
);
END;
$$ LANGUAGE plpgsql;
CALL import_from_file('personal_information', 'Personal_Data_Mini.tsv');
CALL import_from_file('cards', 'Cards_Mini.tsv');
CALL import_from_file('transactions', 'Transactions_Mini.tsv');
CALL import_from_file('sku_group', 'Groups_SKU_Mini.tsv');
CALL import_from_file('product_grid', 'SKU_Mini.tsv');
CALL import_from_file('checks', 'Checks_Mini.tsv');
CALL import_from_file('stores', 'Stores_Mini.tsv');
CALL import_from_file(
    'date_of_analysis_formation',
    'Date_Of_Analysis_Formation.tsv'
);
-- Для Windows
COPY personal_information
FROM
    'C:\datasets\Personal_Data_Mini.tsv' DELIMITER E '\t';
COPY cards
FROM
    'C:\datasets\Cards_Mini.tsv' DELIMITER E '\t';
COPY transactions
FROM
    'C:\datasets\Transactions_Mini.tsv' DELIMITER E '\t';
COPY sku_group
FROM
    'C:\datasets\Groups_SKU_Mini.tsv' DELIMITER E '\t';
COPY product_grid
FROM
    'C:\datasets\SKU_Mini.tsv' DELIMITER E '\t';
COPY checks
FROM
    'C:\datasets\Checks_Mini.tsv' DELIMITER E '\t';
COPY stores
FROM
    'C:\datasets\Stores_Mini.tsv' DELIMITER E '\t';
COPY date_of_analysis_formation
FROM
    'C:\datasets\Date_Of_Analysis_Formation.tsv' DELIMITER E '\t';
-- Export
SET
    export_path.const TO '/tmp/export/';
DROP PROCEDURE IF EXISTS export_to_file;
CREATE
OR REPLACE PROCEDURE export_to_file(
    IN table_name VARCHAR,
    IN file_name VARCHAR,
    IN delimeter VARCHAR DEFAULT 'E''\t'''
) LANGUAGE plpgsql AS $$ BEGIN EXECUTE format(
    'COPY %s TO %L WITH DELIMITER %s',
    $1,
    current_setting('export_path.const') || $2,
    $3
);
END;
$$;
CALL export_to_file('personal_information', 'Personal_Data_Mini.tsv');
CALL export_to_file('cards', 'Cards_Mini.tsv');
CALL export_to_file('transactions', 'Transactions_Mini.tsv');
CALL export_to_file('sku_group', 'Groups_SKU_Mini.tsv');
CALL export_to_file('product_grid', 'SKU_Mini.tsv');
CALL export_to_file('checks', 'Checks_Mini.tsv');
CALL export_to_file('stores', 'Stores_Mini.tsv');
CALL export_to_file(
    'data_of_analysis_formation',
    'Date_Of_Analysis_Formation.tsv'
);