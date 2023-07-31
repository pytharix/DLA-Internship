import re
import json
import petl as etl
from google.cloud import bigquery
from google.oauth2 import service_account
from EL_pipeline_resources.config import BQ_SC, BQ_TG


class StaticClass:
    @staticmethod
    def dummy_table():
        table1_cols = [
            'id_item', 'name_item', 'id_store', 'price_item', 'qty_sold',
            'rate_item', 'category_item', 'many_review', 'link_item'
        ]
        table1 = [
            [
                'id_item', 'name_item', 'id_store', 'price_item', 'qty_sold',
                'rate_item', 'category_item', 'many_review', 'link_item'
            ],  # Column Name

            ['A01', 'Adidas Hybrid 50 Boxing Glove - 6oz', 'S01',
             499000, 100, 4.9, 'Sarung Tinju', 83,
             'https://www.tokopedia.com/adidas-combat/adidas-hybrid-50-boxing-glove-6oz-merah?extParam=whid%3D2542495'],
            ['A02', 'Adidas Hybrid 80 Boxing Glove - Hitam Merah, 12oz', 'S01',
             499000, 100, 4.9, 'Sarung Tinju', 104,
             'https://www.tokopedia.com/adidas-combat/adidas-hybrid-80-boxing-glove-hitam-merah-12oz?extParam=whid%3D2542495'],
            ['A03', 'Bundle Adidas Karate Protector - S', 'S01',
             1650000, 100, 4.9, 'Perlengkapan Karate', 80,
             'https://www.tokopedia.com/adidas-combat/bundle-adidas-karate-protector-s?extParam=whid%3D2542495'],
            ['A04', 'Adidas Kids Boxing Kit', 'S01',
             795000, 30, 5, 'Sarung Tinju', 10,
             'https://www.tokopedia.com/adidas-combat/adidas-kids-boxing-kit?extParam=whid%3D2542495'],
            ['A05', 'Adidas Dobok Adistar Black vNeck - 150', 'S01',
             525000, 24, 5, 'Dobok Taekwondo', 13,
             'https://www.tokopedia.com/adidas-combat/adidas-hybrid-50-boxing-glove-6oz-merah?extParam=whid%3D2542495'],
            ['A06', 'Adidas Taekwondo Poomsae Male - 150', 'S01',
             1450000, 11, 5, 'Dobok Taekwondo', 7,
             'https://www.tokopedia.com/adidas-combat/adidas-taekwondo-poomsae-male-150?extParam=whid%3D2542495'],
            ['A07', 'Adidas Dobok Taekwondo Adifighter ECO - 150', 'S01',
             1700000, 15, 4.8, 'Dobok Taekwondo', 10,
             'https://www.tokopedia.com/adidas-combat/adidas-dobok-taekwondo-adifighter-eco-150?extParam=whid%3D2542495'],

            ['A08', 'Under Armour Sportsmask - 1368010-002 - XS/S', 'S02',
             199500, 250, 4.9, 'Accessories', 246,
             'https://www.tokopedia.com/underarmourid/under-armour-sportsmask-1368010-002-xs-s?extParam=whid%3D8616694'],
            ['A09', 'Under Armour Sportsmask - 1368010-013 - XS/S', 'S02',
             199500, 500, 4.9, 'Accessories', 337,
             'https://www.tokopedia.com/underarmourid/under-armour-sportsmask-1368010-013-xs-s?extParam=whid%3D8616694'],
            ['A10', 'Under Armour Training Vent 2.0 Ss - 1361426-100 - White,XL', 'S02',
             499000, 40, 4.5, 'Discount', 26,
             'https://www.tokopedia.com/underarmourid/under-armour-training-vent-2-0-ss-1361426-100-white-xl?extParam=whid%3D8616694'],
            ['A11', 'Under Armour Men Pjt Hm Gym Hwt Crew Tops - 1373571-110 - MD', 'S02',
             359700, 30, 5, 'Men Top', 8,
             'https://www.tokopedia.com/underarmourid/under-armour-men-pjt-hm-gym-hwt-crew-tops-1373571-110-md?extParam=whid%3D8616694'],
            ['A12', 'Under Armour Rush High Top - 1363485-781 - 38B', 'S02',
             317700, 40, 5, 'Women Top', 16,
             'https://www.tokopedia.com/underarmourid/under-armour-rush-high-top-1363485-781-38b?extParam=whid%3D8616694'],
            ['A13', 'Under Armour Women High Crossback BraZP Tops - 1355110-437 - 36C', 'S02',
             257700, 40, 4.9, 'Women Top', 13,
             'https://www.tokopedia.com/underarmourid/under-armour-women-high-crossback-brazp-tops-1355110-437-36c?extParam=whid%3D8616694'],
            ['A14', 'Under Armour Women UA Train CW Jacket Tops - 1373968-001 - SM', 'S02',
             359700, 15, 5, 'Women Top', 5,
             'https://www.tokopedia.com/underarmourid/under-armour-women-ua-train-cw-jacket-tops-1373968-001-sm?extParam=whid%3D8616694'],

            ['A15', 'LeviS 505 Regular Fit Timberwolf Sat Slub Wt - 28', 'S03',
             799900, 100, 4.9, 'Diskon', 144,
             'https://www.tokopedia.com/levis-official/levi-s-505-regular-fit-timberwolf-sat-slub-wt-28?extParam=whid%3D3788878'],
            ['A16', 'LeviS 512 Slim Taper Fit Native Cali - 29', 'S03',
             999900, 100, 4.9, 'Diskon', 109,
             'https://www.tokopedia.com/levis-official/levi-s-512-slim-taper-fit-native-cali-29?extParam=whid%3D3788878'],
            ['A17', 'LeviS 502 Regular Taper Native Cali - 28', 'S03',
             999900, 60, 4.9, 'Diskon', 38,
             'https://www.tokopedia.com/levis-official/levi-s-502-regular-taper-native-cali-28?extParam=whid%3D3788878'],
            ['A18', 'Levis Revel Shaping Skinny Hr Long Shot (74896-0010) - 24', 'S03',
             1012425, 90, 4.9, 'Diskon', 53,
             'https://www.tokopedia.com/levis-official/levi-s-revel-shaping-skinny-hr-long-shot-74896-0010-24?extParam=whid%3D3788878'],
            ['A19', 'Levis The Perfect Tee New Logo Chest Hit_Perf. (17369-1248) - XS', 'S03',
             193455, 80, 5, 'Women Tops', 48,
             'https://www.tokopedia.com/levis-official/levi-s-the-perfect-tee-new-logo-chest-hit-perf-17369-1248-xs?extParam=whid%3D3788878'],
            ['A20', 'Levis Graphic Jordie Tee Daisy Bw Outline Left Amber (A0458-0045) - S', 'S03',
             337425, 40, 4.7, 'Diskon', 26,
             'https://www.tokopedia.com/levis-official/levi-s-graphic-jordie-tee-daisy-bw-outline-left-amber-a0458-0045-s?extParam=whid%3D3788878'],
            ['A21', 'LeviS 501 Levis Original Fit Crispy Rinse - 27', 'S03',
             1299900, 100, 4.9, 'Diskon', 111,
             'https://www.tokopedia.com/levis-official/levi-s-501-levis-original-fit-crispy-rinse-27?extParam=whid%3D3788878'],

            ['A22', 'Deus Ex Machina - Wooblet 2 wallet - Cokelat', 'S04',
             425000, 250, 4.9, 'Accessories', 219,
             'https://www.tokopedia.com/deusvertical/deus-ex-machina-wooblet-2-wallet-cokelat?extParam=whid%3D8447343'],
            ['A23', 'DEUS - KIDS ADDRESS BALI - Putih, 2', 'S04',
             32500, 100, 5, 'Woman', 72,
             'https://www.tokopedia.com/deusvertical/deus-kids-address-bali-putih-2?extParam=whid%3D8447343'],
            ['A24', 'DEUS - BAYLAND TRUCKER - BLUE/RED - Biru', 'S04',
             385000, 200, 4.9, 'Accessories', 201,
             'https://www.tokopedia.com/deusvertical/deus-bayland-trucker-blue-red-biru?extParam=whid%3D8447343'],
            ['A25', 'DEUS - MORETOWN TRUCKER - DK BLUE', 'S04',
             385000, 100, 4.9, 'Accessories', 88,
             'https://www.tokopedia.com/deusvertical/deus-moretown-trucker-dk-blue?extParam=whid%3D8447343'],
            ['A26', 'DEUS - WOODEN PACKAGING', 'S04',
             50000, 16, 5, 'Accessories', 9,
             'https://www.tokopedia.com/deusvertical/deus-wooden-packaging?extParam=whid%3D8447343'],
            ['A27', 'DEUS - WOMENS ADDRESS BALI - Putih, XS', 'S04',
             425000, 29, 5, 'Woman', 18,
             'https://www.tokopedia.com/deusvertical/deus-womens-address-bali-putih-xs?extParam=whid%3D8447343'],
            ['A28', 'DEUS - LA WORKWEAR JACKET - Hitam, M', 'S04',
             1500000, 100, 5, 'Jacket', 74,
             'https://www.tokopedia.com/deusvertical/deus-la-workwear-jacket-hitam-m?extParam=whid%3D8447343']
        ]
        table1t_cols = ['id_store', 'name_store', 'rate_store', 'location_store', 'mean_process_store', 'link_store']
        table1t = [
            ['id_store', 'name_store', 'rate_store', 'location_store', 'mean_process_store', 'link_store'],
            ['S01', 'Adidas Combat Sports', 4.9, 'Jakarta Pusat', '8 Jam', 'https://www.tokopedia.com/adidas-combat'],
            ['S02', 'Under Armour Indonesia', 4.9, 'Kota Tangerang Selatan', '38 Menit',
             'https://www.tokopedia.com/underarmourid'],
            ['S03', 'Levis Official', 4.9, 'Kota Tangerang Selatan', '37 Menit',
             'https://www.tokopedia.com/levis-official'],
            ['S04', 'Deus Ex Machina Official', 4.9, 'Kab. Badung', '8 Jam', 'https://www.tokopedia.com/deusvertical']
        ]
        table_product = etl.cat(table1, header=table1_cols)
        table_store = etl.cat(table1t, header=table1t_cols)
        dataset_ = {'product': table_product, 'store': table_store}
        return dataset_

    @staticmethod
    def create_table_name(string_):
        string_list = string_.split('/')[3:]
        warm_name = ''
        for each in string_list:
            warm_name += each
        luke_table_name = re.findall(r'[a-zA-Z]+', warm_name)
        table_name = ''

        for each in luke_table_name:
            table_name += each

        return table_name

    @staticmethod
    def create_data_set(client, dataset_id):
        location = "US"

        dataset_ref = client.dataset(dataset_id)

        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        return

    @staticmethod
    def without_keys(d, keys):
        return {x: d[x] for x in d if x not in keys}


class Extractor:
    def __init__(self, type_extraction=None, dataset=None, table_name=None):
        #  Connection (type of Extraction)
        self.__bq_connection = None
        self.__api_url = None
        self.__data_set = dataset
        self.__table_name = table_name

        #  Type of Extraction to make the connection
        if type_extraction:
            self.__type_extraction = type_extraction.lower()
        else:
            self.__type_extraction = None

        #  Connection BQ
        if self.__type_extraction == "bq":
            self.__connect_bq()

    def __connect_bq(self):
        print('connecting Big Query')
        BQ_Sources = BQ_SC
        bq_detail = BQ_Sources['data']
        credentials = service_account.Credentials.from_service_account_file(
            bq_detail['location']
        )

        project_id = bq_detail['project_id']
        self.__bq_connection = bigquery.Client(credentials=credentials, project=project_id)
        print(self.__bq_connection.project)

    def extract_from_bq(self, table_name_source):
        if not table_name_source:
            print("all table")
            show_all_table = f"""
                        SELECT table_name 
                          FROM {self.__data_set}.INFORMATION_SCHEMA.TABLES
                        """

        else:
            print('specific table')
            show_all_table = f"""
                        SELECT table_name 
                          FROM {self.__data_set}.INFORMATION_SCHEMA.TABLES
                         WHERE table_name = '{table_name_source}'
                        """

        query_job = self.__bq_connection.query(show_all_table)

        result = list(query_job.result())

        data_tables = {}

        for each in result:
            table_name = each.get('table_name')

            table_id = f"{self.__data_set}.{table_name}"

            select_all = f"""
            SELECT * from `{table_id}`
            """
            select_all_query = self.__bq_connection.query(select_all)
            data_frame = select_all_query.result().to_dataframe()
            sourceDataSet = etl.fromdataframe(data_frame)
            data_tables[table_name] = sourceDataSet

        return data_tables

    @staticmethod
    def extract_from_json_scr_tok_shop(json_file_loc):
        jsonfile = open(json_file_loc)

        json_data = json.load(jsonfile)

        the_data = json_data['data']['stores']

        stores_only = []
        all_products = []
        for each_store in the_data:
            store_only = StaticClass.without_keys(each_store, {"products"})
            stores_only.append(store_only)
            products = each_store["products"]
            all_products += products

        stores_json_wanabe = f'''{json.dumps(stores_only)}'''
        products_json_wanabe = f'''{json.dumps(all_products)}'''

        with open("FileNeeded/only_stores.json", "w") as outfile:
            outfile.write(stores_json_wanabe)

        with open("FileNeeded/only_products.json", "w") as outfile:
            outfile.write(products_json_wanabe)

        stores_table = etl.fromjson("FileNeeded/only_stores.json",
                                    header=[
                                        'id-store',
                                        'name-store',
                                        'rate-store',
                                        'mean-process-store',
                                        'link-store',
                                        'location-store'
                                    ])
        products_table = etl.fromjson("FileNeeded/only_products.json",
                                      header=[
                                          "product-name",
                                          "product-price",
                                          "product-sold",
                                          "product-rate",
                                          "product-category",
                                          "product-review",
                                          "product-link",
                                          "product-id",
                                          "store-id"
                                      ])

        petl_table = {
            'product': products_table,
            'store': stores_table
        }

        return petl_table

    @staticmethod
    def extract_from_json_scr_rev_tok_shop(json_file_loc):
        jsonfile = open(json_file_loc)

        json_data = json.load(jsonfile)

        the_data = json_data

        reviews_json_wanabe = f'''{json.dumps(the_data)}'''

        with open("reviews.json", "w") as outfile:
            outfile.write(reviews_json_wanabe)

        reviews_table = etl.fromjson("reviews.json",
                                     header=[
                                         'date_time',
                                         'name_reviewer',
                                         'link_image',
                                         'id_store'
                                     ])

        return {'reviews': reviews_table}

    @staticmethod
    def to_csv(data_set):
        path_csv = 'FileNeeded/'
        list_of_csv = []
        for index, data in data_set.items():
            real_path = path_csv + index + '.csv'
            etl.tocsv(data, real_path)
            list_of_csv.append(real_path)
        return list_of_csv


class Loader:
    def __init__(self, type_target=None):
        self.__db_connection = None
        self.__bq_detail = None
        self.__bq_credentials = None

        if type_target:
            self.__type_target = type_target.lower()
        else:
            self.__type_target = None

        if self.__type_target == 'bq':
            self.__connect_bq()

    def __connect_bq(self):
        print('Connecting to BQ')
        self.__bq_detail = BQ_TG['data']
        credentials = service_account.Credentials.from_service_account_file(
            self.__bq_detail['location']
        )

        self.__bq_credentials = credentials

        project_id = self.__bq_detail['project_id']
        self.__bq_connection = bigquery.Client(credentials=credentials, project=project_id)

        print(self.__bq_connection)

    def load_to_bq(self, creat_new=False, data_set_id=None, data_sets: dict = None):
        datasets = self.__bq_connection.list_datasets()
        list_of_dataset = []
        for each_dataset in datasets:
            dataset = ("{}".format(each_dataset.dataset_id))
            list_of_dataset.append(dataset)

        dataset_id_registered = data_set_id in list_of_dataset

        if creat_new:
            if dataset_id_registered:
                print('Create new failed')
                return  # Exit
            print('Creating new')
            StaticClass.create_data_set(
                client=self.__bq_connection,
                dataset_id=data_set_id
            )

        else:
            if not dataset_id_registered:
                print('Override failed')
                return  # Exit
            print('Overriding')

        # Insert Dataset
        print(len(data_sets))
        for each_table, each_data in data_sets.items():
            converted_data_df = etl.todataframe(each_data)

            project_id = self.__bq_detail['project_id']
            table_id = each_table
            converted_data_df.to_gbq(
                destination_table=f'{project_id}.{data_set_id}.{table_id}',
                project_id=project_id,
                if_exists='replace',
                credentials=self.__bq_credentials
            )

            print(f"Table {each_table} inserted.")

        print("Process Load Success")

        return '-End- Process Load Success All -End-'

    @staticmethod
    def csv_to_dataset(list_path):
        data_set = {}
        print(list_path)
        for each_path in list_path:
            name_table = each_path.split('/')[1].split('.')[0]
            dataset = etl.fromcsv(each_path)
            data_set[name_table] = dataset

        return data_set


class Transformer:
    def __init__(self):
        pass

    @staticmethod
    def to_csv(data_set):
        path_csv = 'FileNeeded/'
        list_of_csv = []
        for index, data in data_set.items():
            real_path = path_csv + index + 'Transformed.csv'
            print(real_path)
            etl.tocsv(data, real_path)
            list_of_csv.append(real_path)
        return list_of_csv

    @staticmethod
    def from_csv(path):
        data_set = {}
        print(path)
        for each_path in path:
            name_table = each_path.split('/')[1].split('.')[0]
            dataset = etl.fromcsv(each_path)
            data_set[name_table] = dataset

        return data_set

    @staticmethod
    def change_cat(datas, product_table_name, stores_table_name, reviews_table_name, category_name: dict):
        products_table = datas[product_table_name]
        stores_table = datas[stores_table_name]
        reviews_table = datas[reviews_table_name]

        products_table = etl.convert(
            products_table,
            'product-category',
            category_name
        )

        products_table = etl.setheader(products_table, [
            "name_item",
            "price_item",
            "qty_sold",
            "rate_item",
            "category_item",
            "many_review",
            "link_item",
            "id_item",
            "id_store"
        ])

        products_table = etl.cut(
            products_table,
            'id_item', 'name_item', 'id_store', 'price_item', 'qty_sold',
            'rate_item', 'category_item', 'many_review', 'link_item'
        )

        stores_table = etl.setheader(stores_table, [
            'id_store',
            'name_store',
            'rate_store',
            'mean_process_store',
            'link_store',
            'location_store'
        ])

        stores_table = etl.cut(
            stores_table,
            'id_store', 'name_store', 'rate_store', 'location_store', 'mean_process_store', 'link_store')

        petl_table = {
            'product': products_table,
            'store': stores_table,
            'reviews': reviews_table
        }

        return petl_table
