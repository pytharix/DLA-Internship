import pendulum
from airflow.decorators import dag, task


@dag(
    schedule="0 0 */1 * *",
    start_date=pendulum.datetime(2023, 6, 22, tz="Asia/Jakarta"),
    catchup=False,
    tags=["EL 1 Pipeline"]
)
def the_etl():
    @task
    def extract():
        print('extractor is running')  # Logger

        from EL_pipeline_resources.ELPytharix import Extractor, StaticClass

        extractor = Extractor()

        data_set = extractor.extract_from_json_scr_tok_shop('FileNeeded/scrapedHtml.json')

        data_set_rev = Extractor.extract_from_json_scr_rev_tok_shop('FileNeeded/scrapedRevHtml.json')

        data_set.update(data_set_rev)

        csv_path = extractor.to_csv(data_set)

        return csv_path

    @task
    def transform(path):
        from EL_pipeline_resources.ELPytharix import Transformer

        transformer = Transformer()

        data_set = transformer.from_csv(path)

        changer = {
            'Disc up to 50': 'Discount',
            'Voucher AC': 'Discount',
            'Semua Etalase': 'All',
            'Free Garnier Men': 'Discount'
        }

        transformed_data_set = transformer.change_cat(data_set, 'product', 'store', 'reviews', changer)

        new_path = transformer.to_csv(transformed_data_set)

        return new_path

    @task
    def loader(path):
        from EL_pipeline_resources.ELPytharix import Loader
        print('Loader is running')

        loader_driver = Loader(type_target='BQ')

        data_sets_driver = loader_driver.csv_to_dataset(path)

        result = loader_driver.load_to_bq(
            creat_new=True,
            data_set_id='DummyClothing',
            data_sets=data_sets_driver
        )

        return result

    task1 = extract()
    task2 = transform(task1)
    task3 = loader(task2)


the_etl()
