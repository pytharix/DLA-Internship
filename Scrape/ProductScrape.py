import time

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as bS
import requests
from selenium.webdriver.common.keys import Keys
import json

product_counter = 1
store_counter_ = 1

options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)

web_driver_main = webdriver.Chrome(options=options)


# Category Product
def find_categ_from_code(codd):
    if codd == '9360272':
        return 'Discount'
    elif codd == '11062415':
        return "Sarung Tinju"
    elif codd == '11062490':
        return "Perlengkapan Tinju"
    elif codd == '11062449':
        return "Pakaian Olahraga Pria"


# 3
def scrape(driver, wait_time, product_div, indicator_, link_, wait, ec):
    driver.get(link_)
    # Get initial list of names
    products = wait(
        driver,
        wait_time
    ).until(ec.presence_of_all_elements_located((indicator_, product_div)))

    while True:
        # Scroll down to last name in list
        driver.execute_script('arguments[0].scrollIntoView();', products[-1])
        try:
            # Wait for more names to be loaded
            wait(
                driver,
                wait_time
            ).until(
                lambda driver_: len(wait(
                    driver_,
                    wait_time
                ).until(
                    ec.presence_of_all_elements_located((indicator_, product_div))
                )) > len(products)
            )
            # Update names list
            products = wait(
                driver,
                wait_time
            ).until(ec.presence_of_all_elements_located((indicator_, product_div)))
        except:
            break

    # the_body = driver.find_element(By.XPATH, "/html/body")
    return products


# ID product
def make_product_id(numbb):
    lenght = len(str(numbb))
    if lenght == 1:
        return f"P000{numbb}"
    elif lenght == 2:
        return f"P00{numbb}"
    elif lenght == 3:
        return f"P0{numbb}"
    elif lenght == 4:
        return f"P{numbb}"


# Thousand format
def more_k_sol(list_):
    after_num = list_[-1]
    num = list_[0]

    indicator = after_num.split('+')[0]

    if indicator == 'rb':
        return num + '000'


# 4
def conversion(liss):
    products_liss = []
    for each in liss:
        products_liss.append(each.get_attribute("outerHTML"))
    return products_liss


# 5
def beautify(products_object, product_items, driver__, store_counterr):
    global product_counter
    for each_p in products_object:
        html_each_p = each_p
        clean_html_p = bS(html_each_p, 'lxml')
        # print(clean_html_p.div)

        header_html = {
            "User-Agent":
                "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"
        }

        product_link = clean_html_p.find(class_='css-1f2quy8').a['href']

        product_image = clean_html_p.find(class_='css-1q90pod')['src']

        #  ----------------------------------------
        failed = True
        while failed:
            try:
                driver__.get(product_link)
                failed = False
            except:
                print('failed...')
                time.sleep(1)

        # driver__.get(product_link)
        time.sleep(4)
        html_code = driver__.page_source
        #  ----------------------------------------

        product_name = clean_html_p.find(class_='prd_link-product-name css-3um8ox').get_text()
        print(product_name)
        product_price = int(clean_html_p.find(
            class_='prd_link-product-price css-1ksb19c'
        ).get_text().split(' ')[-1].replace('.', ''))

        try:
            product_sold = int(clean_html_p.find(
                class_='prd_label-integrity css-1duhs3e'
            ).get_text().split(' ')[-1].split('+')[0])

        except:
            product_sold = int(more_k_sol(
                clean_html_p.find(
                    class_='prd_label-integrity css-1duhs3e'
                ).get_text().split(' ')[1:]
            ))

        try:
            product_rate = float(clean_html_p.find(
                class_='prd_rating-average-text css-t70v7i'
            ).get_text())
        except:
            product_rate = 0.0

        trymax = True
        while trymax:
            try:
                product_html = requests.get(product_link, headers=header_html).text
                trymax = False
            except:
                print("max Try, Ln105")
                time.sleep(4)

        product_detail = bS(product_html, 'lxml')

        detail_html = bS(html_code, 'lxml')

        try:
            product_category = product_detail.find_all(
                class_='css-bwcbiv'
            )[2].get_text().split(':')[-1][1:]
        except:
            product_category = 'Unknown'

        try:
            product_review = int(detail_html.find(class_='css-bczdt6').find_all(
                'p'
            )[1].get_text().split('(')[-1].split(' ')[0].replace('.', ''))
        except:
            product_review = int(product_sold * 0.38)
            print('failed retrieve data')

        product_item = {
            "product-name": product_name,
            "product-price": product_price,
            "product-sold": product_sold,
            "product-rate": product_rate,
            "product-category": product_category,
            "product-review": product_review,
            "product-link": product_image,
            "product-id": make_product_id(product_counter),
            "store-id": "S0" + str(store_counterr)
        }

        product_counter += 1

        print(product_item)

        product_items.append(product_item)
        time.sleep(2)


def beautify_shopee(products_object, product_items, driver__, store_counterr, product_code):
    global product_counter

    header_html = {
        "User-Agent":
            "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"
    }

    for each_p in products_object:
        clean_html_p = bS(each_p, 'lxml')

        product_link = "https://shopee.co.id/" + clean_html_p.find(class_='shop-search-result-view__item').a['href']

        # Image Taker
        # -----------------------------------------------------------------------------

        test_ini = scrape(driver__, 2, 'MZ9yDd', By.CLASS_NAME, product_link, wait, EC)

        imagetaker = bS(test_ini[0].get_attribute("outerHTML"), 'lxml')

        test_pr = imagetaker.find(class_='A4dsoy')['style'].split('"')[1]

        product_image = test_pr

        # -----------------------------------------------------------------------------

        product_name = clean_html_p.find(class_='h0HBrE').get_text()

        product_price = clean_html_p.find(class_='_0ZJOIv').get_text().replace('.', '')

        try:
            product_sold = int(clean_html_p.find(class_='sPnnFI').get_text().split(' ')[0])
        except:
            product_sold = 0

        try:
            product_rate_ = scrape(driver__, 2, '_1k47d8', By.CLASS_NAME, product_link, wait, EC)
            product_rate__ = conversion(product_rate_)[0]
            product_rate = float(bS(product_rate__, 'lxml').get_text())
        except:
            product_rate = 0.0

        product_category = find_categ_from_code(product_code)

        try:
            product_review__ = conversion(product_rate_)[1]
            product_review = int(bS(product_review__, 'lxml').get_text())
        except:
            product_review = 0

        product_item = {
            "product-name": product_name,
            "product-price": product_price,
            "product-sold": product_sold,
            "product-rate": product_rate,
            "product-category": product_category,
            "product-review": product_review,
            "product-link": product_image,
            "product-id": make_product_id(product_counter),
            "store-id": "S0" + str(store_counterr)
        }

        product_counter += 1

        print(product_item)

        product_items.append(product_item)
        time.sleep(2)


# 1
def scrape_main(list_store, web_driver_):
    stores = []
    for each_in_store in list_store:

        wait_time_ = 2

        product_div_ = 'css-1sn1xa2'

        indicator = By.CLASS_NAME

        def scrapp_tokped():
            global store_counter_

            side_link = '/etalase/sold?sort=8'

            store_scr = False

            while not store_scr:
                try:
                    print('Scraping')
                    store_info = scrape_store(each_in_store + '/etalase/sold?sort=8', web_driver_)
                    print('store Success')
                    store_scr = True

                except Exception as kes:
                    print('failed Src, ', kes)
                    time.sleep(1)

            product_item_each_store = []
            real_link = each_in_store + side_link

            print(real_link)
            products = 0

            failed = True
            while failed:
                try:
                    products = scrape(web_driver_, wait_time_, product_div_, indicator, real_link, wait, EC)
                    failed = False
                except:
                    print('time Out...')
                    time.sleep(2)

            products_list = conversion(products)

            # web_driver_.close()

            beautify(products_list, product_item_each_store, web_driver_, store_counter_)

            # product_item_each_store = product_item_each_store + next_item

            print(len(product_item_each_store))

            store_info['products'] = product_item_each_store

            store_info['id-store'] = "S0" + str(store_counter_)

            store_counter_ += 1

            stores.append(store_info)

        scrapp_tokped()

        def scrapp_shopee():
            global store_counter_
            categ = [
                "9360272",  # Sedang Diskon
                "11062415",  # Sarung Tinju
                "11062490",  # Perlengkapan Tinju
                "11062449"  # Pakaian Olahraga Pria
            ]

            # categ = [
            #     "11062415"  # Sedang Diskon
            # ]

            shopee_link = 'https://shopee.co.id/adidascombatsports'

            store_scr = False

            while not store_scr:
                try:
                    print('Scraping')
                    shopee_store_info = scrape_store_shop(shopee_link, web_driver_)
                    print('store Success')
                    store_scr = True

                except Exception as kes:
                    print('failed Src, ', kes)
                    time.sleep(1)

            product_item_each_store_shopee = []

            for each_cat in categ:
                cat_link = 'https://shopee.co.id/adidascombatsports?page=0&shopCollection=' + each_cat

                failed = True
                while failed:
                    try:
                        products_shop = scrape(web_driver_, wait_time_, "shop-search-result-view__item", indicator,
                                               cat_link,
                                               wait, EC)
                        failed = False
                    except:
                        print('time Out...')
                        time.sleep(2)

                products_shop_con = conversion(products_shop)

                beautify_shopee(products_shop_con, product_item_each_store_shopee, web_driver_, store_counter_, each_cat)

                print(len(product_item_each_store_shopee))

            shopee_store_info['products'] = product_item_each_store_shopee

            shopee_store_info['id-store'] = "S0" + str(store_counter_)

            store_counter_ += 1

            stores.append(shopee_store_info)

        scrapp_shopee()

    json_data = {
        "data": {
            "stores": stores
        }
    }
    return json_data


# 2
def scrape_store(link_store_d, driver_):
    driver_.get(link_store_d)

    time.sleep(2)

    html_store = driver_.page_source

    bs_store = bS(html_store, 'lxml')

    name_store = bs_store.find(class_='css-1g675hl').get_text()

    rating_store = float(bs_store.find(class_='css-6x4cyu').p.get_text())

    mean_process_store = bs_store.find_all(class_='css-6x4cyu')[1].p.get_text().split('± ')[-1]

    link_store = '/'.join(link_store_d.split('/')[:4])

    # ---------------------------------------------------------------------------

    driver_.find_element(By.TAG_NAME, 'body').send_keys(Keys.CONTROL + Keys.HOME)

    time.sleep(2)

    buton_str_det = driver_.find_element(By.CLASS_NAME, 'css-edw0t8-unf-btn')

    buton_str_det.click()

    time.sleep(2)

    # ---------------------------------------------------------------------------

    location_store = driver_.find_element(By.CLASS_NAME, 'css-vni7t6-unf-heading').text

    stores_info = {
        "id-store": '',
        "name-store": 'Tokopedia',
        "rate-store": rating_store,
        "mean-process-store": mean_process_store,
        "link-store": "https://images.tokopedia.net/blog-tokopedia-com/uploads/2015/08/tokopedia.png",
        "location-store": location_store
    }

    return stores_info


def scrape_store_shop(link_store_d, driver_):
    driver_.get(link_store_d)

    time.sleep(2)
    login = True

    while login:
        try:
            driver_.find_element(By.NAME, "loginKey").send_keys("087789896668")
            driver_.find_element(By.NAME, "password").send_keys("Delapan8")
            login = False

        except:
            print("login failed")
            time.sleep(1)

    time.sleep(2)

    driver_.find_element(By.CLASS_NAME, "wyhvVD").click()

    time.sleep(4)

    html_store = driver_.page_source

    bs_store = bS(html_store, 'lxml')

    # print(bs_store)

    name_store = 'Shopee'

    rating_store = float(bs_store.find_all(
        class_='section-seller-overview__item-text-value'
    )[3].get_text().split(" ")[0])

    mean_process_store = bs_store.find_all(
        class_='section-seller-overview__item-text-value'
    )[4].get_text().split(" ")[0]

    stores_info = {
        "id-store": '',
        "name-store": name_store,
        "rate-store": rating_store,
        "mean-process-store": mean_process_store,
        "link-store": "https://upload.wikimedia.org/wikipedia/commons/thumb/f/fe/Shopee.svg/1200px-Shopee.svg.png",
        "location-store": "-"
    }

    return stores_info


store_list = [
    'https://www.tokopedia.com/adidas-combat'
]

stores_list = scrape_main(store_list, web_driver_main)

json_stores = json.dumps(stores_list)

with open("storesonly1_trial_230724_002.json", "w") as outfile:
    outfile.write(json_stores)
