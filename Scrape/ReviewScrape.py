import time
from bs4 import BeautifulSoup as bS
from datetime import date, timedelta
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import json


def convert_str_to_date(date_):
    try:
        date_shown = date_.split(" ")
        many = int(date_shown[0])
        indicator = date_shown[1].lower()

        if indicator == "hari":
            indicator_int = 1
        elif indicator == "minggu":
            indicator_int = 7
        elif indicator == "bulan":
            indicator_int = 30
        else:
            indicator_int = 365

        day_different = many * indicator_int

    except:
        day_different = 365

    nows = date.today()
    real_date = str(nows - timedelta(days=day_different))

    return real_date


def conversion(liss):
    products_liss = []
    for each in liss:
        products_liss.append(each.get_attribute("outerHTML"))
    return products_liss


def scrape(driver, wait_time, product_div, indicator_, wait, ec):
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


def take_review_information_tp(html_format):
    beauty_html = bS(html_format, 'lxml')

    date_original = convert_str_to_date(beauty_html.find(class_="css-1dfgmtm-unf-heading").get_text())
    name_reviewer = beauty_html.find(class_="name").get_text()

    return {
        "date_time": date_original,
        "name_reviewer": name_reviewer,
        "link_image": "https://color-hex.org/colors/41b549.png",
        "id_store": "S01"
    }


def take_review_information_sp(html_format):
    beauty_html = bS(html_format, 'lxml')

    date_original = beauty_html.find(class_="shopee-product-rating__time").get_text().split(" ")[0]
    name_reviewer = beauty_html.find(class_="shopee-product-rating__author-name").get_text()

    return {
        "date_time": date_original,
        "name_reviewer": name_reviewer,
        "link_image": "https://www.colorbook.io/imagecreator.php?hex=f07f3f&width=1080&height=1920&text=%201080x1920",
        "id_store": "S02"
    }


def tokopedia_scrape(driver_sel, link_store):
    driver_sel.get(link_store)
    button_class = "css-16uzo3v-unf-pagination-item"
    reviews = []
    for each_page in range(0, 19):
        # Scrolling + Take Reviews
        not_success = True
        while not_success:
            try:
                all_reviews = scrape(driver_sel, 1, "css-ccpe8t", By.CLASS_NAME, wait, EC)
                not_success = False
            except:
                print("time Out")
                time.sleep(2)
        time.sleep(2)

        # Finding Button
        button = driver_sel.find_elements(By.CLASS_NAME, button_class)

        # Scraping
        converted_reviews = conversion(all_reviews)
        for each_review in converted_reviews:
            review_information = take_review_information_tp(each_review)
            reviews.append(review_information)
        # -----------------------------------------

        # Next Page
        button[1].click()
        time.sleep(2)

    return reviews


def shopee_scrape(driver_sel, link_store):
    driver_sel.get(link_store)
    time.sleep(4)
    button_class = "shopee-icon-button--right"
    reviews = []
    for each_page in range(0, 137):
        print(each_page)
        # Scrolling + Take Reviews
        not_success = True
        while not_success:
            try:
                all_reviews = scrape(driver_sel, 1, "shopee-product-rating", By.CLASS_NAME, wait, EC)[:6]
                not_success = False
            except:
                print("Time Out...")
                time.sleep(2)

        # Finding Button
        button = driver_sel.find_elements(By.CLASS_NAME, button_class)

        # Scraping
        converted_reviews = conversion(all_reviews)
        for each_review in converted_reviews:
            review_information = take_review_information_sp(each_review)
            reviews.append(review_information)
        # -----------------------------------------

        # Next Page
        button[0].click()
        time.sleep(2)

    return reviews


def main():
    product_counter = 1
    store_counter_ = 1

    options = webdriver.ChromeOptions()
    options.add_experimental_option("detach", True)

    web_driver_main = webdriver.Chrome(options=options)

    links = ["https://www.tokopedia.com/adidas-combat/review",
             "https://shopee.co.id/buyer/70696210/rating?shop_id=70694749"]

    tokopedia_reviews = tokopedia_scrape(web_driver_main, links[0])

    shopee_reviews = shopee_scrape(web_driver_main, links[1])

    all_reviews = tokopedia_reviews + shopee_reviews

    json_object = json.dumps(all_reviews)

    print(json_object)

    with open("storesonly1_trial_230725_002.json", "w") as outfile:
        outfile.write(json_object)


main()
