import time

import undetected_chromedriver
from parser.parsers import Parser


class YandexParser:
    def __init__(self, id_yandex: int):
        """
        @param id_yandex: ID Яндекс компании
        """
        self.id_yandex = id_yandex

    def __open_page(self):
        url: str = 'https://yandex.ru/maps/org/{}/reviews/'.format(str(self.id_yandex))
        opts = undetected_chromedriver.ChromeOptions()
        opts.add_argument('--no-sandbox')
        opts.add_argument('--disable-dev-shm-usage')
        opts.add_argument('headless')
        opts.add_argument('--disable-gpu')
        driver = undetected_chromedriver.Chrome(options=opts)
        parser = Parser(driver)
        driver.get(url)
        return parser

    def parse(self, type_parse: str = 'default', limit=False) -> dict:
        """
        Функция получения данных в виде
        @param type_parse: Тип данных, принимает значения:
            reviews - получает данные по отчетам
        @return: Данные по запрошенному типу
        """
        result:dict = {}
        page = self.__open_page()
        time.sleep(4)
        try:
            if type_parse == 'reviews':
                result = page.parse_reviews(limit=limit)
        except Exception as e:
            print(e)
            return result
        finally:
            page.driver.close()
            page.driver.quit()
            return result
