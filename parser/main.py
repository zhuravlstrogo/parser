from parser.utils import YandexParser

if __name__ == '__main__':
    # id_ya = input("Введите ID яндекс компании: ")
    # parser = YandexParser(int(id_ya))
    # 1494695036

    parser = YandexParser(1494695036)

    reviews = parser.parse(type_parse='reviews')
    if 'company_reviews' in reviews.keys():
        print(len(reviews['company_reviews']))
    else:
        print("I failed :(")
