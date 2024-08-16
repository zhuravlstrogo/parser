Код написан под Ubuntu, MacOS. 
Для работы на сервере нужен [Chrome version 114](https://mirror.cs.uchicago.edu/google-chrome/pool/main/g/google-chrome-stable/) и библиотеки из requirements.txt. 
Примерные шаги установки описаны в run.sh.
Также для работы потребуется token [API Поиска по организациям яндекса](https://developer.tech.yandex.ru/services/12) и список городов для парсинга в файле cities.txt.

Цель: сбор данных по присутствию банкоматов различных банков во всех городах РФ для выстраивания аналитики с опорой на данные по расположению конкурентов в конкретном населенном пункте.
Уточнение условий: в приоритете парсинг данных по банкоматам трех банков: втб, альфа-банк, сбербанк
Исходные данные: данные Яндекс Карт по банкоматам
Пример: https://yandex.ru/maps/org/sberbank/1174127313/?ll=35.897022%2C56.868163&z=16.76\
Инструменты: API Яндекс Карт, среда разработки Jupyter Notebook
Реализованная механика:
По API Яндекс карт получение данных по одному банкомату одного выбранного банка в городе. Список городов передается через txt файл.

Ограничения:

Лимиты: В API Яндекс карт установлен дневной лимит на кол-во запросов – 500 штук.

Города без банкоматов банка: В случае, если банкомата в городе нет, Яндекс отдает данные по банкоматам соседнего города. Задача: проверять соответствие адреса банкомата и города, в котором происходит поиск банкоматов. Проверка на вхождение города в адрес пропускает ошибки содержания наименования города в любом месте адреса (совпадение названия улицы и города поиска).

Результат главной функции данного этапа: сформированный pickle файл со словарем, содержащим наименование города из передаваемого списка городов и yandex_id первого найденного банкомата.

Пример содержания pickle файла:
![alt text](https://github.com/zhuravlstrogo/parser/blob/main/img/dict.png)


По данным из pickle файла с помощью библиотеки Selenium собирается весь список банкоматов, представленных в городе из раздела chain на сайте Яндекс Карт, url = f'https://yandex.ru/maps/org/{bank_name}/{yndx_bank_id}/chain/', результатом главной функции является сохранение данных в pickle файл в виде ссылок на каждый отдельный банкомат в городе в разрезе города и банкомата.

Пример содержания pickle файла:
![alt text](https://github.com/zhuravlstrogo/parser/blob/main/img/links.png)

Парсинг данных по банкоматам с использованием библиотеки для парсинга HTML и XML документов BeautifulSoup по найденным на предыдущем шаге ссылкам на банкоматы. Сохранение распарсенных данных в csv файл. 

Пример вывода данных после парсинга::
![alt text](https://github.com/zhuravlstrogo/parser/blob/main/img/df.png)

Периодичность: Еженедельное обновление.
Требуемые данные на выходе: по каждому найденному банкомату банка из списка в исследуемых городах должна быть получена следующая информация в формате csv-файла:
ID – ID банкомата в Яндекс Картах
name – наименование банка
wbsite – вебсайт
opening_house – часы работы
lat – ширина
lon – долгота
ration – рейтинг
phone – телефон
social – социальные сети
load_time – дата выгрузки