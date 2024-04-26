Для работы на сервере нужен [Chrome version 114](https://mirror.cs.uchicago.edu/google-chrome/pool/main/g/google-chrome-stable/) и библиотеки из requirements.txt.
примерные шаги установки описаны в run.sh, сталкивалась с [ошибкой man-db](https://stackoverflow.com/questions/76607092/google-compute-engine-processing-triggers-for-man-db-2-9-4-2-it-takes-a).

Парсер на 1) этапе по списку городов формирует словарь город - id банка в яндекс картах. Например, для альфа банка в [Ржеве](https://yandex.ru/maps/org/alfa_bank/34376605990/?ll=34.328759%2C56.265052&z=16) {‘Ржев’: 34376605990}. 
На 2) этапе переходим по вышеуказанной ссылке в раздел [филиалы](https://yandex.ru/maps/org/alfa_bank/34376605990/chain/?ll=34.328759%2C56.265052&z=16) и достаём оттуда все банки в этом городе.

Далее можем по списку банков-ссылок 3) спрасить информацию о банках или 4) [отзывы](https://yandex.ru/maps/org/alfa_bank/34376605990/reviews/?ll=34.328759%2C56.265052&z=16).
Первые два этапа можно запускать редко (новые банки редко появляются). Этап 3 работает около 3 дней для 1 банка по всем городам (можно оптимизировать/ускорить).

Этап 1.
Используется официальное яндекс API Поиска по организациям, нужен завести api-token на [сайте](https://developer.tech.yandex.ru/services) и записать его в config.py как apikey. Ограничения по запросам - 50 в секунду, 500 в сутки. 
Код  в call_yandex_api_org.py, основная функция вызова апи - get_cities_dict(). Из списка городов cities.txt создаём файлик с пустым словарём город-яндекс id, называется cities_dict_{bank_name}.pickle. Если банк не найден - значение в словаре 0.
TODO: проверка, что количество городов cities.txt не сильно больше, чем в cities_dict_{bank_name}.pickle. Если в словаре не хватает городов - вызов основной функции вызова апи. 
Важно в списке городов прописывать букву “ё” (Пугачёв и прочее).
Этап 2-3.
Код по формированию ссылок и получению инфо выполняется пока количество городов в cities.txt сильно больше количества банков с сохранённой информацией. информацией 
Код в main pipeline_info.py. После его отработки нужно соединить все банки по отдельным городам в один dataframe - merge_all_info() в after_work.py

Этап 4.
Логика: Проходимся по всем городам для банка N -> в городе N получаемя все банки
3 ->  для каждого банка получаем список отзывов. Код в pipeline_review, основная функция в main TODO.
Предполагается, что ссылки для всех банков уже готовы на шаге 2 - TODO: сделать шаг 2 независимым.  
