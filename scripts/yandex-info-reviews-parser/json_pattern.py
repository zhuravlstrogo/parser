def into_json(org_id, name, address, website, opening_hours, lat, lon, rating, phone, social, business_aspect):
    """ Шаблон файла OUTPUT.json"""

    opening_hours_new = []
    days = ['mo', 'tu', 'we', 'th', 'fr', 'sa', 'su']

    # Проверка opening_hours на отсутствие одного их рабочих дней
    # Создается отдельный список (opening_hours_new) с полученными значениями
    # Далее он проверяется на отсутствие того или иного рабочего дня
    # На индекс отсутствующего элемента вставляется значение  "   выходной"
    for day in opening_hours:
        opening_hours_new.append(day[:2].lower())
    for i in days:
        if i not in opening_hours_new:
            opening_hours.insert(days.index(i), '   выходной')

    business_aspect_dict = {}
    for i, aspect in enumerate(business_aspect):
        business_aspect_dict[business_aspect[i][0]] = business_aspect[i][1] 

    data_grabbed = {
        "ID": org_id,
        "name": name,
        "address": address,
        "website": website,
        "opening_hours": 
        f"'mon': {opening_hours[0][3:]}, "
        f"'tue': {opening_hours[1][3:]}, "
        f"'wed': {opening_hours[2][3:]}, "
        f"'thu': {opening_hours[3][3:]}, "
        f"'fri': {opening_hours[4][3:]}, "
        f"'sat': {opening_hours[5][3:]}, "
        f"'sun': {opening_hours[6][3:]}",
        "lat": lat,
        "lon": lon,
        "rating": rating,
        "phone": phone,
        "social": social,
    }
    data_grabbed.update(business_aspect_dict)
    return data_grabbed