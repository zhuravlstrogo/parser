import re 

def find_between(s, first, last):
    """находит строку между символами first и last"""
    try:
        regex = rf'{first}(.*?){last}'
        return re.findall(regex, s)
    except ValueError:
        return -1


def search_end_of_str(start_with, full_str):
    end_with = re.search(f"{start_with}.*?(\d+)", full_str).group(1)
    return end_with