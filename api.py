import requests


class HHResponseError(Exception):
    pass


def get_response(url: str):
    """
    Функция для получения запроса.
    """
    response = requests.get(url)
    if not response.ok:
        raise HHResponseError
    return response.json()
