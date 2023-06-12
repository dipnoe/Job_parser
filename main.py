from utils.config import config
from utils.dbmanager import DBManager
from utils.api import get_response
from utils.txt_reader import txt_reader


def main():
    file = 'companies.txt'
    params = config()

    db = DBManager(params=params)
    db.drop_tables()
    db.create_tables()

    # Список id интересных компаний.
    companies_ids = txt_reader(file)

    # Получение данных от апи и вставка их в таблицы.
    for id_ in companies_ids:
        companies = get_response('https://api.hh.ru/employers/' + id_)
        vacancies = get_response('https://api.hh.ru/vacancies?employer_id=' + id_)['items']
        db.insert_company_data(companies)
        for vacancy in vacancies:
            db.insert_vacancy_data(vacancy)

    print('Компании и количество вакансий:')
    for i in db.get_companies_and_vacancies_count():
        print(*i)

    print('\nВесь список вакансий:')
    for i in db.get_all_vacancies():
        print(*i)

    print()
    print(*db.get_avg_salary())

    print('\nВакансии с зп выше средней:')
    for i in db.get_vacancies_with_higher_salary():
        print(*i)

    keyword = input('Введите слово для поиска по вакансиям:\n')
    for i in db.get_vacancies_with_keyword(keyword):
        print(*i)


if __name__ == '__main__':
    main()
