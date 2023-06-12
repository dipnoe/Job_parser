import psycopg2


class DBManager:
    """
    Класс для работы с базой данных
    """

    def get_companies_and_vacancies_count(self) -> list[tuple]:
        """
        Получает список всех компаний и количество вакансий у каждой компании.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT companies.company_name, COUNT(*) AS number_of_vacancies FROM vacancies
            RIGHT JOIN companies USING (company_id)
            GROUP BY company_name;
            """)

            return cur.fetchall()

    def get_all_vacancies(self) -> list[tuple]:
        """
        Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT companies.company_name AS company_name, vacancy_name, salary_from, vacancy_url FROM vacancies
            JOIN companies USING (company_id);
            """)

            return cur.fetchall()

    def get_avg_salary(self) -> tuple:
        """
        Получает среднюю зарплату по вакансиям.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT AVG(salary_from) AS average_salary FROM vacancies;
            """)

            return cur.fetchone()

    def get_vacancies_with_higher_salary(self) -> list[tuple]:
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT vacancy_name, salary_from, salary_to, vacancy_url FROM vacancies
            WHERE salary_from > (SELECT AVG(salary_from) FROM vacancies);
            """)

            return cur.fetchall()

    def get_vacancies_with_keyword(self, keyword: str) -> list[tuple]:
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова.
        """
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT vacancy_name, salary_from, salary_to, vacancy_url FROM vacancies
            WHERE vacancy_name ILIKE '%{keyword}%';
            """)

            return cur.fetchall()

    def __init__(self, params: dict):
        self.conn = psycopg2.connect(dbname='course_5', **params)
        self.conn.autocommit = True

    def create_tables(self) -> None:
        """
        Создание таблиц компаний и вакансий.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                company_id SERIAL PRIMARY KEY,
                company_name VARCHAR(50) NOT NULL,
                city VARCHAR(50),
                description TEXT,
                company_url TEXT NOT NULL
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                company_id INT REFERENCES companies(company_id),
                vacancy_name VARCHAR(100) NOT NUll,
                salary_from INTEGER,
                salary_to INTEGER,
                vacancy_url TEXT
                )
            """)

    def drop_tables(self) -> None:
        """
        Удаление таблиц компаний и вакансий.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                DROP TABLE vacancies
                """)
            cur.execute("""
                DROP TABLE companies
                """)

    def insert_company_data(self, company: dict) -> None:
        """
        Заполнение таблицы компаний данными.
        """
        with self.conn.cursor() as cur:
            cur.execute(f"""
            INSERT INTO companies (company_id, company_name, city, description, company_url)
            VALUES (%s, %s, %s, %s, %s)""", (int(company['id']),
                                             company['name'],
                                             company['area']['name'],
                                             company['description'],
                                             company['alternate_url']))

    def insert_vacancy_data(self, vacancies: dict) -> None:
        """
        Заполнение таблицы вакансий данными.
        """
        with self.conn.cursor() as cur:
            if vacancies['salary'] is None:
                salary_from, salary_to = None, None
            else:
                salary_from, salary_to = vacancies['salary']['from'], vacancies['salary']['to']

            cur.execute(""" INSERT INTO vacancies (company_id, vacancy_name, salary_from, salary_to, vacancy_url)
            VALUES (%s, %s, %s, %s, %s)""", (int(vacancies['employer']['id']),
                                             vacancies['name'],
                                             salary_from,
                                             salary_to,
                                             vacancies['alternate_url']
                                             ))
