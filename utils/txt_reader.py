def txt_reader(txt_file: str) -> list[str]:
    """
    Функция для чтения txt файла.
    """
    with open(txt_file, 'r', encoding='utf-8') as file:
        company_ids = list()
        for line in file:
            company_ids.append(line.split()[0])
        return company_ids
