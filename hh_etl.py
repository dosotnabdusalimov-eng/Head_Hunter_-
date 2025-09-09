import requests
from datetime import datetime, timedelta
from db import cursor, connection


date_from = (datetime.now() - timedelta(days=30)).isoformat()
date_to = datetime.now().isoformat()

url = "https://api.hh.ru/vacancies"
params = {
    "area": 1,
    "date_from": date_from,
    "date_to": date_to,
    "per_page": 30
}

response = requests.get(url, params=params)
data = response.json()

for item in data.get('items', []):
    h_id = item['id']
    title = item['name']
    publish_date = item['published_at'][:10]

    company_id = item['employer']['id'] if item.get('employer') else None
    company_name = item['employer']['name'] if item.get('employer') else None

    location_id = item['address']['id'] if item.get('address') else None
    location_name = item['address']['city'] if item.get('address') else None

    salary = item.get('salary')
    min_salary = salary.get('from') if salary else None
    max_salary = salary.get('to') if salary else None

    category = item['professional_roles'][0]['name'] if item.get('professional_roles') else None
    position = item['experience']['name'] if item.get('experience') else None


    if company_id:
        cursor.execute("""
            INSERT IGNORE INTO companies (companies_id, name)
            VALUES (%s, %s)
        """, (company_id, company_name))
        connection.commit()

        cursor.execute("SELECT id FROM companies WHERE companies_id = %s", (company_id,))
        row = cursor.fetchone()
        company_db_id = row[0] if row else None
    else:
        company_db_id = None


    if location_id:
        cursor.execute("SELECT id FROM locations WHERE location_id = %s", (location_id,))
        row = cursor.fetchone()
        if row:
            location_db_id = row[0]
        else:
            cursor.execute("""
                INSERT INTO locations (location_id, country, city)
                VALUES (%s, %s, %s)
            """, (location_id, 'Russia', location_name))
            connection.commit()
            cursor.execute("SELECT id FROM locations WHERE location_id = %s", (location_id,))
            location_db_id = cursor.fetchone()[0]
    else:
        location_db_id = None


    cursor.execute("""
        INSERT IGNORE INTO vacancies 
            (h_id, title, position, publish_date, category, company_id, location_id, min_salary, max_salary)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (h_id, title, position, publish_date, category, company_db_id, location_db_id, min_salary, max_salary))
    connection.commit()

    cursor.execute("SELECT id FROM vacancies WHERE h_id = %s", (h_id,))
    vacancy_db_id = cursor.fetchone()[0]


    detail_url = f'https://api.hh.ru/vacancies/{h_id}'
    detail_res = requests.get(detail_url)
    detail_data = detail_res.json()

    key_skills = [skill['name'] for skill in detail_data.get('key_skills', [])]

    for skill_name in key_skills:
        cursor.execute("""
            INSERT IGNORE INTO skills (name)
            VALUES (%s)
        """, (skill_name,))
        connection.commit()

        cursor.execute("SELECT id FROM skills WHERE name = %s", (skill_name,))
        skill_db_id = cursor.fetchone()[0]
        if vacancy_db_id and skill_db_id:
            cursor.execute("""
                INSERT IGNORE INTO vacancy_skill (vacancy_id, skill_id)
                VALUES (%s, %s)
            """, (vacancy_db_id, skill_db_id))
            connection.commit()

cursor.close()
connection.close()
print("ok")
