import pandas as pd
import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        database="ds7trab",
        user="postgres",
        password="postgres123" 
    )
    cursor = conn.cursor()
    print("Conectado ao banco de dados com sucesso!")
except Exception as e:
    print(f"Erro ao conectar ao banco: {e}")
    exit()

csv_path = "./healthcare_dataset.csv"
df = pd.read_csv(csv_path)

create_table_query = """
CREATE TABLE IF NOT EXISTS healthcare_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    age INTEGER,
    gender VARCHAR(10),
    blood_type VARCHAR(5),
    medical_condition TEXT,
    date_of_admission DATE,
    doctor VARCHAR(100),
    hospital VARCHAR(100),
    insurance_provider VARCHAR(100),
    billing_amount FLOAT,
    room_number INTEGER,
    admission_type VARCHAR(50),
    discharge_date DATE,
    medication TEXT,
    test_results TEXT
);
"""
cursor.execute(create_table_query)
conn.commit()
print("Tabela criada com sucesso!")

for _, row in df.iterrows():
    insert_query = """
    INSERT INTO healthcare_data (
        name, age, gender, blood_type, medical_condition, 
        date_of_admission, doctor, hospital, insurance_provider, 
        billing_amount, room_number, admission_type, discharge_date, 
        medication, test_results
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, tuple(row))
conn.commit()
print("Dados inseridos com sucesso!")

cursor.close()
conn.close()
