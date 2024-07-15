import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
import traceback
import time
import random as r
# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener los datos de conexión de las variables de entorno
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
CSV_PATH = os.getenv('CSV_PATH')

# Función para leer el archivo CSV y filtrar los datos necesarios
def read_and_filter_csv(file_path):
    # Leer el archivo CSV
    df = pd.read_csv(file_path, low_memory=False)
    
    # Seleccionar solo las columnas necesarias
    df_filtered = df[['continent', 'location', 'date', 'total_cases', 'new_cases',
                      'total_deaths', 'new_deaths', 'total_vaccinations', 'new_vaccinations', 'population']]
    
    # Eliminar filas con NaN en las columnas 'iso_code', 'continent' y 'location'
    df_filtered = df_filtered.dropna(subset=['continent', 'location'])
    idx_eliminados = r.sample(range(0, len(df_filtered)), int(len(df_filtered)*0.8))
    df_filtered = df_filtered.drop(df_filtered.index[idx_eliminados])
    # df_filtered['location'] = df_filtered['location'].str.replace("'", "")
    df_filtered['location'] = df_filtered['location'].apply(lambda x: x.replace("'", ""))
    print("Dataframe read and filtered")
    # print(df_filtered)
    
    return df_filtered

# Función para insertar datos en la base de datos MariaDB
def insert_continents(df,db_url):
    # Crear el motor de SQLAlchemy
    engine = create_engine(db_url, connect_args={'charset': 'utf8mb4'})
    
    # Crear una sesión
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for continent in df['continent'].dropna().unique():
            print(continent)
            insert_statement = text("""
                INSERT INTO dim_continent (continent)
                VALUES (:continent)
            """)
            session.execute(insert_statement, {'continent':continent})
        # Confirmar la transacción
        session.commit()
        print("Continents data inserted successfully")
    except Exception as e:
        # En caso de error, deshacer la transacción
        session.rollback()
        print(f"Error: {e}")
    finally:
            # Cerrar la sesión
        session.close()
        
def insert_location(df,db_url):
    # Crear el motor de SQLAlchemy
    engine = create_engine(db_url, connect_args={'charset': 'utf8mb4'})
    
    # Crear una sesión
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
            # Recorrer las filas del DataFrame y ejecutar comandos SQL INSERT
        df_filtrado = df[['continent', 'location', 'population']].dropna().drop_duplicates()
        print("DF FILTRADo")
        print(df_filtrado)
        for _,row in df_filtrado.iterrows():
            continente = row['continent']
            ciudad = row['location']
            population = row['population']
            consulta = text(f"""
            SELECT id FROM dim_continent WHERE continent = '{continente}'
            """)
            id = session.execute(consulta).fetchone()[0] 
            insert_statement = text("""
                INSERT INTO dim_location (continent_id,country, population)
                VALUES (:continent_id,:country, :population)
            """)
            session.execute(insert_statement, {'continent_id':id,'country':ciudad, 'population':population} )
        # Confirmar la transacción
        session.commit()
        print("Location data inserted successfully")
    except Exception as e:
        # En caso de error, deshacer la transacción
        session.rollback()
        print(f"Error: {e}")
    finally:
            # Cerrar la sesión
        session.close()

def insert_date_dims(df,db_url):
    # Crear el motor de SQLAlchemy
    engine = create_engine(db_url, connect_args={'charset': 'utf8mb4'})
    
    # Crear una sesión
    Session = sessionmaker(bind=engine)
    session = Session()
    aux = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}
    
    try:
        for i,date in enumerate(df['date'].dropna().unique()):
            year,month,day = date.split("-")
            year = int(year)
            day = int(day)
            month = int(month)
            if i == 0:
                insert = text("""
                INSERT IGNORE INTO dim_month (month,id)
                VALUES (:month,:id)
                """)
                session.execute(insert,{'month':aux[month],'id':month})
                insert = text("""
                INSERT IGNORE INTO dim_year (year)
                VALUES (:year)
                """)
                session.execute(insert,{'year':year})
                insert = text("""
                INSERT IGNORE INTO dim_day (id)
                VALUES (:id)
                """)
                session.execute(insert,{'id':day})
            else:
                check = text(f"""
                SELECT * FROM dim_month WHERE id = {month}
                """)
                result = session.execute(check).fetchall()
                if len(result) == 0:
                    insert = text("""
                    INSERT INTO dim_month (month,id)
                    VALUES (:month,:id)
                    """)
                    session.execute(insert,{'month':aux[month],'id':month})
                check = text(f"""
                SELECT * FROM dim_year WHERE year = {year}
                """)
                result = session.execute(check).fetchall()
                if len(result) == 0:
                    insert = text("""
                    INSERT INTO dim_year (year)
                    VALUES (:year)
                    """)
                    session.execute(insert,{'year':year})
                check = text(f"""
                SELECT * FROM dim_day WHERE id = {day}
                """)
                result = session.execute(check).fetchall()
                if len(result) == 0:
                    insert = text("""
                    INSERT INTO dim_day (id)
                    VALUES (:id)
                    """)
                    session.execute(insert,{'id':day})
        # Confirmar la transacción
        session.commit()
        print("dates_dims inserted successfully")
    except Exception as e:
        # En caso de error, deshacer la transacción
        session.rollback()
        print(f"Error: {e}")
    finally:
            # Cerrar la sesión
        session.close()
        
def insert_date(df,db_url):
     # Crear el motor de SQLAlchemy
    engine = create_engine(db_url, connect_args={'charset': 'utf8mb4'})
    
    # Crear una sesión
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for date in df['date'].dropna().unique():
            year,month,day = date.split("-")
            year = int(year)
            day = int(day)
            month = int(month)
            check = text(f"""
            SELECT * FROM dim_month WHERE id = {month}
            """)
            month_id = session.execute(check).fetchone()[0]
            check = text(f"""
            SELECT * FROM dim_year WHERE year = {year}
            """)
            year_id = session.execute(check).fetchone()[0]
            check = text(f"""
            SELECT * FROM dim_day WHERE id = {day}
            """)
            day_id = session.execute(check).fetchone()[0]
            # print("PASO UNICOS")
            check = text(f"""
            SELECT * FROM dim_date WHERE day_id = {day_id} AND month_id = {month_id} AND year_id = {year_id}
            """)
            date_id = session.execute(check).fetchall()
            if date_id == []:
                insert = text("""
                INSERT INTO dim_date (day_id,month_id,year_id)
                VALUES (:day_id,:month_id,:year_id)
                """)    
                session.execute(insert,{'day_id':day_id,'month_id':month_id,'year_id':year_id})

        # Confirmar la transacción
        session.commit()
        print("dates inserted successfully")
    except Exception as e:
        # En caso de error, deshacer la transacción
        session.rollback()
        print(f"Error: {e}")
    finally:
            # Cerrar la sesión
        session.close()


def insert_data_to_db(df, db_url):
    # Crear el motor de SQLAlchemy
    engine = create_engine(db_url, connect_args={'charset': 'utf8mb4'})
    # Crear una sesión
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        # Recorrer las filas del DataFrame y ejecutar comandos SQL INSERT
        for _, row in df.iterrows():
            #continent = row['continent']
            location = row['location']
            year,month,day = row['date'].split("-")
            year = int(year)
            day = int(day)
            month = int(month)
            if not pd.isna(location) :
                check = text(f"""
                    SELECT id FROM dim_location WHERE country = '{location}'
                """)
                location_id = session.execute(check).fetchone()[0]
                # print(location_id)
                # check = text(f"""
                #     SELECT dim_date.id FROM dim_date JOIN dim_year ON dim_date.year_id = dim_year.year WHERE dim_date.month_id = {month} AND dim_date.day_id = {day} AND dim_year.year = {year}
                # """)
                check =  text(f"""
                    SELECT id FROM dim_date WHERE month_id = {month} AND day_id = {day} AND year_id = {year-2019}
                """)
                date_id = session.execute(check).fetchone()[0]
                
                insert_statement = text("""
                    INSERT INTO hechos (location_id, date_id, total_deaths, total_vaccinations, total_covid_cases, new_cases)
                    VALUES (:location_id, :date_id, :total_deaths, :total_vaccinations, :total_covid_cases, :new_cases)
                """)
                dictionary =  {
                    "location_id": location_id,
                    "date_id": date_id,
                    "total_deaths": row['total_deaths'] if not pd.isna(row['total_deaths']) else 0,
                    "total_vaccinations": row['total_vaccinations'] if not pd.isna(row['total_vaccinations']) else 0,
                    "total_covid_cases": row['total_cases'] if not pd.isna(row['total_cases']) else 0,
                    "new_cases": row['new_cases'] if not pd.isna(row['new_cases']) else 0
                }
                # print(dictionary)
                session.execute(insert_statement, dictionary)
            # print("SELECTED")
        # Confirmar la transacción
        session.commit()
        print("Hechos data inserted successfully")
    except Exception as e:
        # En caso de error, deshacer la transacción
        session.rollback()
        print(traceback.format_exc())
        print(f"Error: {e}")
    finally:
        # Cerrar la sesión
        session.close()

def select_data_to_db(df, db_url):
    # Crear el motor de SQLAlchemy
    engine = create_engine(db_url, connect_args={'charset': 'utf8mb4'})
    # Crear una sesión
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Recorrer las filas del DataFrame y ejecutar comandos SQL INSERT
        for _, row in df.iterrows():
            result = session.execute(text("SELECT * FROM hechos"))
            for row2 in result:
                print(row2)
            print("SELECTED")
        # Confirmar la transacción
        session.commit()
        print("Data inserted successfully")
    except Exception as e:
        # En caso de error, deshacer la transacción
        session.rollback()
        print(f"Error: {e}")
    finally:
        # Cerrar la sesión
        session.close()

def check_db_connection(db_url, retries=5, delay=5):
    """
    Verifica la conexión a la base de datos.
    """
    for i in range(retries):
        try:
            engine = create_engine(db_url)
            with engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                if result.scalar() == 1:
                    print("Conexión a la base de datos establecida.")
                    return True
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")
            print(f"Reintentando en {delay} segundos...")
            time.sleep(delay)
    return False

def main():
    # URL de la base de datos MariaDB
    db_url = f'mariadb://{MYSQL_USER}:{MYSQL_PASSWORD}@{DB_HOST}:{DB_PORT}/{MYSQL_DATABASE}'
    print(db_url)
    # Verificar la conexión a la base de datos
    if not check_db_connection(db_url):
        print("No se pudo establecer la conexión a la base de datos. Saliendo...")
        return

    # Leer y filtrar el archivo CSV
    df_filtered = read_and_filter_csv(CSV_PATH)
    # Insertar los datos filtrados en la base de datos
    insert_continents(df_filtered, db_url)
    insert_location(df_filtered, db_url)
    insert_date_dims(df_filtered,db_url)
    insert_date(df_filtered,db_url)
    insert_data_to_db(df_filtered, db_url)
    print("-- FIN PROGRAMA ---")

main()