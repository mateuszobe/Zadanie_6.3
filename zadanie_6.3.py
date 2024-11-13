from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Date, MetaData
from sqlalchemy.sql import select, update, delete
import csv

def create_database():
    #tworzymy połączenie z bazą danych SQLite:
    engine = create_engine('sqlite:///weather.db', echo=True)
    meta = MetaData()

    #definiujemy tabelę 'stations':
    stations = Table(
        'stations', meta,
        Column('station', String, primary_key=True),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('elevation', Float),
        Column('name', String),
        Column('country', String),
        Column('state', String),
    )

    #definiujemy tabelę 'measurements':
    measurements = Table(
        'measurements', meta,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('station', String),
        Column('date', Date),
        Column('precip', Float),
        Column('tobs', Integer),
    )

    #tworzymy tabele w bazie danych:
    meta.create_all(engine)

    #połączenie z bazą danych:
    conn = engine.connect()

    #wczytujemy dane z CSV do tabeli 'stations':
    with open('clean_stations.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        stations_data = [
            {
                'station': row['station'],
                'latitude': float(row['latitude']),
                'longitude': float(row['longitude']),
                'elevation': float(row['elevation']),
                'name': row['name'],
                'country': row['country'],
                'state': row['state']
            }
            for row in reader
        ]
        conn.execute(stations.insert(), stations_data)

    #wczytujemy dane z CSV do tabeli 'measurements':
    with open('clean_measure.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        measurements_data = [
            {
                'station': row['station'],
                'date': row['date'],
                'precip': float(row['precip']) if row['precip'] else None,
                'tobs': int(row['tobs']) if row['tobs'] else None
            }
            for row in reader
        ]
        conn.execute(measurements.insert(), measurements_data)

    print("Dane zostały wczytane do bazy danych.")

    #testowanie zapytań SQLAlchemy:
    #SELECT * FROM stations:
    results = conn.execute(select([stations])).fetchall()
    print("Wszystkie rekordy z tabeli 'stations':")
    for result in results:
        print(result)

    #SELECT z filtrem:
    results = conn.execute(select([measurements]).where(measurements.c.station == 'USC00519397')).fetchall()
    print("\nPomiar dla stacji 'USC00519397':")
    for result in results:
        print(result)

    #UPDATE:
    conn.execute(update(stations).where(stations.c.station == 'USC00519397').values(name='NEW STATION NAME'))
    print("\nPo aktualizacji nazwy stacji:")
    result = conn.execute(select([stations]).where(stations.c.station == 'USC00519397')).fetchone()
    print(result)

    #DELETE:
    conn.execute(delete(measurements).where(measurements.c.station == 'USC00519397'))
    print("\nPo usunięciu pomiarów dla stacji 'USC00519397':")
    remaining_results = conn.execute(select([measurements]).where(measurements.c.station == 'USC00519397')).fetchall()
    print(remaining_results)

    #zamknięcie połączenia:
    conn.close()

#plik główny:
if __name__ == "__main__":
    create_database()
