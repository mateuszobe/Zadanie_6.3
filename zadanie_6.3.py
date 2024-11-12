from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Date, MetaData
from sqlalchemy.sql import select

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
        Column('station', String),
        Column('date', Date),
        Column('precip', Float),
        Column('tobs', Integer),
    )

    #tworzymy tabele w bazie danych:
    meta.create_all(engine)

    #dodajemy przykładowe dane do tabeli 'stations':
    stations_data = [
        {'station': 'USC00519397', 'latitude': 21.2716, 'longitude': -157.8168, 'elevation': 3.0, 'name': 'WAIKIKI 717.2', 'country': 'US', 'state': 'HI'},
        {'station': 'USC00513117', 'latitude': 21.4234, 'longitude': -157.8015, 'elevation': 14.6, 'name': 'KANEOHE 838.1', 'country': 'US', 'state': 'HI'},
    ]

    #dodajemy dane do tabeli 'measurements':
    measurements_data = [
        {'station': 'USC00519397', 'date': '2010-01-01', 'precip': 0.08, 'tobs': 65},
        {'station': 'USC00519397', 'date': '2010-01-02', 'precip': 0.0, 'tobs': 63},
    ]

    #łączymy się z bazą danych i wstawiamy dane:
    conn = engine.connect()

    #wstawiamy dane do tabeli 'stations':
    conn.execute(stations.insert(), stations_data)

    #wstawiamy dane do tabeli 'measurements':
    conn.execute(measurements.insert(), measurements_data)

    #zapytanie SQL:
    results = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()

    #wyświetlenie wyników zapytania:
    for result in results:
        print(result)

    #zamykamy połączenie:
    conn.close()


if __name__ == "__main__":
    create_database()
