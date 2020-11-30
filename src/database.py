import sqlite3
import retrieve


def create_databases():
    print("Connecting to database...")
    conn = sqlite3.connect("covid.db")
    c = conn.cursor()
    print("Database connected!")

    print("Creating database...")

    query = "CREATE TABLE IF NOT EXISTS Global(id INTEGER PRIMARY KEY, date TEXT, NewConfirmed INTEGER, TotalConfirmed INTEGER, NewDeaths INTEGER, TotalDeaths INTEGER, NewRecovered INTEGER, TotalRecovered INTEGER);"
    c.execute(query)

    query = "CREATE TABLE IF NOT EXISTS Countries(id INTEGER PRIMARY KEY, date TEXT, Country TEXT, date_id REFERENCES Global(id), NewConfirmed INTEGER, TotalConfirmed INTEGER, NewDeaths INTEGER, TotalDeaths INTEGER, NewRecovered INTEGER, TotalRecovered INTEGER);"
    c.execute(query)

    query = "CREATE TABLE IF NOT EXISTS USA(id INTEGER PRIMARY KEY, date TEXT, Confirmed INTEGER, Negative INTEGER, Deaths INTEGER, Recovered INTEGER);"
    c.execute(query)

    query = "CREATE TABLE IF NOT EXISTS States(id INTEGER PRIMARY KEY, date TEXT, State TEXT, date_id REFERENCES USA(id), Confirmed INTEGER, Negative INTEGER, Deaths INTEGER, Recovered INTEGER);"
    c.execute(query)

    print("Database created!")

    conn.commit()
    conn.close()


def save_usa_data():
    url = 'https://api.covidtracking.com/v1/us/current.json'
    conn = sqlite3.connect("covid.db")
    c = conn.cursor()

    data = retrieve.retrieve_data(url)[0]
    date = retrieve.retrieve_date()

    query = "INSERT INTO USA(date, Confirmed, Negative, Deaths, Recovered) VALUES (?, ?, ?, ?, ?);"
    c.execute(query, (date, data['positive'],
                      data['negative'], data['death'], data['recovered']))

    conn.commit()
    conn.close()


def save_states_data():
    url = 'https://api.covidtracking.com/v1/states/current.json'
    conn = sqlite3.connect("covid.db")
    c = conn.cursor()

    data = retrieve.retrieve_data(url)
    date = retrieve.retrieve_date()

    data = sorted(data, key=lambda x: x['positive'], reverse=True)

    query = "SELECT date, id FROM USA"
    c.execute(query)
    ref = c.fetchall()
    ref_dict = dict(ref)

    query = "SELECT * FROM States;"
    c.execute(query)

    count = len(c.fetchall())

    us_state_abbrev = {
        'Alabama': 'AL',
        'Alaska': 'AK',
        'American Samoa': 'AS',
        'Arizona': 'AZ',
        'Arkansas': 'AR',
        'California': 'CA',
        'Colorado': 'CO',
        'Connecticut': 'CT',
        'Delaware': 'DE',
        'District of Columbia': 'DC',
        'Florida': 'FL',
        'Georgia': 'GA',
        'Guam': 'GU',
        'Hawaii': 'HI',
        'Idaho': 'ID',
        'Illinois': 'IL',
        'Indiana': 'IN',
        'Iowa': 'IA',
        'Kansas': 'KS',
        'Kentucky': 'KY',
        'Louisiana': 'LA',
        'Maine': 'ME',
        'Maryland': 'MD',
        'Massachusetts': 'MA',
        'Michigan': 'MI',
        'Minnesota': 'MN',
        'Mississippi': 'MS',
        'Missouri': 'MO',
        'Montana': 'MT',
        'Nebraska': 'NE',
        'Nevada': 'NV',
        'New Hampshire': 'NH',
        'New Jersey': 'NJ',
        'New Mexico': 'NM',
        'New York': 'NY',
        'North Carolina': 'NC',
        'North Dakota': 'ND',
        'Northern Mariana Islands': 'MP',
        'Ohio': 'OH',
        'Oklahoma': 'OK',
        'Oregon': 'OR',
        'Pennsylvania': 'PA',
        'Puerto Rico': 'PR',
        'Rhode Island': 'RI',
        'South Carolina': 'SC',
        'South Dakota': 'SD',
        'Tennessee': 'TN',
        'Texas': 'TX',
        'Utah': 'UT',
        'Vermont': 'VT',
        'Virgin Islands': 'VI',
        'Virginia': 'VA',
        'Washington': 'WA',
        'West Virginia': 'WV',
        'Wisconsin': 'WI',
        'Wyoming': 'WY'
    }

    us_states = {y: x for x, y in us_state_abbrev.items()}

    if count < 56:
        if count == 0:
            for i in data[:25]:
                date_id = ref_dict[date]
                state = us_states[i['state']]
                query = "INSERT INTO States(date, state, date_id, Confirmed, Negative, Deaths, Recovered) VALUES (?, ?, ?, ?, ?, ?, ?);"
                c.execute(
                    query, (date, state, date_id, i['positive'], i['negative'], i['death'], i['recovered']))
        elif count == 25:
            for i in data[25:50]:
                date_id = ref_dict[date]
                state = us_states[i['state']]
                query = "INSERT INTO States(date, state, date_id, Confirmed, Negative, Deaths, Recovered) VALUES (?, ?, ?, ?, ?, ?, ?);"
                c.execute(
                    query, (date, state, date_id, i['positive'], i['negative'], i['death'], i['recovered']))
        elif count == 50:
            for i in data[50:]:
                date_id = ref_dict[date]
                state = us_states[i['state']]
                query = "INSERT INTO States(date, state, date_id, Confirmed, Negative, Deaths, Recovered) VALUES (?, ?, ?, ?, ?, ?, ?);"
                c.execute(
                    query, (date, state, date_id, i['positive'], i['negative'], i['death'], i['recovered']))
    elif count < 112:
        if count == 56:
            for i in data[:25]:
                date_id = ref_dict[date]
                state = us_states[i['state']]
                query = "INSERT INTO States(date, state, date_id, Confirmed, Negative, Deaths, Recovered) VALUES (?, ?, ?, ?, ?, ?, ?);"
                c.execute(
                    query, (date, state, date_id, i['positive'], i['negative'], i['death'], i['recovered']))
        if count == 81:
            for i in data[25:50]:
                date_id = ref_dict[date]
                state = us_states[i['state']]
                query = "INSERT INTO States(date, state, date_id, Confirmed, Negative, Deaths, Recovered) VALUES (?, ?, ?, ?, ?, ?, ?);"
                c.execute(
                    query, (date, state, date_id, i['positive'], i['negative'], i['death'], i['recovered']))
        if count == 106:
            for i in data[50:]:
                date_id = ref_dict[date]
                state = us_states[i['state']]
                query = "INSERT INTO States(date, state, date_id, Confirmed, Negative, Deaths, Recovered) VALUES (?, ?, ?, ?, ?, ?, ?);"
                c.execute(
                    query, (date, state, date_id, i['positive'], i['negative'], i['death'], i['recovered']))

    conn.commit()
    conn.close()


def save_global_data():
    url = 'https://api.covid19api.com/summary'
    conn = sqlite3.connect("covid.db")
    c = conn.cursor()

    data = retrieve.retrieve_data(url)['Global']
    date = retrieve.retrieve_date()

    query = "INSERT INTO Global(date, NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered) VALUES (?, ?, ?, ?, ?, ?, ?);"
    c.execute(query, (date, data['NewConfirmed'], data['TotalConfirmed'], data['NewDeaths'],
                      data['TotalDeaths'], data['NewRecovered'], data['TotalRecovered']))

    conn.commit()
    conn.close()


def save_country_data():
    url = 'https://api.covid19api.com/summary'
    conn = sqlite3.connect("covid.db")
    c = conn.cursor()

    data = retrieve.retrieve_data(url)['Countries']
    date = retrieve.retrieve_date()

    data = sorted(data, key=lambda x: x['TotalConfirmed'], reverse=True)

    query = "SELECT date, id FROM Global"
    c.execute(query)
    ref = c.fetchall()
    ref_dict = dict(ref)

    query = "SELECT * FROM Countries"
    c.execute(query)

    count = len(c.fetchall())

    if count < 75:
        if count == 0:
            for i in data[:25]:
                date_id = ref_dict[date]
                query = "INSERT INTO Countries(date, date_id, Country, NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
                c.execute(query, (date, date_id, i['Country'], i['NewConfirmed'], i['TotalConfirmed'], i['NewDeaths'],
                                  i['TotalDeaths'], i['NewRecovered'], i['TotalRecovered']))
        elif count == 25:
            for i in data[25:50]:
                date_id = ref_dict[date]
                query = "INSERT INTO Countries(date, date_id, Country, NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
                c.execute(query, (date, date_id, i['Country'], i['NewConfirmed'], i['TotalConfirmed'], i['NewDeaths'],
                                  i['TotalDeaths'], i['NewRecovered'], i['TotalRecovered']))
        elif count == 50:
            for i in data[50:75]:
                date_id = ref_dict[date]
                query = "INSERT INTO Countries(date, date_id, Country, NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
                c.execute(query, (date, date_id, i['Country'], i['NewConfirmed'], i['TotalConfirmed'], i['NewDeaths'],
                                  i['TotalDeaths'], i['NewRecovered'], i['TotalRecovered']))
    elif count < 150:
        if count == 75:
            for i in data[:25]:
                date_id = ref_dict[date]
                query = "INSERT INTO Countries(date, date_id, Country, NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
                c.execute(query, (date, date_id, i['Country'], i['NewConfirmed'], i['TotalConfirmed'],
                                  i['NewDeaths'], i['TotalDeaths'], i['NewRecovered'], i['TotalRecovered']))
        elif count == 100:
            for i in data[25:50]:
                date_id = ref_dict[date]
                query = "INSERT INTO Countries(date, date_id, Country, NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
                c.execute(query, (date, date_id, i['Country'], i['NewConfirmed'], i['TotalConfirmed'],
                                  i['NewDeaths'], i['TotalDeaths'], i['NewRecovered'], i['TotalRecovered']))
        elif count == 125:
            for i in data[50:75]:
                date_id = ref_dict[date]
                query = "INSERT INTO Countries(date, date_id, Country, NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
                c.execute(query, (date, date_id, i['Country'], i['NewConfirmed'], i['TotalConfirmed'],
                                  i['NewDeaths'], i['TotalDeaths'], i['NewRecovered'], i['TotalRecovered']))
    elif count < 225:
        if count == 150:
            for i in data[:25]:
                date_id = ref_dict[date]
                query = "INSERT INTO Countries(date, date_id, Country, NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
                c.execute(query, (date, date_id, i['Country'], i['NewConfirmed'], i['TotalConfirmed'],
                                  i['NewDeaths'], i['TotalDeaths'], i['NewRecovered'], i['TotalRecovered']))
        elif count == 175:
            for i in data[25:50]:
                date_id = ref_dict[date]
                query = "INSERT INTO Countries(date, date_id, Country, NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
                c.execute(query, (date, date_id, i['Country'], i['NewConfirmed'], i['TotalConfirmed'],
                                  i['NewDeaths'], i['TotalDeaths'], i['NewRecovered'], i['TotalRecovered']))
        elif count == 200:
            for i in data[50:75]:
                date_id = ref_dict[date]
                query = "INSERT INTO Countries(date, date_id, Country, NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
                c.execute(query, (date, date_id, i['Country'], i['NewConfirmed'], i['TotalConfirmed'],
                                  i['NewDeaths'], i['TotalDeaths'], i['NewRecovered'], i['TotalRecovered']))

    conn.commit()
    conn.close()


def delete_rows(table, rows):
    conn = sqlite3.connect("covid.db")
    c = conn.cursor()

    query = f"DELETE FROM {table} WHERE ID > {rows};"
    c.execute(query)

    print("Rows deleted!")

    conn.commit()
    conn.close()


def start_over(table):
    conn = sqlite3.connect("covid.db")
    c = conn.cursor()

    query = f"DROP TABLE {table};"
    c.execute(query)

    print("Table deleted!")

    conn.commit()
    conn.close()


def start_all_over():
    conn = sqlite3.connect("covid.db")
    c = conn.cursor()

    query = "DROP TABLE Countries;"
    c.execute(query)

    query = "DROP TABLE Global;"
    c.execute(query)

    query = "DROP TABLE States;"
    c.execute(query)

    query = "DROP TABLE USA;"
    c.execute(query)

    conn.commit()
    conn.close()
