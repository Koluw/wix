import time
import json
from os import path, getcwd, sep

import pymysql
import requests


def read_cities_json():
    """
    check the file with geo_points and cities.
    rearrange the data to dictionary for next iterations
    :return: jsonData => dict with cities [id, name, latitude, longitude]
    """
    jsonData = {}
    currentDirectory = getcwd()

    buf = False
    # if we're needed to enter fileName manually
    """
    while not buf:
        fileName = 'cities'
        # fileName = input("Enter file_name of json's cities storage: ")
        if ".json" not in fileName:
            fileName += '.json'
        buf = path.exists(currentDirectory + sep + fileName)
    """
    fileName = 'cities.json'
    try:
        buf = path.exists(currentDirectory + sep + fileName)
        if buf:
            with open(currentDirectory + sep + fileName, 'r') as sf:
                jsonLoad = json.load(sf)

            for key in jsonLoad['cities']:
                jsonData[key['cityName']] = {'lat': key['lat'], 'lon': key['lon']}

            del jsonLoad
        else:
            raise FileNotFoundError
    except FileNotFoundError:
        jsonData["error"] = "FileNotExists"

    return jsonData


def collect_json(citiesDict, steps=5):
    """
    collect the whole json for send to DB(with changed timestamp to UTC)
    :param citiesDict: Dict with city's geo_points
    :param steps: how many appearance of ISS we want to check
    :return: jsonData
    """
    jsonData = {}
    for city in citiesDict:
        jsonData[city] = read_url_json(citiesDict[city]['lat'], citiesDict[city]['lon'], steps)
    return jsonData


def read_url_json(lat, lon, steps=2):
    """
    get json from API f"http://api.open-notify.org/iss-pass.json?
    :param lat: latitude
    :param lon: longitude
    :param steps: how many appearance above one point is interesting for us
    :return: jsonData['response'] with {"risetime": TIMESTAMP, "duration": DURATION}
    """
    jsonAPI = f"http://api.open-notify.org/iss-pass.json?lat={lat}&lon={lon}&alt=20&n={steps}".format(lat, lon, steps)

    try:
        jsonData = requests.get(jsonAPI).json()['response']
    except:
        jsonData["error"] = "API_BadResponse"

    for row in jsonData:
        row['UTC'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(row['risetime']))

    return jsonData


def read_conn_string():
    """
    read connection string parameters
    :return: jsonData => dict with parameters [host, port, user, pass]
    """
    jsonData = {}
    currentDirectory = getcwd()

    buf = False
    fileName = 'conn_string.json'
    try:
        buf = path.exists(currentDirectory + sep + fileName)
        if buf:
            with open(currentDirectory + sep + fileName, 'r') as sf:
                jsonLoad = json.load(sf)

            for key in jsonLoad:
                jsonData[key] = jsonLoad[key]

            del jsonLoad
        else:
            raise FileNotFoundError
    except FileNotFoundError:
        jsonData["error"] = "FileNotExists"
    return jsonData


def sop(someDict):
    """
    function to create table view of collected Data
    and put it to MySQL
    :param someDict: dictionary with all interested Data
    :return:
    """
    # Connect to the database
    conn_string = read_conn_string()
    try:
        connection = pymysql.connect(host=conn_string['host'],
                                     port=conn_string['port'],
                                     user=conn_string['user'],
                                     password=conn_string['pass'],
                                     db=conn_string['db'],
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)

        for city in someDict:
            final_str = ''
            for row in someDict[city]:
                final_str += "('{0}', {1}, '{2}', '{3}'),".format(city, row['duration'], row['risetime'], row['UTC'])

            final_str = final_str[:-1]

            with connection.cursor() as cursor:
                sql = "INSERT INTO interview.orbital_data_stanley (city_name, duration, UNIX, UTC) VALUES {}".format(final_str)
                # print(sql)
                cursor.execute(sql)

            connection.commit()
            print('{0} has been added to DB'.format(city))
    finally:
        connection.close()
        print('insert was successfuly finished')


def step_by_step():
    """
    call of all needed functions in correct order
    :return: nothing, just call another functions
    """
    cities = read_cities_json()

    orbital_data_stanley = collect_json(cities, 50)

    sop(orbital_data_stanley)

    # print(orbital_data_stanley)
    # orbital_data_stanley = {'Haifa': [{'duration': 245, 'risetime': 1591279496, 'UTC': '2020-06-04 14:04:56'}, {'duration': 437, 'risetime': 1591285312, 'UTC': '2020-06-04 15:41:52'}],
    # 'Tel_Aviv': [{'duration': 426, 'risetime': 1591273487, 'UTC': '2020-06-04 12:24:47'}, {'duration': 169, 'risetime': 1591279531, 'UTC': '2020-06-04 14:05:31'}],
    # 'Beer_Sheva': [{'duration': 372, 'risetime': 1591285352, 'UTC': '2020-06-04 15:42:32'}, {'duration': 619, 'risetime': 1591291094, 'UTC': '2020-06-04 17:18:14'}],
    # 'Eilat': [{'duration': 281, 'risetime': 1591285410, 'UTC': '2020-06-04 15:43:30'}, {'duration': 605, 'risetime': 1591291121, 'UTC': '2020-06-04 17:18:41'}]}
    # sop(orbital_data_stanley) # test call
    # print(read_url_json('32.085300', '34.781769')) # test call


if __name__ == '__main__':
    step_by_step()
