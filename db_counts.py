import csv
from dbfread import DBF
import os
import re
import sqlite3
import requests
import xml.etree.ElementTree as ET


class Globals:
    def __init__(self, route_report, exclude_file):
        self.route_report = route_report
        self.exclude_file = exclude_file


def export_final_files():
    """
        Export the final route report, as well as a removed route report
    """
    conn = sqlite3.connect('route_db.db')
    c = conn.cursor()

    # 
    print("Exporting final file")
    qry = c.execute(("SELECT b.city, b.state, a.* "
                     "FROM routes a JOIN usps_data b "
                     "ON a.zipcode = b.zipcode "
                     "WHERE a.deleted IS NULL;"))
   
    with open("{}_FINAL.txt".format(g.route_report[:-4]), 'w+', newline='') as f:
        csvw = csv.DictWriter(f, ['City', 'State', 'ZIP', 'CRRT', 'RES', 'POS'],
                              delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        csvw.writeheader()
        for line in qry:
            csvw.writerow({'City': line[0],
                           'State': line[1],
                           'ZIP': line[2],
                           'CRRT': line[3],
                           'RES': line[4],
                           'POS': line[5]})
    # 
    print("Exporting No City, State file")
    qry = c.execute(("SELECT b.city, b.state, a.* "
                     "FROM routes a LEFT JOIN usps_data b "
                     "ON a.zipcode = b.zipcode "
                     "WHERE a.deleted IS NULL AND b.city IS NULL;"))

    with open("{}_NO CITY STATE NAMES.txt".format(g.route_report[:-4]), 'w+', newline='') as f:
        csvw = csv.DictWriter(f, ['City', 'State', 'ZIP', 'CRRT', 'RES', 'POS'],
                              delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        csvw.writeheader()
        for line in qry:
            csvw.writerow({'City': line[0],
                           'State': line[1],
                           'ZIP': line[2],
                           'CRRT': line[3],
                           'RES': line[4],
                           'POS': line[5]})

    # 
    print("Exporting deleted routes file")
    qry = c.execute(("SELECT * FROM routes WHERE deleted;"))

    with open("{}_DELETED.txt".format(g.route_report[:-4]), 'w+', newline='') as f:
        csvw = csv.DictWriter(f, ['ZIP', 'CRRT', 'RES', 'POS'],
                              delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        csvw.writeheader()
        for line in qry:
            csvw.writerow({'ZIP': line[0],
                           'CRRT': line[1],
                           'RES': line[2],
                           'POS': line[3]})

    conn.close()


def usps_zip_lookup(zipcode):
    url = 'https://secure.shippingapis.com/ShippingAPI.dll?API=CityStateLookup&XML={0}'
    usps_userid = '813BINDE5230'

    if isinstance(zipcode, (list, tuple, set)):
        shell = '<CityStateLookupRequest USERID="{userid}">{r}</CityStateLookupRequest>'
        r = ''
        for n, rec in enumerate(zipcode):
            r += ('<ZipCode ID="{n}"><Zip5>{zipcode}</Zip5>'
                  '</ZipCode>'.format(n=n, zipcode=rec))
        r = shell.format(userid=usps_userid, r=r)

    else:
        r = ('<CityStateLookupRequest USERID="{userid}">'
             '<ZipCode ID="0"><Zip5>{zipcode}</Zip5>'
             '</ZipCode></CityStateLookupRequest>'.format(userid=usps_userid, zipcode=zipcode))

    response = requests.get(url.format(r))
    tree = ET.fromstring(response.content)

    request_d = dict()
    for branch in tree:
        response_d = dict()
        for child in branch:
            response_d[child.tag] = child.text
        request_d[response_d['Zip5']] = response_d

    return request_d


def chunks(lst, n):
    lst = list(lst)
    # For item i in a range that is a length of lst,
    for i in range(0, len(lst), n):
        # Create an index range for lst of n items:
        yield lst[i: i + n]


def append_city_state():
    """
        Creates zipcode, city, state table, populates it using the USPS API
    """
    print("Appending City, State to ZIP table")
    conn = sqlite3.connect('route_db.db')
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS usps_data;")
    c.execute("CREATE TABLE usps_data (zipcode text(5), city text(40), state text(2));")
    conn.commit()


    qry_zips = c.execute("SELECT DISTINCT zipcode FROM routes WHERE DELETED IS NULL;")
    qry_zips = [z[0] for z in qry_zips]

    for n, chunk in enumerate(chunks(qry_zips, 5)):
        z = usps_zip_lookup(chunk)
        sql = "INSERT INTO usps_data VALUES(?, ?, ?);"
        for key, val in z.items():
            c.execute(sql, (key, val['City'], val['State']))

    conn.commit()
    conn.close()


def remove_routes():
    """
        Imports files into sqlite database and exports final count file
            with exclude routes removed
        Imports g.route_report, csv file with fields [ZIP,CRRT,RES,POS]
        Imports g.exclude_file, csv file with fields [ZIP,CRRT,Routecode]
    """
    conn = sqlite3.connect('route_db.db')
    c = conn.cursor()
    print("Importing route data")
    c.execute("DROP TABLE IF EXISTS routes;")
    c.execute("DROP TABLE IF EXISTS exclude_route;")
    c.execute(("CREATE TABLE routes (zipcode TEXT(5), crrt TEXT(5), "
               "res TEXT(10), pos TEXT(10), deleted INT(1));"))
    c.execute("CREATE TABLE exclude_route (zipcode TEXT(5), crrt TEXT(5), Routecode TEXT(10));")
    conn.commit()

    with open(g.route_report, 'r') as f:
        csvr = csv.DictReader(f, ['ZIP','CRRT','RES','POS'])
        next(csvr)
        sql = "INSERT INTO routes (zipcode, crrt, res, pos) VALUES (?, ?, ?, ?);"
        for line in csvr:
            c.execute(sql, (line['ZIP'], line['CRRT'], line['RES'], line['POS']))

    with open(g.exclude_file, 'r') as f:
        csvr = csv.DictReader(f, ['ZIP','CRRT','Routecode'])
        next(csvr)
        sql = "INSERT INTO exclude_route VALUES (?, ?, ?);"
        for line in csvr:
            c.execute(sql, (line['ZIP'], line['CRRT'], line['Routecode']))

    # conn.commit()
    print("Marking excluded routes for deletion")
    c.execute("UPDATE routes SET deleted = 1 "
              "WHERE EXISTS(SELECT * FROM exclude_route "
              "WHERE exclude_route.crrt = routes.crrt AND "
              "exclude_route.zipcode = routes.zipcode);")

    conn.commit()
    conn.close()


def get_dbf_counts(folder_path=None):
    if not folder_path:
        return
    else:
        files = [f for f in os.listdir(os.path.join(".", folder_path))
                 if f[-10:] == 'Counts.dbf']
    return DBF(os.path.join(folder_path, files[0]))


def create_route_report():
    folders = [f for f in os.listdir() if os.path.isdir(f) if f[-5:].upper() != 'LISTS']
    dbfs = set()

    with open(g.route_report, 'w+', newline='') as ofile:
        csvwriter = csv.DictWriter(ofile,['ZIP', 'CRRT', 'RES', 'POS'])
        csvwriter.writeheader()
        for n, folder in enumerate(folders):
            print(folder)
            count_recs = get_dbf_counts(folder)
            for rec in count_recs:
                csvwriter.writerow(rec)

def main():
    global g
    g = Globals('IA Routes.csv', 'Routes_to_Exclude_gtet20_20190613.csv')
    create_route_report()
    remove_routes()
    append_city_state()
    export_final_files()


if __name__ == '__main__':
    main()
