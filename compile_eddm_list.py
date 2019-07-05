import datetime
import os
import sqlite3
import csv

def list_count_report():
    datestring = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
    conn = sqlite3.connect('route_count.db')
    c = conn.cursor()
    print("Importing route data")
    c.execute("DROP TABLE IF EXISTS route;")
    c.execute("CREATE TABLE route (src text(50), state text(2));")

    state_list = ['UT', 'AZ', 'IA', 'KS', 'MN', 'NE', 'NM', 'SD']
    for folder in state_list:
        routepath = os.path.join(os.path.curdir, folder, 
                                     'Completed Lists', "{0} Routes_FINAL.txt".format(folder))

        with open(routepath, 'r') as report:
            header = ['City','State','ZIP','CRRT','RES','POS']
            csvr = csv.DictReader(report, header, delimiter='\t')
            next(report)
            for row in csvr:
                sql = "INSERT INTO route VALUES(?, ?);"
                c.execute(sql, ("{0} Routes_FINAL.txt".format(folder), row['State'],))

    conn.commit()

    with open('state count report_{}.txt'.format(datestring), 'w+') as r:
        qry = c.execute("SELECT src, state, count(*) FROM route GROUP BY src, state;")
        r.write("{:<30}{:<4}{:>20}\n".format("Source", "ST", "Total Routes Count"))
        for line in qry:
            r.write("{:<30}{:<4}{:>20,}\n".format(line[0], line[1], line[2]))

    conn.close()


def main():
    # create master list
    datestring = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
    state_list = ['UT', 'AZ', 'IA', 'KS', 'MN', 'NE', 'NM', 'SD']
    # state_list = ['UT', 'AZ']
    with open("FULL_EDDM_LIST_{}.txt".format(datestring), 'w+') as s:
        s.write('City\tState\tZIP\tCRRT\tRES\tPOS\n')
        for folder in state_list:
            print(folder)
            routepath = os.path.join(os.path.curdir, folder, 
                                     'Completed Lists', "{0} Routes_FINAL.txt".format(folder))
            with open(routepath, 'r') as report:
                next(report)
                for line in report:
                    s.write(line)


if __name__ == '__main__':
    main()
    list_count_report()
