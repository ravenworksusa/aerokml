#!/usr/bin/python3

import sys
import argparse
import sqlite3
import simplekml
import logging
import os
import time
from pprint import pprint
from pathlib import Path

# Args
parser = argparse.ArgumentParser()
parser.add_argument('--db', help='Path to Aeroscope SQLite Database. This parameter is required.')
parser.add_argument('--sn', help='Exports logs to KML for the drone with this serial number. If no S/N sepcified, will process all drones.')
parser.add_argument('--dir', nargs='?', type=str, default="./kml/", help='Export KML files to this directory. by default, this is ./kml/. You must have write permissions to the specified directory. The directory must already exist.')
args = parser.parse_args()

# Logging
logging.basicConfig(format='%(message)s',level=logging.DEBUG)

# Queries
SQL_DRONES = 'SELECT DISTINCT sn,productType FROM dji_pilot_detect_model_DroneRecordInfo;'
SQL_SNS = 'SELECT DISTINCT sn FROM dji_pilot_detect_model_DroneRecordInfo;'
SQL_FLIGHT = 'SELECT lastDronePushUpdateTime,latitude,longitude,absoluteHeight FROM dji_pilot_detect_model_DroneRecordInfo WHERE sn=(?) AND flightIndex=(?) AND latitude!=0.0 AND longitude!=0.0 ORDER BY lastDronePushUpdateTime;'
SQL_GCS = 'SELECT lastDronePushUpdateTime,personLatitude,personLongitude,height FROM dji_pilot_detect_model_DroneRecordInfo WHERE sn=(?) AND flightIndex=(?) AND personLatitude!=0.0 AND personLongitude!=0.0 ORDER BY lastDronePushUpdateTime;'
SQL_HOME = 'SELECT DISTINCT  homeLatitude, homeLongitude FROM dji_pilot_detect_model_DroneRecordInfo WHERE sn=(?) AND flightIndex=(?) AND homeLatitude!=0.0 AND homeLongitude!=0.0 ORDER BY lastDronePushUpdateTime;'


def conv_time(epoch):
    epoch = int(epoch) / 1000
    t = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(int(epoch)))
    return str(t)

def gen_kml(sn, flight_id, flight, prod_type, loc_type):
    kmldoc = simplekml.Kml()
    kmldoc_name = [str(sn), str(prod_type), str(flight_id), str(loc_type)]
    kmldoc.document.name = '_'.join(kmldoc_name)
    logging.info('Generating KML for S/N: %s Flight number: %s Type: %s', str(sn), str(flight_id), str(loc_type))
    for i in flight:
        if str(loc_type) == 'home':
            kmldoc.newpoint(name='home', coords=[(i[1],i[0])])
        else:
            pnt = kmldoc.newpoint(name=str(i[0]), coords=[(i[2],i[1],i[3])])
            pnt.timestamp.when = conv_time(str(i[0]))
    fname = str(kmldoc.document.name)+'.kml'
    kmldoc.save(os.path.join(os.getcwd(), str(args.dir), fname))

def gen_ls(sn, flight_id, flight, prod_type, loc_type):
    kmldoc = simplekml.Kml()
    kmldoc_name = [str(sn), str(prod_type), str(flight_id), str(loc_type)]
    kmldoc.document.name = '_'.join(kmldoc_name)
    ls_flight = []
    ls = kmldoc.newlinestring(name='_'.join(kmldoc_name))
    logging.info('Generating KML for S/N: %s Flight number: %s Type: %s', str(sn), str(flight_id), str(loc_type))
    for i in flight:
        ls_flight.append(tuple((i[2], i[1], i[3]))) 
    ls.coords = ls_flight
    ls.extrude = 0
    ls.altitudemode = simplekml.AltitudeMode.absolute
    ls.style.linestyle.width = 5
    ls.style.linestyle.color = simplekml.Color.blue
    fname = str(kmldoc.document.name)+'.kml'
    kmldoc.save(os.path.join(os.getcwd(), str(args.dir), fname))

def query_flights(sn, prod_type, flights, cur):
    for i in flights:
        flight = cur.execute(SQL_FLIGHT, (sn, str(i[0]))).fetchall()
        gcs = cur.execute(SQL_GCS, (sn, str(i[0]))).fetchall()
        home = cur.execute(SQL_HOME, (sn, str(i[0]))).fetchall()
        gen_kml(sn, str(i[0]), flight, prod_type, str('flight'))
        gen_kml(sn, str(i[0]), gcs, prod_type, str('gcs'))
        gen_kml(sn, str(i[0]), home, prod_type, str('home'))
        gen_ls(sn, str(i[0]), flight, prod_type, str('ls'))

def main():
    con = None
    cur = None
    save_path = None

    # Check to make sure database exists and destination directory is writable.
    if os.path.exists(args.db):
        con = sqlite3.connect(args.db)
        cur = con.cursor()
        
    else:
        logging.error('%s does not exist.', str(args.db))
        sys.exit(1)

    if os.access(args.dir, os.W_OK):
        save_path = Path(str(args.dir))
        logging.info('Direcotry: %s is writable and KML will be saved here.', str(args.dir))
        
    else:
        logging.error('%s does not exist or is not writable.', str(args.dir))
        sys.exit(1)

    if not save_path.is_dir():
        logging.error('%s is not a directory.', str(args.dir))
        sys.exit(1)
        
    logging.info('Processing Aeroscope Database file: %s', str(args.db))

    try:
        drones = cur.execute(SQL_DRONES).fetchall()
        sns = cur.execute(SQL_SNS).fetchall()
        print(drones)
    except:
        logging.error('%s is not a properly formatted Aeroscope database.', str(args.db))
        sys.exit(1)

    if args.sn is not None:
        flights = cur.execute('SELECT DISTINCT flightIndex FROM dji_pilot_detect_model_DroneRecordInfo WHERE sn=(?)', (args.sn,)).fetchall()
        prod_type = cur.execute('SELECT DISTINCT productType FROM dji_pilot_detect_model_DroneInfo WHERE sn=(?)', (args.sn,)).fetchall()
        prod_type = str(prod_type[0][0])
        logging.info('====================')
        logging.info('Querying flights for: %s S/N: %s', str(prod_type), str(args.sn))
        query_flights(str(args.sn), prod_type, flights, cur)
    else:
        for drone in sns:
            flights = cur.execute('SELECT DISTINCT flightIndex FROM dji_pilot_detect_model_DroneRecordInfo WHERE sn=(?)', (str(drone[0]),)).fetchall()
            prod_type = cur.execute('SELECT DISTINCT productType FROM dji_pilot_detect_model_DroneInfo WHERE sn=(?)', (str(drone[0]),)).fetchall()
            prod_type = str(prod_type[0][0])
            logging.info('====================')
            logging.info('Querying flights for: %s S/N: %s', str(prod_type), str(drone[0]))
            query_flights(str(drone[0]), prod_type, flights, cur)

if __name__ == '__main__':
    main()
