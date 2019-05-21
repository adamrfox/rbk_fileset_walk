#!/usr/bin/python

import sys
import getpass
import getopt
import urllib3
import rubrik_cdm
import datetime
import pytz
urllib3.disable_warnings()

def usage():
    print "Usage goes here!"
    exit(0)

def get_creds_from_file(file, array):
    with open(file) as fp:
        data = fp.read()
    fp.close()
    data = data.decode('uu_codec')
    data = data.decode('rot13')
    lines = data.splitlines()
    for x in lines:
        if x == "":
            continue
        xs = x.split(':')
        if xs[0] == array:
            user = xs[1]
            password = xs[2]
    return (user, password)

if __name__ == "__main__":
    user = ""
    password = ""
    rubrik_host = ""
    host = ""
    fileset_template = ""
    fs_id = ""
    snap_date = "1970-01-01T01:00"
    date_flag = False
    slf = []
    snap_id = ""
    depth = 0

    optlist, args = getopt.getopt(sys.argv[1:], 'hc:t:d', ['--help', '--creds=', '--timestamp=', '--depth'])
    for opt, a in optlist:
        if opt in ('-h', '--help'):
            usage()
        if opt in ('-c', '--creds'):
            if ':' in a:
                (user, password) = a.split(':')
            else:
                (user, password) = get_creds_from_file(a, 'rubrik')
        if opt in ('-t', '--timestamp'):
            date_flag = True
            snap_date = a
        if opt in ('-d', '--depth'):
            depth = int(a)

    if args[0] == "?":
        usage()
    (rubrik_host, host, fileset) = args
    if user == "":
        user = raw_input("User: ")
    if password == "":
        password = getpass.getpass("Passoword: ")
    rubrik_api = rubrik_cdm.Connect(rubrik_host, user, password)
    rubrik_config = rubrik_api.get('v1', '/cluster/me')
    rubrik_tz = rubrik_config['timezone']['timezone']
    snap_date = datetime.datetime.strptime(snap_date, "%Y-%m-%dT%H:%M")
    local_zone = pytz.timezone(rubrik_tz)
    utc_zone = pytz.timezone("UTC")
    snap_date = local_zone.localize(snap_date)
    snap_date = snap_date.astimezone(pytz.utc)
    rubrik_fileset = rubrik_api.get('v1', '/fileset')
    for fs in rubrik_fileset['data']:
        if fs['hostName'] == host and fs['name'] == fileset:
            fs_id = fs['id']
            break
    if fs_id == "":
        sys.stderr.write("Can't find fileset\n")
    rubrik_snaps = rubrik_api.get('v1', '/fileset/' + str(fs_id))
    for snap in rubrik_snaps['snapshots']:
        sf = snap['date'].split(':')
        slf.append(sf[0])
        slf.append(sf[1])
        date_s = ':'.join(slf)
        date_dt = datetime.datetime.strptime(date_s, "%Y-%m-%dT%H:%M")
        date_dt = utc_zone.localize(date_dt)
        if not date_flag:
            if date_dt > snap_date:
                snap_id = snap['id']
        else:
            if date_dt == snap_date:
                snap_id = snap['id']
    if snap_id == "":
        sys.stderr.write("Can't find snapshot\n")
        exit(2)
    print snap_id





