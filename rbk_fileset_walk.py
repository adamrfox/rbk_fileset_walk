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

def dprint(message):
    if DEBUG:
        print(message)

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

def walk_dir(rubrik_api, snap_id, path, parent_ent):
    tree_size[path] = 0
    tree_files[path] = 0
    offset = 0
    done = False
    while not done:
        params = {"path": path, "offset": offset}
        dprint(params)
        dprint(snap_id)
        if VERBOSE:
            print(". ")
        rubrik_walk = rubrik_api.get('v1', '/fileset/snapshot/' + str(snap_id) + '/browse', params=params)
        for dir_ent in rubrik_walk['data']:
            offset += 1
            if dir_ent == parent_ent:
                return(tree_size[path], tree_files[path])
            if dir_ent['fileMode'] == "file":
                tree_files[path] += 1
                tree_size[path] += int(dir_ent['size'])
            elif dir_ent['fileMode'] == "directory" or dir_ent['fileMode'] == "drive":
                tree_files[path] += 1
                if dir_ent['fileMode'] == "drive":
                    new_path = dir_ent['filename']
                elif share_type == 'NFS' or fs_type in ('linux', 'unix'):
                    if path != "/":
                        new_path = path + "/" + dir_ent['path']
                    else:
                        new_path = "/" + dir_ent['path']
                else:
                    if path != "\\":
                        new_path = path + "\\" + dir_ent['path']
                    else:
                        new_path = "\\" + dir_ent['path']
                (tree_size[new_path], tree_files[new_path]) = walk_dir(rubrik_api, snap_id, new_path, dir_ent)
                tree_size[path] += tree_size[new_path]
                tree_files[path] += tree_files[new_path]
        if not rubrik_walk['hasMore']:
            done = True
    return (tree_size[path], tree_files[path])


if __name__ == "__main__":
    user = ""
    password = ""
    rubrik_host = ""
    host = ""
    fileset_template = ""
    fs_id = ""
    snap_date = "1970-01-01T01:00"
    date_flag = False
    snap_id = ""
    depth = 0
    path = "/"
    done = False
    tree_files = {}
    tree_size = {}
    fs_type = ""
    hs_id = ""
    share_type = ""
    DEBUG = False
    VERBOSE = False

    optlist, args = getopt.getopt(sys.argv[1:], 'hc:t:dp:Dv', ['--help', '--creds=', '--timestamp=', '--depth=', '--path=', '--DEBUG', '--VERBOSE'])
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
        if opt in ('-p', '--path'):
            path = a
        if opt in ('-D', '--DEBUG'):
            DEBUG = True
        if opt in ('-v', '--VERBOSE'):
            VERBOSE = True
    if args[0] == "?":
        usage()
    if args[2] == "nas":
        (rubrik_host, host, fs_type, share, fileset) = args
        if share.startswith("/"):
            share_type = "NFS"
        else:
            share_type = "SMB"
            path = "\\"
    elif args[2] == "windows":
        (rubrik_host, host, fs_type, fileset) = args
        path = "\\"
    elif args[2] == "unix" or args[2] == "linux":
        (rubrik_host, host, fs_type, fileset) = args
        path = "/"
    else:
        sys.stderr.write("Invalid fileset type: " + args[2] + "\n")
        exit(2)
    if user == "":
        user = raw_input("User: ")
    if password == "":
        password = getpass.getpass("Password: ")
    rubrik_api = rubrik_cdm.Connect(rubrik_host, user, password)
    rubrik_config = rubrik_api.get('v1', '/cluster/me')
    rubrik_tz = rubrik_config['timezone']['timezone']
    snap_date = datetime.datetime.strptime(snap_date, "%Y-%m-%dT%H:%M")
    local_zone = pytz.timezone(rubrik_tz)
    utc_zone = pytz.timezone("UTC")
    snap_date = local_zone.localize(snap_date)
    snap_date = snap_date.astimezone(pytz.utc)
    if fs_type in ('windows', 'linux', 'unix'):
        rubrik_fileset = rubrik_api.get('v1', '/fileset?name=' + fileset + '&host=' + host)
        for fs in rubrik_fileset['data']:
            if fs['hostName'] == host and fs['name'] == fileset:
                fs_id = fs['id']
                break
    elif fs_type == "nas":
        rubrik_hostshare = rubrik_api.get('internal', '/host/share?share_type' + share_type)
        for hs in rubrik_hostshare['data']:
            if hs['hostname'] == host and hs['exportPoint'] == share:
                hs_id = hs['id']
                break
        if hs_id == "":
            sys.stderr.write("Can't find share\n")
            exit(1)
        rubrik_fileset = rubrik_api.get('v1', '/fileset?name=' + fileset + "&share_id=" + str(hs_id))
        for fs in rubrik_fileset['data']:
            if fs['name'] == fileset:
                fs_id = fs['id']
                break
    if fs_id == "":
        sys.stderr.write("Can't find fileset: " + fileset + "\n")
        exit(1)
    rubrik_snaps = rubrik_api.get('v1', '/fileset/' + str(fs_id))
    for snap in rubrik_snaps['snapshots']:
        slf = []
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
    (tree_size[path], tree_files[path]) = walk_dir(rubrik_api, snap_id, path, {})

    print ('\n')
    for x in sorted(tree_size.keys()):
        print x + "," + str(tree_size[x]) + "," + str(tree_files[x])

