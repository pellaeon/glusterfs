#!/usr/bin/python

import sys
import os
import re
import tempfile
import subprocess
from multiprocessing import Pool
import time
from optparse import OptionParser

subordinate_dict = {}
main_res = ''


def get_arequal_checksum(me, mnt):
    global subordinate_dict
    main_cmd = ['./tests/utils/arequal-checksum', '-p', mnt]
    print "Calculating  "+me+" checksum ..."
    print ""
    p = subprocess.Popen(main_cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    ret = p.wait()
    stdout, stderr = p.communicate()
    if ret:
        print "Failed to get the checksum of " + me + " with following error"
        print stderr
        return 1
    else:
        return stdout


def get_file_count(me, mnt):
    global subordinate_dict
    main_cmd = ['find ' + mnt + ' |wc -l']
    print "Calculating  " + me + " files ..."
    print ""
    p = subprocess.Popen(main_cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=True)
    ret = p.wait()
    stdout, stderr = p.communicate()
    if ret:
        print "Failed to get the count of files in " + me
        + " with following error"
        print stderr
        return 1
    else:
        return stdout.strip()


def compare_checksum(main_mnt, subordinate_dict):
    proc = len(subordinate_dict)+1
    pool = Pool(processes=proc)
    main_res = pool.apply_async(get_arequal_checksum, args=("main",
                                                              main_mnt))
    results = [(subordinate, pool.apply_async(get_arequal_checksum,
                                        args=(subordinate_dict[subordinate]["vol"],
                                              subordinate_dict[subordinate]["mnt"])))
               for subordinate in subordinate_dict]

    pool.close()
    pool.join()
    for subordinate, result in results:
        subordinate_dict[subordinate]["res"] = result.get()
        # exception:  OSError

    main_res = main_res.get()

    print "arequal-checksum of main is : \n %s" % main_res
    for subordinate in subordinate_dict:
        print "arequal-checksum of geo_rep_subordinate %s: \n %s" % (
            subordinate_dict[subordinate]["vol"], subordinate_dict[subordinate]["res"])

    main_files, main_total = re.findall('Total[\s]+:\s(\w+)', main_res)
    main_reg_meta, main_reg = re.findall('Regular files[\s]+:\s(\w+)',
                                             main_res)[1:]
    main_dir_meta, main_dir = re.findall('Directories[\s]+:\s(\w+)',
                                             main_res)[1:]

    ret = 0
    for subordinate in subordinate_dict:
        subordinate_dict[subordinate]["files"], subordinate_dict[subordinate]["total"] = re.findall(
            'Total[\s]+:\s(\w+)', subordinate_dict[subordinate]["res"])
        subordinate_dict[subordinate]["reg_meta"], subordinate_dict[subordinate]["reg"] = re.findall(
            'Regular files[\s]+:\s(\w+)', subordinate_dict[subordinate]["res"])[1:]
        subordinate_dict[subordinate]["dir_meta"], subordinate_dict[subordinate]["dir"] = re.findall(
            'Directories[\s]+:\s(\w+)', subordinate_dict[subordinate]["res"])[1:]

        if main_reg_meta != subordinate_dict[subordinate]["reg_meta"]:
            print ("Meta data checksum for regular files doesn't match " +
                   "between main and  "+subordinate_dict[subordinate]["vol"])
            ret = 67

        if main_dir_meta != subordinate_dict[subordinate]["dir_meta"]:
            print ("Meta data checksum for directories doesn't match " +
                   "between main and "+subordinate_dict[subordinate]["vol"])
            ret = 68

        if main_files != subordinate_dict[subordinate]["files"]:
            print ("Failed to sync all the files from main to " +
                   subordinate_dict[subordinate]["vol"])
            ret = 1

        if main_total != subordinate_dict[subordinate]["total"]:
            if main_reg != subordinate_dict[subordinate]["reg"]:
                print ("Checksum for regular files doesn't match " +
                       "between main and "+subordinate_dict[subordinate]["vol"])
                ret = 1
            elif main_dir != subordinate_dict[subordinate]["dir"]:
                print ("Checksum for directories doesn't match between " +
                       "main and "+subordinate_dict[subordinate]["vol"])
                ret = 1
            else:
                print ("Checksum for symlinks or others doesn't match " +
                       "between main and "+subordinate_dict[subordinate]["vol"])
                ret = 1

        if ret is 0:
            print ("Successfully synced all the files from main " +
                   "to the "+subordinate_dict[subordinate]["vol"])

    return ret


def compare_filecount(main_mnt, subordinate_dict):
    proc = len(subordinate_dict)+1
    pool = Pool(processes=proc)

    main_res = pool.apply_async(get_file_count, args=("main", main_mnt))
    results = [(subordinate, pool.apply_async(get_file_count,
                                        args=(subordinate_dict[subordinate]["vol"],
                                              subordinate_dict[subordinate]["mnt"])))
               for subordinate in subordinate_dict]

    pool.close()
    pool.join()
    for subordinate, result in results:
        subordinate_dict[subordinate]["res"] = result.get()

    main_res = main_res.get()
    ret = 0
    for subordinate in subordinate_dict:
        if not main_res == subordinate_dict[subordinate]["res"]:
            print ("files count between main and " +
                   subordinate_dict[subordinate]["vol"]+" doesn't match yet")
            ret = 1

    return ret


def parse_url(url):
    match = re.search(r'([\w - _ @ \.]+)::([\w - _ @ \.]+)', url)
    if match:
        node = match.group(1)
        vol = match.group(2)
    else:
        print 'given url is not a valid.'
        sys.exit(1)
    return node, vol


def cleanup(main_mnt, subordinate_dict):
    try:
        os.system("umount %s" % (main_mnt))
    except:
        print("Failed to unmount the main volume")

    for subordinate in subordinate_dict:

        try:
            os.system("umount %s" % (subordinate_dict[subordinate]["mnt"]))
            os.removedirs(subordinate_dict[subordinate]["mnt"])
        except:
            print("Failed to unmount the "+subordinate+" volume")

    os.removedirs(main_mnt)


def main():

    subordinates = args[1:]

    mainurl = args[0]
    main_node, mainvol = parse_url(mainurl)
    main_mnt = tempfile.mkdtemp()

    i = 1
    for subordinate in subordinates:
        subordinate_dict["subordinate"+str(i)] = {}
        subordinate_dict["subordinate"+str(i)]["node"], subordinate_dict[
            "subordinate"+str(i)]["vol"] = parse_url(subordinate)
        subordinate_dict["subordinate"+str(i)]["mnt"] = tempfile.mkdtemp()
        i += 1

    try:
        print ("mounting the main volume on "+main_mnt)
        os.system("glusterfs -s  %s --volfile-id %s %s" % (main_node,
                                                           mainvol,
                                                           main_mnt))
        time.sleep(3)
    except:
        print("Failed to mount the main volume")

    for subordinate in subordinate_dict:
        print subordinate
        print subordinate_dict[subordinate]
        try:
            print ("mounting the subordinate volume on "+subordinate_dict[subordinate]['mnt'])
            os.system("glusterfs -s %s --volfile-id %s %s" % (
                subordinate_dict[subordinate]["node"], subordinate_dict[subordinate]["vol"],
                subordinate_dict[subordinate]["mnt"]))
            time.sleep(3)
        except:
            print("Failed to mount the "+subordinate+" volume")

    res = 0
    if option.check == "arequal":
        res = compare_checksum(main_mnt, subordinate_dict)
    elif option.check == "find":
        res = compare_filecount(main_mnt, subordinate_dict)
    else:
        print "wrong options given"

    cleanup(main_mnt, subordinate_dict)

    sys.exit(res)


if __name__ == '__main__':

    usage = "usage: %prog [option] <main-host>::<main-vol> \
    <subordinate1-host>::<subordinate1-vol> . . ."
    parser = OptionParser(usage=usage)
    parser.add_option("-c", dest="check", action="store", type="string",
                      default="arequal",
                      help="size of the files to be used [default: %default]")
    (option, args) = parser.parse_args()
    if not args:
        print "usage: <script> [option] <main-host>::<main-vol>\
         <subordinate1-host>::<subordinate1-vol> . . ."
        print ""
        sys.exit(1)

    main()
