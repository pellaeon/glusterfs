#!/usr/bin/python

# Most of this script was written by M S Vishwanath Bhat (vbhat@redhat.com)

import re
import os
import sys
import xattr
import tempfile


def parse_url(url):
    match = re.search(r'([\w - _ @ \.]+)::([\w - _ @ \.]+)', url)
    if match:
        node = match.group(1)
        vol = match.group(2)
    else:
        print 'given url is not a valid url.'
        sys.exit(1)
    return node, vol


def cleanup(main_mnt, subordinate_mnt):
    try:
        os.system("umount %s" % (main_mnt))
    except:
        print("Failed to unmount the main volume")
    try:
        os.system("umount %s" % (subordinate_mnt))
    except:
        print("Failed to unmount the subordinate volume")

    os.removedirs(main_mnt)
    os.removedirs(subordinate_mnt)


def main():

    mainurl = sys.argv[1]
    subordinateurl = sys.argv[2]
    subordinate_node, subordinatevol = parse_url(subordinateurl)
    main_node, mainvol = parse_url(mainurl)

    main_mnt = tempfile.mkdtemp()
    subordinate_mnt = tempfile.mkdtemp()

    try:
        print "Mounting main volume on a temp mnt_pnt"
        os.system("glusterfs -s %s --volfile-id %s %s" % (main_node,
                                                          mainvol,
                                                          main_mnt))
    except:
        print("Failed to mount the main volume")
        cleanup(main_mnt, subordinate_mnt)
        sys.exit(1)

    try:
        print "Mounting subordinate voluem on a temp mnt_pnt"
        os.system("glusterfs -s %s --volfile-id %s %s" % (subordinate_node, subordinatevol,
                                                          subordinate_mnt))
    except:
        print("Failed to mount the main volume")
        cleanup(main_mnt, subordinate_mnt)
        sys.exit(1)

    subordinate_file_list = [subordinate_mnt]
    for top, dirs, files in os.walk(subordinate_mnt, topdown=False):
        for subdir in dirs:
            subordinate_file_list.append(os.path.join(top, subdir))
        for file in files:
            subordinate_file_list.append(os.path.join(top, file))

    # chdir and then get the gfid, so that you don't need to replace
    gfid_attr = 'glusterfs.gfid'
    ret = 0
    for sfile in subordinate_file_list:
        mfile = sfile.replace(subordinate_mnt, main_mnt)
        if xattr.getxattr(sfile, gfid_attr, True) != xattr.getxattr(
                mfile, gfid_attr, True):
            print ("gfid of file %s in subordinate is different from %s" +
                   " in main" % (sfile, mfile))
            ret = 1

    cleanup(main_mnt, subordinate_mnt)

    sys.exit(ret)


if __name__ == '__main__':
    if len(sys.argv[1:]) < 2:
        print ("Please pass main volume name and subordinate url as arguments")
        print ("USAGE : python <script> <main-host>::<main-vol> " +
               "<subordinate-host>::<subordinate-vol>")
        sys.exit(1)
    main()
