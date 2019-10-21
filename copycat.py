#!/usr/bin/env python3

import time, os, sys, hashlib, subprocess, glob, queue, shutil
from multiprocessing import Process, Queue
import platform
import sqlite3
import configparser
import json

cp = configparser.ConfigParser(default_section='copycat')

cp['copycat'] = {
    'backupdir': "/mnt/copycat",
    'mountdir': "/media/copycat",
    'patterns': json.dumps(["/dev/sd?", "/dev/mmcblk?", "/dev/da?", "/dev/ada?"]),
    'blacklist': "",
    'hardlink': "yes",
    'min_free_inodes': 8*1024,
    'min_free_mib': 10*1024,
    'debug': "no",
    'verbose': "yes",
}

configpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
cp.read(configpath)

config = cp['copycat']

def Ex(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    c = p.communicate()
    if config.getboolean('debug'):
        if c[0] is not None:
            print ("STDOUT: {}".format(c[0]))
        if c[1] is not None:
            print ("STDERR: {}".format(c[1]))
    return c[0]

def get_free_space_in_dir(dir):
    statfs = os.statvfs(dir)
    state = {}
    state['blocks_free'] = statfs.f_bfree
    state['blocks_avail'] = statfs.f_bfree
    state['blocksize'] = statfs.f_bsize
    state['inodes_free'] = statfs.f_ffree
    state['bytes_free'] = statfs.f_bfree * statfs.f_bsize
    state['bytes_avail'] = statfs.f_bavail * statfs.f_bsize
    return state

def get_disks():
    disks = []
    patterns = json.loads(config.get('patterns'))
    for pattern in patterns:
        patterndisks = glob.glob(pattern)
        for disk in patterndisks:
            disks.append(disk)
    disks.sort()
    return disks


def get_partitions(disk):
    partitions = glob.glob("{}*".format(disk))
    partitions = [p for p in partitions if p != disk]
    partitions.sort()
    return partitions


def hash_file(file, partial = False):
    block_size = 1024*1024
    m = hashlib.sha512()

    with open(file, 'rb') as f:
        if partial:
            m.update(f.read(block_size))  # start of file
            f.seek(block_size * -1, 2)
            m.update(f.read(block_size))  # end of file
            f.seek(int(f.tell() / 2) - int(block_size / 2))
            m.update(f.read(block_size))  # middle of file
        else:
            while True:
                data = f.read(block_size)
                if not data:
                    break
                m.update(data)
        return m.hexdigest()
    return None


def copyfile(disk_name, location, subdir, file, backuptimestamp, q, config = None, db = None, numtry = 1):
    if config.getboolean('verbose'):
        q.put("copying: {} {}".format(subdir, file))
    elif config.getboolean('debug'):
        q.put("DEBUG: copyfile: {} {} {}".format(location, subdir, file))
    if numtry > 3:
        q.put("Could not copy {}".format(os.path.join(location, subdir, file)))
        return
    backuplocation = os.path.join(config['backupdir'], backuptimestamp, disk_name)
    src = os.path.join(location, subdir, file)
    dest = os.path.join(backuplocation, subdir, file)
    os.makedirs(os.path.join(backuplocation, subdir), exist_ok=True)

    # partial hashing for files bigger than 32 MiB
    hash_is_partial = False
    fstat = os.stat(src)
    if fstat.st_size > 33554432:
        hash_is_partial = True
    
    # hash file
    pre_copy_file_hash = hash_file(src, hash_is_partial)

    if config.getboolean('hardlink'):
        db_hash_table = ['TODO']
        if pre_copy_file_hash in db_hash_table:
            existingfile = db_hash_table[pre_copy_file_hash]
            if config.getboolean('debug'):
                q.put("DEBUG: ln {} {}".format(existingfile, dest))
            Ex(["ln", existingfile, dest])
            return

    if config.getboolean('debug'):
        q.put("DEBUG: {}".format(" ".join(["cp", "-a", src, dest])))
    Ex(["cp", "-a", src, dest])
    post_copy_file_hash = hash_file(dest, hash_is_partial)

    if pre_copy_file_hash is None or post_copy_file_hash is None or pre_copy_file_hash != post_copy_file_hash:
        # file hash does not match
        copyfile(disk_name, location, subdir, file, backuptimestamp, q, config, db, numtry = numtry + 1)
    else:
        if config.getboolean('verbose'):
            q.put("copied: {}".format(file))
        # file hash matches, ensure file is recorded in database
        cur = db.cursor()
        info = (post_copy_file_hash, backuptimestamp, src, dest)
        cur.execute("INSERT INTO files (hash, backuptime, source, target) VALUES (?, ?, ?, ?);", info)
        db.commit()


def backup_dir(disk_name, srcmount, location, backuptimestamp, q, config = None, db = None):
    sourcedir = os.path.join(srcmount, location)
    backupdir = os.path.join(config['backupdir'], backuptimestamp, disk_name)
    os.makedirs(backupdir, exist_ok=True)

    for file in [file for file in os.listdir(sourcedir) if not file in [".",".."]]:
        nfile = os.path.join(sourcedir,file)
        if os.path.isdir(nfile):
            backup_dir(disk_name, srcmount, nfile, backuptimestamp, q, config, db)
        elif os.path.isfile(nfile):
            subdir = location.lstrip(srcmount).lstrip(os.sep)
            copyfile(disk_name, srcmount, subdir, file, backuptimestamp, q, config, db)


def backup(disk, q, config, db):
    disklocation = os.path.join(config['mountdir'], disk.split(os.sep)[-1])
    # remove (sub-)directories previously mounted there
    if (os.path.exists(disklocation) and os.path.isdir(disklocation)):
        os.removedirs(disklocation)

    # recreate the directory
    os.makedirs(disklocation)

    backuptimestamp = time.strftime("%Y-%m-%d_%H_%M-%S")

    ostype = platform.system()
    fstypes = None
    if (ostype == 'FreeBSD'):
        # Kernel modules
        # ext2fs: ext2, ext3, ext4 (pkg: fusefs-ext2)
        # fuse,exfat-fuse: exfat (port: fusefs-exfat)
        # fusefs-ntfs: ntfs (pkg: fusefs-ntfs)
        fstypes = "msdosfs,exfat,ntfs"

    partitions = get_partitions(disk)
    
    if len(partitions) == 0:
        q.put("Mount and backup disk {}.".format(disk))
        if (fstypes is not None):
            # Mount with specific fstypes enabled
            Ex(["mount", "-t", fstypes, "-o", "ro", disk, disklocation])
        else:
            # Mount with fstype autodetected
            Ex(["mount", "-o", "ro", disk, disklocation])
        # disk name
        disk_name = disk.split(os.sep)[-1]
        try:
            backup_dir(disk_name, disklocation, "", backuptimestamp, q, config, db)
        finally:
            Ex(["umount", disklocation])
            os.rmdir(disklocation)
    else:
        for partition in partitions:
            if partition == disk:
                continue
            partition_name = partition.split(os.sep)[-1]
            partitionlocation = os.path.join(disklocation, partition_name)
            os.mkdir(partitionlocation)

            q.put("Mount and backup partition {}.".format(partition))
            if (fstypes is not None):
                # Mount with specific fstypes enabled
                Ex(["mount", "-t", fstypes, "-o", "ro", partition, partitionlocation])
            else:
                # Mount with fstype autodetected
                Ex(["mount", "-o", "ro", partition, partitionlocation])
            time.sleep(2)
            try:
                backup_dir(partition_name, partitionlocation, "", backuptimestamp, q, config, db)
            finally:
                Ex(["umount", partitionlocation])
                os.rmdir(partitionlocation)


if __name__ == '__main__':
    processes = []
    q = Queue()
    last_disks = get_disks()
    if config.getboolean('debug'):
        print ("Disks already there at startup: {}".format(last_disks))

    # ensure backup directory exists
    os.makedirs(config['backupdir'], exist_ok=True)

    db = sqlite3.connect(os.path.join(config['backupdir'], 'files.db'))
    # ensure table is present
    cur = db.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS files (hash TEXT, backuptime TEXT, source TEXT, target TEXT);")
    db.commit()

    while True:
        time.sleep(3)
        current_disks = get_disks()
        # show current state of disk list
        if config.getboolean('debug'):
            print ("Disks known: {}".format(current_disks))
        # check for enough free space
        free_space = get_free_space_in_dir(config['backupdir'])
        # check if there are at least 8192 free inodes
        if (free_space['inodes_free'] < int(config['min_free_inodes'])):
            print ("WARNING: only {} free inodes for backuptarget {}!".format(free_space['inodes_free'], config['backupdir']))
        # check if at least 1GB is free
        free_mib = free_space['bytes_avail'] / 1024 / 1024
        if (free_mib < int(config['min_free_mib'])):
            print ("WARNING: only {} MiB free for backuptarget {}!".format(free_mib, config['backupdir']))
        # iterate over known disks
        for disk in current_disks:
            if disk not in last_disks:
                if disk not in config['blacklist']:
                    time.sleep(3)
                    recheck_disks = get_disks()
                    if disk in recheck_disks:
                        print ("Starting backup of disk {}.".format(disk))
                        p = Process(target=backup, args=(disk, q, config, db))
                        p.start()
                        processes.append((disk, p))

        try:
            while True:
                message = q.get(block=False)
                print (message)
        except queue.Empty:
            pass

        still_running = []
        for disk, process in processes:
            process.join(timeout=1)
            if process.exitcode is None:
                still_running.append((disk, process))
            elif process.exitcode == 0:
                Ex(['sync'])
                print ("Backup of disk {} has finished.".format(disk))
                continue
            elif process.exitcode < 0:
                Ex(['sync'])
                print ("Backup process died from signal {}".format(process.exitcode))
                continue
            elif process.exitcode > 0:
                Ex(['sync'])
                print ("Backup process terminated with exit code {}".format(process.exitcode))
                continue
            else:
                Ex(['sync'])
                print ("Unknown exitcode: {}".format(process.exitcode))
        processes = still_running
        last_disks = current_disks
