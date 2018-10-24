#!/usr/bin/env python3

import time, os, sys, hashlib, subprocess, glob, queue, shutil
from multiprocessing import Process, Queue
import platform
import sqlite3

config = {
    'backupdir': "/mnt/copycat",
    'mountdir': "/media/copycat",
    'diskpatterns': ["/dev/sd?", "/dev/mmcblk?", "/dev/da?", "/dev/ada?"],
    'blacklist': [],
    'hardlink': True,
    'debug': True,
    'verbose': True,
}

def Ex(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    c = p.communicate()
    if config['debug']:
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

def get_disks():
    disks = []
    for pattern in config['diskpatterns']:
        patterndisks = glob.glob(pattern)
        for disk in patterndisks:
            disks.append(disk)
    return disks


def get_partitions(disk):
    partitions = glob.glob("{}*".format(disk))
    partitions = [p for p in partitions if p != disk]
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


def copyfile(location, subdir, file, backuptimestamp, q, db = None, numtry = 1):
    if config['verbose']:
        q.put("copying: {} {} {}".format(location, subdir, file))
    elif config['debug']:
        q.put("DEBUG: copyfile: {} {} {}".format(location, subdir, file))
    if numtry > 3:
        q.put("Could not copy {}".format(os.path.join(location, file)))
        return
    backuplocation = os.path.join(config['backupdir'], backuptimestamp)
    src = os.path.join(location, subdir, file)
    dest = os.path.join(backuplocation, subdir, file)
    os.makedirs(os.path.join(backuplocation, subdir), exist_ok=True)
    hash_is_partial = False
    fstat = os.stat(src)
    if fstat.st_size > 33554432:
        hash_is_partial = True
    
    pre_copy_file_hash = hash_file(src, hash_is_partial)

    if config['hardlink'] == True:
        db_hash_table = ['TODO']
        if pre_copy_file_hash in db_hash_table:
            existingfile = db_hash_table[pre_copy_file_hash]
            if config['debug']:
                q.put("DEBUG: ln {} {}".format(existingfile, dest))
            Ex(["ln", existingfile, dest])
            return

    if config['debug']:
        q.put("DEBUG: {}".format(" ".join(["cp", src, dest])))
    Ex(["cp", src, dest])
    post_copy_file_hash = hash_file(dest, hash_is_partial)

    if pre_copy_file_hash is None or post_copy_file_hash is None or pre_copy_file_hash != post_copy_file_hash:
        # file hash does not match
        copyfile(location, subdir, file, backuptimestamp, q, db, numtry = numtry + 1)
    else:
        # file hash matches, ensure file is recorded in database
        cur = db.cursor()
        info = (post_copy_file_hash, backuptimestamp, src, dest)
        cur.execute("INSERT INTO files (hash, backuptime, source, target) VALUES (?, ?, ?, ?);", info)
        db.commit()


def backup_dir(disk_name, srcmount, location, backuptimestamp, q, db = None):
    sourcedir = os.path.join(srcmount, location)
    backupdir = os.path.join(config['backupdir'], backuptimestamp, disk_name)
    os.makedirs(backupdir, exist_ok=True)

    for file in [file for file in os.listdir(sourcedir) if not file in [".",".."]]:
        nfile = os.path.join(sourcedir,file)
        if os.path.isdir(nfile):
            backup_dir(disk_name, srcmount, nfile, backuptimestamp, q, db)
        elif os.path.isfile(nfile):
            subdir = location.lstrip(srcmount).lstrip(os.sep)
            copyfile(srcmount, subdir, file, backuptimestamp, q, db)


def backup(disk, q):
    disklocation = os.path.join(config['mountdir'], disk.split(os.sep)[-1])
    shutil.rmtree(disklocation, ignore_errors=True)
    os.makedirs(disklocation)

    backuptimestamp = time.strftime("%Y-%m-%d_%H_%M-%S")

    db = sqlite3.connect(os.path.join(config['backupdir'], 'files.db'))
    # ensure table is present
    cur = db.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS files (hash TEXT, backuptime TEXT, source TEXT, target TEXT);")
    db.commit()

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
        backup_dir(disk_name, disklocation, "", backuptimestamp, q, db)
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
            backup_dir(partition_name, partitionlocation, "", backuptimestamp, q, db)
            Ex(["umount", partitionlocation])
            os.rmdir(partitionlocation)


if __name__ == '__main__':
    processes = []
    q = Queue()
    last_disks = get_disks()
    if config['debug']:
        print ("Disks already there at startup: {}".format(last_disks))

    # ensure backup directory exists
    os.makedirs(config['backupdir'], exist_ok=True)

    while True:
        time.sleep(3)
        current_disks = get_disks()
        # show current state of disk list
        if config['debug']:
            print ("Disks known: {}".format(current_disks))
        # check for enough free space
        free_space = get_free_space_in_dir(config['backupdir'])
        # check if there are at least 8192 free inodes
        if (free_space['inodes_free'] < (8*1024)):
            print ("WARNING: only {} free inodes for backuptarget {}!".format(free_space['inodes_free'], config['backupdir']))
        # check if at least 1GB is free
        if (free_space['bytes_avail'] < (10*1024*1024*1024)):
            free_mib = free_space['bytes_avail'] / 1024 / 1024
            print ("WARNING: only {} MiB free for backuptarget {}!".format(free_mib, config['backupdir']))
        # iterate over known disks
        for disk in current_disks:
            if disk not in last_disks:
                if disk not in config['blacklist']:
                    time.sleep(3)
                    recheck_disks = get_disks()
                    if disk in recheck_disks:
                        print ("Starting backup of disk {}.".format(disk))
                        p = Process(target=backup, args=(disk, q))
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
