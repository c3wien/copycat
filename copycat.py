#!/usr/bin/env python3

import time, os, sys, hashlib, subprocess, glob, queue, shutil
from multiprocessing import Process, Queue

config = {
    'backupdir': "/tmp/copycat",
    'mountdir': "/media/copycat",
    'diskpatterns': ["/dev/sd?", "/dev/mmcblk?", "/dev/da?"],
    'blacklist': [],
    'hardlink': True,
    'debug': True,
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


def copyfile(location, subdir, file, backuptimestamp, q, numtry = 1):
    if config['debug']:
        print ("DEBUG: copyfile: {} {} {}".format(location, subdir, file))
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
        copyfile(location, subdir, file, backuptimestamp, q, numtry = numtry + 1)


def backup_dir(srcmount, location, backuptimestamp, q):
    sourcedir = os.path.join(srcmount, location)
    backupdir = os.path.join(config['backupdir'], backuptimestamp)
    os.makedirs(backupdir, exist_ok=True)

    for file in [file for file in os.listdir(sourcedir) if not file in [".",".."]]:
        nfile = os.path.join(sourcedir,file)
        if os.path.isdir(nfile):
            backup_dir(srcmount, nfile, backuptimestamp, q)
        elif os.path.isfile(nfile):
            subdir = location.lstrip(srcmount).lstrip(os.sep)
            copyfile(srcmount, subdir, file, backuptimestamp, q)


def backup(disk, q):
    disklocation = os.path.join(config['mountdir'], disk.split(os.sep)[-1])
    shutil.rmtree(disklocation, ignore_errors=True)
    os.makedirs(disklocation)

    backuptimestamp = time.strftime("%Y-%m-%d_%H_%M-%S")

    partitions = get_partitions(disk)
    
    if len(partitions) == 0:
        q.put("Mount and backup disk {}.".format(disk))
        Ex(["mount", disk, disklocation])
        backup_dir(disklocation, "", backuptimestamp, q)
        Ex(["umount", disklocation])
        os.rmdir(disklocation)
    else:
        for partition in partitions:
            if partition == disk:
                continue
            partitionlocation = os.path.join(disklocation, partition.split(os.sep)[-1])
            os.mkdir(partitionlocation)

            q.put("Mount and backup partition {}.".format(partition))
            Ex(["mount", partition, partitionlocation])
            time.sleep(2)
            backup_dir(partitionlocation, "", backuptimestamp, q)
            Ex(["umount", partitionlocation])
            os.rmdir(partitionlocation)


if __name__ == '__main__':
    processes = []
    q = Queue()
    last_disks = get_disks()
    if config['debug']:
        print ("Disks already there at startup: {}".format(current_disks))

    while True:
        time.sleep(3)
        current_disks = get_disks()
        if config['debug']:
            print ("Disks known: {}".format(current_disks))
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
                print ("Disk backup of disk {} has finished.".format(disk))
                continue
            elif process.exitcode < 0:
                print ("Disk backup process died from signal {}".format(process.exitcode))
                continue
            elif process.exitcode > 0:
                print ("Disk backup process terminated with exit code {}".format(process.exitcode))
                continue
            else:
                print ("Unknown exitcode: {}".format(process.exitcode))
        processes = still_running
        last_disks = current_disks
