#!/usr/bin/env python3
# salloc_common.py ---
#
# Filename: salloc_common.py
# Description:
# Author: Kwang Moo Yi
# Maintainer:
# Created: Tue Sep  4 16:33:02 2018 (-0700)
# Version:
#

# Commentary:
#
#
#
#

# Change Log:
# split salloc.py to two files: salloc_common.py and salloc.py
#
#

# Code:

import argparse
import getpass
import json
import os
import shutil
import subprocess
import sys
import time
import random
import string
import numpy as np
from flufl.lock import Lock

dir_sps = "/var/sps"
dir_gpu = os.path.join(dir_sps, "gpu")
dir_addqueue = os.path.join(dir_sps, "addqueue")
dir_queue = os.path.join(dir_sps, "queue")
lock_file = os.path.join(dir_sps, "locks/lock")

max_wait = 10                   # maximum wait time
sleep_time = 2                  # maximum wait time

# -----------------------------------------------------------------------------
# Misc

def random_str(N=10):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=N))

def get_username():
    return getpass.getuser()

def get_user_homedir():
    return '/home/' + getpass.getuser()

def check_own(dir_path):
    stat_info = os.stat(dir_path)
    uid = stat_info.st_uid
    return uid == os.getuid()

# -----------------------------------------------------------------------------
# Access functions for jobs


def read_job(job_fullpath):
    """ TODO: Docstring
    """

    # Parse the contents of the job
    with Lock(lock_file):
        with open(job_fullpath, "r") as ifp:
            job_spec = json.load(ifp)

    return job_spec


def write_job(job_fullpath, job_spec):
    """ TODO: Docstring
    """

    # Write the contents to a job
    with Lock(lock_file):
        with open(job_fullpath, "w") as ofp:
            json.dump(job_spec, ofp, sort_keys=True)


def write_env(job_fullpath, env):
    """TODO: write"""

    env_fullpath = job_fullpath.replace(".job", ".env")

    # write env to env_fullpath
    with Lock(lock_file):
        with open(env_fullpath, "w") as ofp:
            json.dump(env, ofp, sort_keys=True)


# -----------------------------------------------------------------------------
# For quota

def list_sub_dir(d):
    return [os.path.join(d, o) for o in os.listdir(d)
            if os.path.isdir(os.path.join(d, o))]


def read_quota():
    """TODO: write"""

    quota = {}
    for dir_userqueue in list_sub_dir(dir_addqueue):
        uname = dir_userqueue.split("/")[-1]
        quota_file = dir_userqueue + ".quota"
        quota[uname] = np.loadtxt(quota_file)

    return quota


def check_quota(usage, quota, job_fullpath):

    job_spec = read_job(job_fullpath)
    job_gpu = int(job_spec["num_gpu"])
    if job_spec["user"] in usage:
        user_gpu = usage[job_spec["user"]]
    else:
        user_gpu = 0
    user_quota = quota[job_spec["user"]]

    return user_gpu + job_gpu <= user_quota


def get_gpu_usage():
    """TODO: docstring


    Returns
    -------

    gpu_usage: dictionary

        Returns the a dictionary where each key is gpu and element is a user
        name that is currently using that gpu

    """

    # Dictionary to return
    gpu_usage = {}

    # For all gpu directories
    dir_gpus = [os.path.join(dir_gpu, d) for d in os.listdir(dir_gpu)
                if os.path.isdir(os.path.join(dir_gpu, d))]
    # print("  -- Total of {} gpus found in {}. Checking".format(
    #     len(dir_gpus), dir_gpu))
    # Look at assigned jobs
    for dir_cur_gpu in dir_gpus:
        assigned = False
        cur_gpu_id = int(dir_cur_gpu.split("/")[-1])
        gpu_usage[cur_gpu_id] = []
        for job in os.listdir(dir_cur_gpu):
            job_fullpath = os.path.join(dir_cur_gpu, job)
            # Pass if not a regular file
            if not os.path.isfile(job_fullpath):
                continue
            if not job_fullpath.endswith(".job"):
                continue
            # Read job specs
            job_spec = read_job(job_fullpath)
            # Mark assigned
            # print("  -- {} is not free, {}'s job is there".format(
            #     cur_gpu_id, job_spec["user"]))
            gpu_usage[cur_gpu_id] += [job_spec["user"]]
        # Remove duplicates
        gpu_usage[cur_gpu_id] = set(gpu_usage[cur_gpu_id])

    return gpu_usage


def convert_to_user_usage(gpu_usage):
    """TODO: writeme"""

    alloc = {}
    for gpu in gpu_usage:
        if len(gpu_usage[gpu]) > 0:
            for cur_gpu in gpu_usage[gpu]:
                if cur_gpu not in alloc:
                    alloc[cur_gpu] = [gpu]
                else:
                    alloc[cur_gpu] += [gpu]
    # Get user-based usage number
    usage = {}
    for user in alloc:
        usage[user] = len(set(alloc[user]))

    return usage


# -----------------------------------------------------------------------------
# This script specific functions

def is_my_quota_valid(num_gpu):

    # Get Username
    user = getpass.getuser()

    # Report user quota
    quota = read_quota()
    if user not in quota:
        print("  -- No quota assigned!")
        return False
    print("  -- Allowed number of GPUs is {}".format(int(quota[user])))

    # Report current gpu usage
    usage = convert_to_user_usage(get_gpu_usage())
    num_used = 0
    if user in usage:
        num_used = usage[user]
    print("  -- I'm currently using {} GPUs".format(num_used))

    # Report validity of this allocation
    if num_used + num_gpu > quota[user]:
        print("  -- I cannot allocate {} more GPUs".format(num_gpu))
        return False

    return True


def add_interactive(num_gpu, num_hour, pid):
    """TODO: docstring
    """

    # Get Username
    uname = getpass.getuser()

    # Get PID
    # pid = os.getpid()

    # Check user queue directory
    dir_userqueue = os.path.join(dir_addqueue, uname)
    if not os.path.exists(dir_userqueue):
        raise RuntimeError("{} is not setup to use GPUs! Contact admin!")

    # Add an interactive job
    cur_time = time.time()
    cur_time_read = time.ctime().replace(" ", "-")
    job_name = "{}-{}-{}-{}.job".format(
        cur_time_read, uname, "salloc", pid
    )
    job_file = os.path.join(dir_userqueue, job_name)
    job_spec = {
        "time": str(cur_time),
        "user": uname,
        "type": "salloc",
        "pid": str(pid),
        "cmd": "",
        "life": str(num_hour),
        "num_gpu": str(num_gpu),
        "start": "",
        "end": "",
    }
    # Write the job
    write_job(job_file, job_spec)

    # Write the env
    sub_env = os.environ.copy()
    write_env(job_file, sub_env)


def get_assigned_gpus(pid):
    """ TODO: Docstring

    Returns
    -------
    assigned_gpus: list of int
        Assigned GPU numbers in ints
    """

    assigned_gpus = []

    # Get Username
    uname = getpass.getuser()

    # Get PID
    # pid = os.getpid()

    # For all gpu directories
    dir_gpus = [os.path.join(dir_gpu, d) for d in os.listdir(dir_gpu)
                if os.path.isdir(os.path.join(dir_gpu, d))]
    # Look at assigned jobs
    for dir_cur_gpu in dir_gpus:
        # print("      -- Checking {}".format(dir_cur_gpu))
        for job in os.listdir(dir_cur_gpu):
            job_fullpath = os.path.join(dir_cur_gpu, job)
            # Pass if not a regular file
            if not os.path.isfile(job_fullpath):
                continue
            # Pass if not a job
            if not job_fullpath.endswith(".job"):
                continue
            # read and check job info
            job_spec = read_job(job_fullpath)
            # print("      -- job = {}".format(job))
            if job_spec["type"] != "salloc":
                continue
            if job_spec["user"] != uname:
                continue
            if job_spec["pid"] != str(pid):
                continue
            # Add to assigned gpu
            assigned_gpus += [int(dir_cur_gpu.split("/")[-1])]

    return assigned_gpus


def wait_for_gpus(num_gpu, pid):
    """TODO: docstring

    Returns
    -------

    gpu_str: string for the environment variable

    TODO: Add signal catching or some sort to undo the job allocation
    TODO: Add early termination if job disappears from queue
    """
    gpu_ids = []

    # Check GPU folders and see if anything is allocated with my job request
    wait_time = 0
    while True:

        gpu_ids = get_assigned_gpus(pid)
        # print("Assigne gpus = {}".format(gpu_ids))
        if len(gpu_ids) == num_gpu:
            break
        # print("  -- waiting: my pid is {}".format(os.getpid()))

        # Sleep 10 seconds

        time.sleep(sleep_time)

        # Add number of seconds I waited. If more than `max_wait`, tell user to
        # check queue.
        wait_time += sleep_time
        if wait_time > max_wait:
            print("Maximum wait time reached! Please check queue!")
            exit(1)

    # Once job is allocated, return the GPU id in string
    gpu_str = ",".join([str(g) for g in gpu_ids])

    return gpu_str
