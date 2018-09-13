#!/usr/bin/env python3
# salloc.py ---
#
# Filename: salloc.py
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
#
#
#

# Code:

import getpass
import json
import os
import subprocess
import sys
import time

from flufl.lock import Lock
from parse import parse

dir_sps = "/var/sps"
dir_gpu = os.path.join(dir_sps, "gpu")
dir_addqueue = os.path.join(dir_sps, "addqueue")
dir_queue = os.path.join(dir_sps, "queue")

lock = Lock(os.path.join(dir_sps, "locks/lock"))


def add_interactive(num_gpu, num_hour):
    """TODO: docstring
    """

    # Get Username
    uname = getpass.getuser()

    # Get PID
    pid = os.getpid()

    # Check user queue directory
    dir_userqueue = os.path.join(dir_addqueue, uname)
    if not os.path.exists(dir_userqueue):
        raise RuntimeError("{} is not setup to use GPUs! Contact admin!")

    # Add an interactive job
    job_name = "{}-{}-{}-{}.job".format(
        time.time(), uname, "salloc", pid
    )
    job_file = os.path.join(dir_userqueue, job_name)
    job_spec = {
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


def write_job(job_fullpath, job_spec):
    """ TODO: Docstring
    """

    # Write the contents to a job
    with lock:
        with open(job_fullpath, "w") as ofp:
            ofp.write(job_spec["cmd"] + "\n")
            ofp.write(job_spec["life"] + "\n")
            ofp.write(job_spec["num_gpu"] + "\n")
            ofp.write(job_spec["start"] + "\n")
            ofp.write(job_spec["end"] + "\n")


def write_env(job_fullpath, env):
    """TODO: write"""

    env_fullpath = job_fullpath.replace(".job", ".env")

    # write env to env_fullpath
    with lock:
        with open(env_fullpath, "w") as ifp:
            env = json.dump(env, ifp)


def get_assigned_gpus():
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
    pid = os.getpid()

    # For all gpu directories
    dir_gpus = [os.path.join(dir_gpu, d) for d in os.listdir(dir_gpu)
                if os.path.isdir(os.path.join(dir_gpu, d))]
    # Look at assigned jobs
    for dir_cur_gpu in dir_gpus:
        for job in os.listdir(dir_cur_gpu):
            job_fullpath = os.path.join(dir_cur_gpu, job)
            # Pass if not a regular file
            if not os.path.isfile(job_fullpath):
                continue
            # Parse and check job info
            parseres = parse("{time}-{user}-{type}-{pid}.job", job)
            if parseres["user"] != uname:
                continue
            if parseres["pid"] != pid:
                continue
            # Add to assigned gpu
            assigned_gpu += [int(dir_cur_gpu.split["/"][-1])]

    return assigned_gpus


def wait_for_gpus(num_gpu):
    """TODO: docstring

    Returns
    -------

    gpu_str: string for the environment variable

    TODO: Add signal catching or some sort to undo the job allocation
    TODO: Add early termination if job disappears from queue
    """
    gpu_ids = []

    # Check GPU folders and see if anything is allocated with my job request
    while True:

        gpu_ids = get_assigned_gpus()
        if len(gpu_ids) == num_gpu:
            break

        # Sleep 10 seconds
        time.sleep(10)

    # Once job is allocated, return the GPU id in string
    gpu_str = ",".join([str(g) for g in gpu_ids])

    return gpu_str


def main(args):

    num_gpu = int(args[1])
    num_hour = float(args[2])

    # Add job to addqueue
    print("Adding interactive job to queue.")
    add_interactive(num_gpu, num_hour)

    # Wait until assigned
    print("Waiting for an available GPU(s)...")
    gpu_str = wait_for_gpus(num_gpu)
    print("GPU(s) {} allocated.".format(gpu_str))

    # Run a sub-process with correct GPU exported
    sub_env = os.environ.copy()
    sub_env["CUDA_VISIBLE_DEVICES"] = gpu_str
    print("-----------------------------------------------------------------------")
    print("Starting shell with CUDA_VISIBLE_DEVICES set, do not edit this variable")
    print("")
    print("Remember to close this interactive once finished to release GPU")
    print("-----------------------------------------------------------------------")
    subprocess.run(
        os.getenv("SHELL", "bash"),
        env=sub_env
    )

    # Print message
    print("-----------------------------------------------------------------------")
    print("GPU(s) {} now released.".format(gpu_str))
    print("-----------------------------------------------------------------------")

    exit(0)


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: salloc <num_gpu> <allocation time in hours>")
        exit(1)

    main(sys.argv)


#
# salloc.py ends here
