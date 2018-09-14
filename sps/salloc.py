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

import argparse
import getpass
import json
import os
import shutil
import subprocess
import sys
import time

from flufl.lock import Lock

dir_sps = "/var/sps"
dir_gpu = os.path.join(dir_sps, "gpu")
dir_addqueue = os.path.join(dir_sps, "addqueue")
dir_queue = os.path.join(dir_sps, "queue")
lock_file = os.path.join(dir_sps, "locks/lock")


# -----------------------------------------------------------------------------
# Options and configurations

parser = argparse.ArgumentParser()

arg_lists = []

def add_argument_group(name):
    arg = parser.add_argument_group(name)
    arg_lists.append(arg)
    return arg

configs = add_argument_group("Configs")
configs.add_argument("--num_gpu", type=int, default=1,
                     help="Number of gpus to allocate.")
configs.add_argument("--num_hour", type=float, default=1,
                     help="Number of hours. When exceeded the job will be killed.")

def get_config():
    config, unparsed = parser.parse_known_args()

    return config, unparsed

def print_usage():
    parser.print_usage()

# -----------------------------------------------------------------------------
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
    cur_time = time.time()
    job_name = "{}-{}-{}-{}.job".format(
        cur_time, uname, "salloc", pid
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
            json.dump(job_spec, ofp)


def write_env(job_fullpath, env):
    """TODO: write"""

    env_fullpath = job_fullpath.replace(".job", ".env")

    # write env to env_fullpath
    with Lock(lock_file):
        with open(env_fullpath, "w") as ofp:
            json.dump(env, ofp)


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
        # print("Assigne gpus = {}".format(gpu_ids))
        if len(gpu_ids) == num_gpu:
            break
        print("  -- waiting: my pid is {}".format(os.getpid()))

        # Sleep 10 seconds
        time.sleep(2)

    # Once job is allocated, return the GPU id in string
    gpu_str = ",".join([str(g) for g in gpu_ids])

    return gpu_str


def main(config):

    num_gpu = config.num_gpu
    num_hour = config.num_hour

    # Add job to addqueue
    print("* Adding interactive job to queue.")
    add_interactive(num_gpu, num_hour)

    # Wait until assigned
    print("* Waiting for an available GPU(s)...")
    gpu_str = wait_for_gpus(num_gpu)
    print("* GPU(s) with ID={} allocated.".format(gpu_str))

    # Run a sub-process with correct GPU exported
    print("-----------------------------------------------------------------------")
    print("Starting shell with CUDA_VISIBLE_DEVICES set, do not edit this variable")
    print("")
    print("Remember to close this interactive once finished to release GPU")
    print("-----------------------------------------------------------------------")
    # Copy and set env
    sub_env = os.environ.copy()
    sub_env["CUDA_VISIBLE_DEVICES"] = gpu_str
    # Copy RC file to tmp
    shell = os.getenv("SHELL", "bash")
    if shell.endswith("zsh"):
        rcfile = os.path.expanduser("~/.zshrc")
        rcopt = "--rcs"
    elif shell.endswith("bash"):
        rcfile = os.path.expanduser("~/.bashrc")
        rcopt = "--rcfile"
    else:
        raise RuntimeError("{} is not supported".format(shell))
    # Lock interferance
    with Lock(lock_file):
        new_rcfile = os.path.expanduser("~/.spsrc")
        shutil.copy(rcfile, new_rcfile)
        # add export CUDA_VISIBLE_DEVICES at the end
        with open(new_rcfile, "a") as ofp:
            ofp.write("\n\nexport CUDA_VISIBLE_DEVICES={}\n\n".format(
                gpu_str))

        # Launch shell with new rc
        subprocess.run(
            [shell, rcopt, new_rcfile, "source", new_rcfile],
            env=sub_env,
            shell=True
        )

    # Print message
    print("-----------------------------------------------------------------------")
    print("GPU(s) with ID={} now released.".format(gpu_str))
    print("-----------------------------------------------------------------------")

    exit(0)


if __name__ == "__main__":

    # ----------------------------------------
    # Parse configuration
    config, unparsed = get_config()
    # If we have unparsed arguments, print usage and exit
    if len(unparsed) > 0:
        print_usage()
        exit(1)

    main(config)


#
# salloc.py ends here
