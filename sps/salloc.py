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
# Options and configurations

parser = argparse.ArgumentParser()

arg_lists = []


def add_argument_group(name):
    arg = parser.add_argument_group(name)
    arg_lists.append(arg)
    return arg


configs = add_argument_group("Configs")
configs.add_argument("--gres", type=str, default="gpu:1", help=""
                     "By default gpu:1. To allocate more than one gpus, use "
                     "gpu:X, wher X is the number of gpus wanted")
configs.add_argument("--num_hour", type=float, default=1, help=""
                     "By default 1. Number of hours that the process should run "
                     "at max. When exceeded the job will be killed.")


def get_config():
    config, unparsed = parser.parse_known_args()

    # parse gres
    gres_list = config.gres.split(",")
    for gres in gres_list:
        gres = gres.split(":")
        if gres[0] == "gpu":
            setattr(config, "num_gpu", int(gres[1]))
        else:
            raise RuntimeError("Unknown!")

    return config, unparsed


def print_usage():
    parser.print_usage()

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
            json.dump(job_spec, ofp)


def write_env(job_fullpath, env):
    """TODO: write"""

    env_fullpath = job_fullpath.replace(".job", ".env")

    # write env to env_fullpath
    with Lock(lock_file):
        with open(env_fullpath, "w") as ofp:
            json.dump(env, ofp)


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
    print("  -- Allowed number of GPUs is {}".format(quota[user]))

    # Report current gpu usage
    usage = convert_to_user_usage(get_gpu_usage())
    print("  -- I'm currently using {} GPUs".format(usage[user]))

    # Report validity of this allocation
    if usage[user] + num_gpu > quota[user]:
        print("  -- I cannot allocate {} more GPUs".format(num_gpu))
        return False

    return True


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
    wait_time = 0
    while True:

        gpu_ids = get_assigned_gpus()
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


def main(config):

    num_gpu = config.num_gpu
    num_hour = config.num_hour

    # Check quota and availability
    print("* Checking quota and availability")
    if is_my_quota_valid(num_gpu):
        print("* Quota is not valid, terminating.")
        exit(1)

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
    # Create a new rc file to folder
    new_rc_dir = os.path.expanduser("~/.spsrc")
    if not os.path.isdir(new_rc_dir):
        if os.path.exists(new_rc_dir):
            os.remove(new_rc_dir)
    if not os.path.exists(new_rc_dir):
        os.makedirs(new_rc_dir)
    # Firgure out shell and adapt the command to rund
    shell = os.getenv("SHELL", "bash")
    if shell.endswith("zsh"):
        rcfile = os.path.expanduser("~/.zshrc")
        new_rcfile = os.path.join(new_rc_dir, rcfile.split("/")[-1])
        prompt_var = "PS1"
        cmd = "ZDOTDIR={} {}".format(new_rc_dir, shell)
    elif shell.endswith("bash"):
        rcfile = os.path.expanduser("~/.bashrc")
        new_rcfile = os.path.join(new_rc_dir, rcfile.split("/")[-1])
        prompt_var = "PS1"
        cmd = "{} --rcfile {}".format(shell, new_rcfile)
    else:
        raise RuntimeError("{} is not supported".format(shell))
    # Lock to prevent interferance
    with Lock(lock_file):
        # copy rc file to new directory
        shutil.copy(rcfile, new_rcfile)
        # add export CUDA_VISIBLE_DEVICES at the end
        with open(new_rcfile, "a") as ofp:
            # ofp.write("\necho SOURCING MODIFIED\n")
            ofp.write("\n\nexport CUDA_VISIBLE_DEVICES={}\n\n".format(
                gpu_str))
            ofp.write("\n\nexport {}=\"(GPU={}) \"${}\n\n".format(
                prompt_var, gpu_str, prompt_var))

    # Launch shell with new rc
    subprocess.run(cmd, env=sub_env, shell=True)

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
