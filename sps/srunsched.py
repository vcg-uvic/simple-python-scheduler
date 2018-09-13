# srunsched.py ---
#
# Filename: srunsched.py
# Description:
# Author: Kwang Moo Yi
# Maintainer:
# Created: Tue Sep  4 16:48:37 2018 (-0700)
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

import json
import os
import pwd
import shutil
import signal
import sys
import time

import numpy as np
from flufl.lock import Lock
from parse import parse

import psutil

dir_sps = "/var/sps"
dir_gpu = os.path.join(dir_sps, "gpu")
dir_addqueue = os.path.join(dir_sps, "addqueue")
dir_queue = os.path.join(dir_sps, "queue")

lock = Lock(os.path.join(dir_sps, "locks/lock"))


def list_sub_dir(d):
    return [os.path.join(d, o) for o in os.listdir(d)
            if os.path.isdir(os.path.join(d, o))]


def collect_user_queue():
    """ TODO: Docstring
    """
    new_jobs = []
    for dir_userqueue in list_sub_dir(dir_addqueue):
        for job in os.listdir(dir_userqueue):
            job_fullpath = os.path.join(dir_userqueue, job)
            # Ignore the ones that are not files
            if not os.path.isfile(job_fullpath):
                continue
            # Ignore the ones that are not ending with job
            if not job_fullpath.endswith(".job"):
                continue
            try:
                # Try parsing job as dictionary
                if check_job_valid(job_fullpath):
                    # Add parsed job
                    new_jobs += [job_fullpath]
                else:
                    raise RuntimeError("")
            except:
                # TODO: throw error or delete job silently
                print("{} is not a proper job!".format(
                    job_fullpath))
                remove_job(job_fullpath)

    return new_jobs


def copy_job(job_fullpath, dst):
    """TODO: Docstring

    Whenever copying, the scheduler will check time and update start and end
    times.

    Parameters
    ----------
    job_fullpath: string
        Full path to the job file 
    dst: string
        Destination directory. File name will automatically be created.

    """

    print("Copying {} to {}".format(
        job_fullpath, dst))

    # read specs of the job
    job_spec = read_job(job_fullpath)

    # set specs, i.e. start and end time
    cur_time = time.time()
    job_spec["start"] = str(cur_time)
    job_spec["end"] = str(cur_time + 60 * 60 * float(job_spec["life"]))

    # write job to new file
    job = job_fullpath.split("/")[-1]
    write_job(os.path.join(dst, job), job_spec)

    # copy env as well
    shutil.copy(
        job_fullpath.replace(".job", ".env"),
        os.path.join(dst, job.replace(".job", ".env")),
    )


def move_jobs_to_queue(new_jobs):
    """ TODO: Docstring
    """

    # For each new job
    for job_fullpath in new_jobs:
        copy_job(job_fullpath, dir_queue)
        remove_job(job_fullpath)


def kill_job(job_fullpath):
    """ TODO: Docstring
    """

    # kill the job
    job_spec = read_job(job_fullpath)
    # Kill 9 for now. No graceful exit. Don't trust the user.
    if psutil.pid_exists(job_spec["pid"]):
        os.kill(job_spec["pid"], signal.SIGKILL)
        # os.kill(job_spec["pid"], signal.SIGTERM)

    # delete job file
    remove_job(job_fullpath)


def check_job_valid(job_fullpath):
    """ TODO: Docstring
    """
    valid = True

    # check if job is valid by simply reading it
    try:
        job_specs = read_job(job_fullpath)
        env = read_env(job_fullpath)
    except:
        valid = False

    return valid


def check_job_finished(job_fullpath):
    """ TODO: Docstring

    """

    # read job contents
    job_specs = read_job(job_fullpath)

    # check time limit
    if float(job_specs["end"]) < time.time():
        return True

    # check if pid is alive
    if not psutil.pid_exists(job_spec["pid"]):
        return True

    # otherwise return false
    return False


def check_gpu_jobs():
    """ TODO: Docstring
    """

    dir_gpus = [os.path.join(dir_gpu, d) for d in os.listdir(dir_gpu)
                if os.path.isdir(os.path.join(dir_gpu, d))]

    # For all gpu directories
    for dir_cur_gpu in dir_gpus:
        for job in os.listdir(dir_cur_gpu):
            job_fullpath = os.path.join(dir_cur_gpu, job)
            # Pass if not a regular file
            if not os.path.isfile(job_fullpath):
                continue
            # Kill finished jobs
            if check_job_finished(job_fullpath):
                kill_job(job_fullpath)


def get_job():
    """TODO: docstring

    Returns
    -------
    job_fullpath: str

        Returns the full path to the job to run. Returns None if there's no job
        to run.

    """

    job_fullpath = None

    # Get all jobs
    jobs = [j for j in os.listdir(dir_queue) if
            j.endswith(".job")]

    # Sort job according to time
    jobs = np.sort(jobs)

    # Try to get the oldest alloc job
    for job in jobs:
        job_spec = parse("{time}-{user}-{type}-{pid}.job", job)
        if job_spec["type"] == "salloc":
            job_fullpath = os.path.join(dir_queue, job)
            return job_fullpath

    # Try to get the oldest batch job
    for job in jobs:
        job_spec = parse("{time}-{user}-{type}-{pid}.job", job)
        if job_spec["type"] == "sbatch":
            job_fullpath = os.path.join(dir_queue, job)
            return job_fullpath

    return job_fullpath


def get_free_gpus():
    """ TODO: docstring


    Returns
    -------

    free_gpus: list of int

        Returns the list of free GPUs
    """

    free_gpus = []              # list of integers

    # Find the list of free GPUS.

    # For all gpu directories
    dir_gpus = [os.path.join(dir_gpu, d) for d in os.listdir(dir_gpu)
                if os.path.isdir(os.path.join(dir_gpu, d))]
    print("Total of {} gpus found in {}. Checking".format(
        len(dir_gpus), dir_gpu))
    # Look at assigned jobs
    for dir_cur_gpu in dir_gpus:
        assigned = False
        for job in os.listdir(dir_cur_gpu):
            job_fullpath = os.path.join(dir_cur_gpu, job)
            # Pass if not a regular file
            if not os.path.isfile(job_fullpath):
                continue
            # Mark assigned
            print("{} is not free, {} is there".format(
                dir_cur_gpu, job))
            assigned = True
        if not assigned:
            free_gpus += [int(dir_cur_gpu.split("/")[-1])]

    return free_gpus


def assign_job(job_fullpath, free_gpus):
    """ TODO: docstring
    
    Returns
    -------

    job_fullpath: str
        Full path to the new job script located under gpu
    
    assigned_gpus: list of int
        List of assigned GPUs.
    """

    if job_fullpath is None:
        print("No job to assign")
        return

    job_spec = read_job(job_fullpath)

    # TODO: Randomly permute GPU

    # Check job requirement, and if it fits, copy job to the gpus
    num_gpu = int(job_spec["num_gpu"])
    if num_gpu <= len(free_gpus):
        # First copy for all gpus
        for gpu in free_gpus[:num_gpu]:
            new_dir = os.path.join(dir_sps, "gpu/{}".format(gpu)) 
            copy_job(job_fullpath, new_dir)
        # Now delete from queue
        remove_job(job_fullpath)
        # Return the new job script in certain gpu to run
        job_fullpath = os.path.join(new_dir, job_fullpath.split("/")[-1])
        assigned_gpus = free_gpus[:num_gpu]
    else:
        print("Job spec requires {} gpus, we have only {} free".format(
            num_gpu, len(free_gpus)))
        job_fullpath = None
        assigned_gpus = None

    return job_fullpath, assigned_gpus

def demote_to(user):
    """ TODO: writeme
    """

    user_info = pwd.getpwnam(user)

    def setids():
        print("Starting demotion to user {}".format(user))
        os.setgid(user_info.pw_gid)
        os.setuid(user_info.pw_uid)
        print("Process demoted to user {}".format(user))

    return setids


def read_job(job_fullpath):
    """ TODO: Docstring
    """

    # First parse the job name
    job = job_fullpath.split("/")[-1]
    job_spec = parse("{time}-{user}-{type}-{pid}.job", job).named

    # Parse the contents of the job
    with lock:
        with open(job_fullpath, "r") as ifp:
            # TODO: Limit maximum
            contents = ifp.readlines()
            assert len(contents) == 3 or len(contents) == 5
            job_spec["cmd"] = contents[0].rstrip("\n")
            job_spec["life"] = contents[1].rstrip("\n")
            job_spec["num_gpu"] = contents[2].rstrip("\n")
            if len(contents) > 3:
                job_spec["start"] = contents[3].rstrip("\n")
                job_spec["end"] = contents[4].rstrip("\n")

    return job_spec


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

def remove_job(job_fullpath):
    """ TODO: writeme
    """

    with lock:
        if os.path.exists(job_fullpath):
            os.remove(job_fullpath)
        if os.path.exists(job_fullpath.replace(".job", ".env")):
            os.remove(job_fullpath.replace(".job", ".env"))


def read_env(job_fullpath):
    """TODO: write"""

    env_fullpath = job_fullpath.replace(".job", ".env")

    # read env from env_fullpath
    with lock:
        with open(env_fullpath, "r") as ifp:
            env = json.load(ifp)

    return env


def write_env(job_fullpath, env):
    """TODO: write"""

    env_fullpath = job_fullpath.replace(".job", ".env")

    # write env to env_fullpath
    with lock:
        with open(env_fullpath, "w") as ifp:
            env = json.dump(env, ifp)


def run_job(job_fullpath, assigned_gpus):

    if job_fullpath is None:
        print("No job to run")
        return

    # Read job
    job_spec = read_job(job_fullpath)

    # Check job type
    if job_spec["type"] == "salloc":
        print("This is an alloc job, assigned, but not running")
        return

    # Read the environment
    sub_env = read_env(job_fullpath)

    # Run the command line from the job
    gpu_str = ",".join(assigned_gpus)
    sub_env["CUDA_VISIBLE_DEVICES"] = gpu_str

    print("Running job with CUDA_VISIBLE_DEVICES={}".format(gpu_str))
    subprocess.Popen(
        job_spec["cmd"],
        preexec_fn=demote_to(job_spec["user"]),
        env=sub_env,
        shell=True,
    )


def main(args):

    print("Starting Scheduler")
    while True:

        # Read Queue Addition Pool (Directory)
        print("Reading queue addition pool")
        new_jobs = collect_user_queue()

        # Add job to current Queue file
        print("Adding job to current queue")
        move_jobs_to_queue(new_jobs)

        # Check liftime of processes, as well as validity of any interactive job
        print("Checking if any process should be killed and killing if finished")
        check_gpu_jobs()

        # Select the oldest asalloc job, if it does not
        print("Grabbing oldest job")
        job_fullpath = get_job()
        print("Grabbed {}".format(job_fullpath))

        # Check if there's a free GPU
        print("Checking GPU availability")
        free_gpus = get_free_gpus()

        # Assign job to GPU by moving the job to the GPU
        print("Assigning job to GPU")
        job_fullpath, assigned_gpus = assign_job(job_fullpath, free_gpus)

        # Run job as user
        run_job(job_fullpath, assigned_gpus)

        # Re-schedule after 30 seconds
        time.sleep(30)

    exit(0)


if __name__ == "__main__":

    main(sys.argv)


#
# srunsched.py ends here
