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

import os
import pwd
import sys

from parse import parse

dir_sps = "/var/sps"
dir_addqueue = os.path.join(dir_sps, "addqueue")
dir_queue = os.path.join(dir_sps, "queue")


def list_sub_dir(d):
    return [os.path.join(d, o) for o in os.listdir(d)
            if os.path.isdir(os.path.join(d, o))]


def collect_user_queue():
    """ TODO: Docstring
    """
    new_jobs = []
    for dir_userqueue in list_sub_dir(dir_addqueue):
        for job in os.listdir(dir_userqueue):
            job_fullpath = os.join(dir_userqueue, job)
            # Ignore the ones that are not files
            if not os.isfile(job_fullpath):
                continue
            # Ignore the ones that are not ending with job
            if not job_fullpath.endswith(".job"):
                continue
            try:
                # Try parsing job as dictionary
                parseres = parse("{time}-{user}-{type}-{pid}.job", job)
                if check_job_valid(job_fullpath):
                    # Add parsed job
                    new_jobs += [job_fullpath]
                else:
                    raise RuntimeError("")
            except:
                # TODO: throw error or delete job silently
                print("{} is not a proper job!".format(
                    job_fullpath))

    return new_jobs


def copy_job(job_fullpath, dst):
    """ TODO: Docstring
    """

    print("Copying {} to {}".format(
        job_fullpath, dst))

    # TODO: read the first three lines
    # TODO: create a new file with proper name at `dir_queue`
    job = job_fullpath.split("/")[-1]
    # TODO: write to new file

    # TODO: Make sure to copy env as well

def remove_job(job_fullpath):
    """ TODO: writeme
    """

    os.remove(job_fullpath)
    os.remove(job_fullpath.replace(".job", ".env"))


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

    # TODO: kill the job

    # TODO: delete job file


def read_job(job_fullpath):
    """ TODO: Docstring
    """

    job_spec = {}

    # TODO: Parse the contents of the job

    return job_spec


def check_job_valid(job_fullpath):
    """ TODO: Docstring
    """
    valid = True

    # TODO: check if job is valid

    return valid


def check_job_finished(job_fullpath):
    """ TODO: Docstring
    """
    finished = False

    # TODO: read job contents

    # TODO: check time limit

    # TODO: if job type is salloc, also check if pid is alive

    return finished


def check_gpu_jobs():
    """ TODO: Docstring
    """

    dir_gpus = [os.path.join(dir_sps, d) for d in os.listdir(dir_sps)
                if d.startswith("gpu")]

    # For all gpu directories
    for dir_gpu in dir_gpus:
        for job in os.listdir(dir_gpu):
            job_fullpath = os.path.join(dir_gpu, job)
            # Pass if not a regular file
            if not os.path.isfile(job_fullpath):
                continue
            if check_job_finished(job_fullpath):
                kill_job(job_fullpath)


def get_job():
    """ TODO: docstring
    """

    job_fullpath = None

    # TODO: Try to get the oldest alloc job

    # TODO: Try to get the oldest batch job

    return job_fullpath


def get_free_gpus():
    """ TODO: docstring
    """

    free_gpus = []              # list of integers

    # TODO: Find the list of free GPUS.

    return free_gpus


def assign_job(job_fullpath, free_gpus):
    """ TODO: docstring
    """

    job_spec = read_job(job_fullpath)

    # Check job requirement, and if it fits, copy job to the gpus
    num_gpu = job_spec["num_gpu"]
    if num_gpu <= len(free_gpus):
        # First copy for all gpus
        for gpu in free_gpus[:num_gpu]:
            copy_job(job_fullpath, os.path.join(dir_sps, "gpu{}".format(gpu)))
        # Now delete from queue
        remove_job(job_fullpath)
    else:
        print("Job spec requires {} gpus, we have only {} free".format(
            num_gpu, len(free_gpus)))


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


def read_env(job_fullpath):
    """TODO: write"""
    env = {}

    env_fullpath = job_fullpath.replace(".job", ".env")
    # TODO: read env from env_fullpath

    return env


def run_job(job_fullpath, assigned_gpus):

    # Read job
    job_spec = read_job(job_fullpath)

    # Check job type
    if job_spec["type"] == "salloc":
        print("This is an alloc job, not running")
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
        print("Checking if any process should be killed")
        check_gpu_jobs()

        # Select the oldest asalloc job, if it does not
        print("Grabbing oldest job")
        job_fullpath = get_job()

        # Check if there's a free GPU
        print("Checking GPU availability")
        free_gpus = get_free_gpus()

        # Assign job to GPU by moving the job to the GPU
        print("Assigning job to GPU")
        assigned_gpus = assign_job(job_fullpath, free_gpus)

        # Run job as user
        run_job(job_fullpath, assigned_gpus)

        # Re-schedule after 30 seconds
        time.sleep(30)

    exit(0)


if __name__ == "__main__":

    main(sys.argv)


#
# srunsched.py ends here
