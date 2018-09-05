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

import os
import subprocess
import sys


def add_interactive(num_gpu, num_hour):
    """TODO: docstring
    """

    # TODO: Get Username
    # TODO: Get PID
    # TODO: Add to queue as interactive

def get_assigned_gpus():

    assigned_gpus = []

    # TODO: Find assigned gpus

    return assigned_gpus
    

def wait_for_gpus(num_gpu):
    """TODO: docstring
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
    gpu_str = ",".join(gpu_ids)

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
