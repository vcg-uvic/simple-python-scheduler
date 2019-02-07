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

import numpy as np
from flufl.lock import Lock
import salloc_common

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
configs.add_argument("--time", type=str, default="00:01:00", help=""
                     "By default 00:01:00. The maximum permitted time for the "
                     "process to run in dd:hh:mm format. "
                     "When exceeded the job will be killed.")
configs.add_argument("--pid", type=int, default=os.getpid(), help=""
                     "By default pid is salloc pid. "
                     "You can specify pid of a docker container.")

def get_config():
    config, unparsed = parser.parse_known_args()

    # parse time
    if config.time == 'forever' or config.time == 'iambaptiste':
        num_hour = 356.0 * 24.0
    else:
        time_list = config.time.split(":")
        in_hours_list = [24, 1, 1/60]
        in_hours_list = in_hours_list[::-1][:len(time_list)]
        num_hour = 0.0
        for time, in_hours in zip(time_list[::-1], in_hours_list):
            num_hour += float(time) * float(in_hours)
    setattr(config, "num_hour", num_hour)

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


def main(config):

    num_gpu = config.num_gpu
    num_hour = config.num_hour
    pid = config.pid

    # Check quota and availability
    print("* Checking quota and availability")
    if not salloc_common.is_my_quota_valid(num_gpu):
        print("* Quota is not valid, terminating.")
        exit(1)
    else:
        print("* Quota valid.")

    # Add job to addqueue
    print("* Adding interactive job to queue.")
    salloc_common.add_interactive(num_gpu, num_hour, pid)

    # Wait until assigned
    print("* Waiting for an available GPU(s)...")
    gpu_str = salloc_common.wait_for_gpus(num_gpu, pid)
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
    with Lock(salloc_common.lock_file):
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
