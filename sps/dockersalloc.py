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
import docker



# -----------------------------------------------------------------------------
# Docker specific functions

client = docker.from_env()
dockerd_path = '/var/run/docker.pid'

def check_image_availability(image):
    pass

def spawn_container(image, container_name, uid, gid, container_dir, host_dir, device_list='all', runtime='nvidia', command=''):
    return client.containers.run(image=image, command=command, auto_remove=True, detach=True, name=container_name, environment=['NVIDIA_VISIBLE_DEVICES='+device_list], user=str(uid)+':'+str(gid), volumes={host_dir: {'bind': container_dir, 'mode': 'rw'}}, runtime=runtime, tty=True, stdin_open=True)

def get_dockerd_pid():
    f = open(dockerd_path)
    dockerd_pid = f.readline()
    f.close()
    return int(dockerd_pid)

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
configs.add_argument("--image", type=str, help=""
                     "The name or id of the docker image. "
                     "use `docker images` to check local images, "
                     "or use `docker pull` to pull images from the internet.",
                     required=True)
configs.add_argument("--container_name", type=str, default=salloc_common.random_str(), help=""
                     "The name of the started container. "
                     "By default the container name is a random string. ",)
configs.add_argument("--host_dir", type=str, default=salloc_common.get_user_homedir(), help=""
                     "The mounted dir of the host. "
                     "By default the mounted dir is `/home/USERNAME`. ",)
configs.add_argument("--container_dir", type=str, default='/home/workspace', help=""
                     "The home dir of the started container. "
                     "By default the home dir name `/home/workspace`. ",)
configs.add_argument("--command", type=str, default='', help=""
                     "The command to run in the container. "
                     "By default the command is an empty string. ",)

def get_config():
    config, unparsed = parser.parse_known_args()

    # parse time
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
    image = config.image
    container_name = config.container_name
    if not salloc_common.check_own(config.host_dir):
        raise Exception('Cannot mount host dir, dir ownership do not match: ' + config.host_dir)
    host_dir = config.host_dir
    container_dir = config.container_dir
    command = config.command

    # Start docker container
    spawned_container = spawn_container(image=image, container_name=container_name, uid=os.getuid(), gid=os.getgid(), container_dir=container_dir, host_dir=host_dir)
    top_data = spawned_container.top()
    container_pid = int(top_data['Processes'][0][2])
    pid = container_pid
    
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

    print('export NVIDIA_VISIBLE_DEVICES='+gpu_str)
    # spawned_container.exec_run(['export NVIDIA_VISIBLE_DEVICES='+gpu_str], stdin=True)

    # Run a sub-process with correct GPU exported
    print("-----------------------------------------------------------------------")
    print("Starting docker container with NVIDIA_VISIBLE_DEVICES set")
    print("")
    print("Remember to exit the container once finished to release GPU")
    print("-----------------------------------------------------------------------")    

    # Print message
    print("-----------------------------------------------------------------------")
    print('Run: export NVIDIA_VISIBLE_DEVICES='+gpu_str+' && export NVIDIA_VISIBLE_DEVICES='+gpu_str)
    print("-----------------------------------------------------------------------")

    os.system("docker attach "+container_name)

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
# dockersalloc.py ends here
