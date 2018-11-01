#!/usr/bin/env python3
# config_file.py ---
#
# Filename: config_file.py
# Description:
# Author: wei jiang
# Maintainer:
# Created: 2018.10.31
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

dir_sps = "/var/sps"
dir_gpu = os.path.join(dir_sps, "gpu")
dir_addqueue = os.path.join(dir_sps, "addqueue")
dir_queue = os.path.join(dir_sps, "queue")
lock_file = os.path.join(dir_sps, "locks/lock")

max_wait = 10                   # maximum wait time
sleep_time = 2                  # maximum wait time
