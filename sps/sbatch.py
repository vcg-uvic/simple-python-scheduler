#!/usr/bin/env python3
# sbatch.py --- 
# 
# Filename: sbatch.py
# Description: 
# Author: Kwang Moo Yi
# Maintainer: 
# Created: Tue Sep  4 16:46:15 2018 (-0700)
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
import sys

from flufl.lock import Lock

lock = Lock(os.path.join(dir_sps, "locks/lock"))

def main(args):

    # Get Username

    # Add to queue

    exit(0)

if __name__ == "__main__":

    main(sys.argv)



# 
# sbatch.py ends here
