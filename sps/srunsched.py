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
import sys

def main(args):

    print("Starting Scheduler")
    while True:

        # Read Queue Addition Pool (Directory)
        print("Reading queue addition pool")

        # Add job to current Queue file
        print("Adding job to current queue")

        # Check liftime of processes, as well as validity of any interactive job
        print("Checking if any process should be killed")

        # Check if there's a free GPU
        print("Checking GPU availability")

        # Assign job to GPU by moving the job to the GPU
        print("Assigning and blocking GPU")


        

    exit(0)

if __name__ == "__main__":

    main(sys.argv)



# 
# srunsched.py ends here
