# Simple Python Scheduler

This is simple python scheduler for a multi-user multi-GPU server. As of now,
this scheduler works purely based on trust that all users will use this to run
their scripts. This script is **NOT INTENDED** for complex environments that
needs security.

Right now, the only thing it can do is allocate GPUs to users on demand, and
kill any processes that are intruding. It will also kill processes that
exceeded the initial lifetime.


# Dependancies

```
flufl.lock
psutil
nvidia-ml-py3
```

# Supported Commands

## `salloc`

Will start an interactive shell with the correct GPU allocated. Closing the
shell will result in releasing the GPU.

## `srunsched`

Run the scheduler.


# Planned Commands

## `sbatch`

Will queue the job for execution. First queued object will run. We won't have
any priority settings or limitations for now. Must define up-time for the job.

## `squeue`

Read and report the current queue.

## `susage`

Report wall time of all users. (later)

# Directory to be monitored

Queue will be located at `/var/sps/queue`

Add queue will be located at `/var/sps/addqueue/<user>`

Quota for GPU will be located at `/var/sps/addqueue/<user>.quota`. Will only
have effect when `/var/sps/addqueue/<user>` exists.

Current job at GPU will be at `/var/sps/gpu/X`

# Job file

Jobs will be named 

`<time>-<user>-<type>-<pid>.job`

and the corresponding shell environment for the batch job

`<time>-<user>-<type>-<pid>.env`

Time will be from Python module `time.time()`.  

`pid` will be the pid of the
job submitter.

`type` will be either `salloc` or `sbatch`.

All job files and env files are in json format.

# TODO

* All variables and functions are now contained in a single file for each
  instance. Structure this better.

# Known Vulnerabilities

- [ ] The lock file can arbitrarilly be deleted. 


