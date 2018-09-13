# Simple Python Scheduler

This is simple python scheduler for a multi-user multi-GPU server. As of now,
this scheduler works purely based on trust that all users will use this to run
their scripts. This script is **NOT INTENDED** for complex environments that
needs security.

# Milestones

- [ ] Implement `salloc.py`
- [ ] Implement `srunsched.py`, except for the `read_env`
- [ ] Test `salloc.py`
- [ ] Implement `sbatch.py`
- [ ] Finish `srunsched.py`
- [ ] Test `sbatch.py`

# Supported Commands

## `salloc`

Will start an interactive shell with the correct GPU allocated. Closing the
shell will result in releasing the GPU.

## `sbatch`

Will queue the job for execution. First queued object will run. We won't have
any priority settings or limitations for now. Must define up-time for the job.

## `squeue`

Read and report the current queue.

## `susage`

Report wall time of all users. (later)

## `srunsched`

Run the scheduler.

# Directory to be monitored

Queue will be located at `/var/sps/queue`

Add queue will be located at `/var/sps/addqueue/<user>`

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


Each job file will have inside a structure as the following:

```
shell command_to_run (for interactive jobs, blank)
life time
number of gpu
start time (written by the scheduler)
end time (written by the scheduler)
```

Which will be parsed respectively with keywords "cmd", "life", "num_gpu",
"start", "end".


# TODO

* All variables and functions are now contained in a single file for each
  instance. Structure this better.

# Known Vulnerabilities

- [ ] The lock file can arbitrarilly be deleted. 


