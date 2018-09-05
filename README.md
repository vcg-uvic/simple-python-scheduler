# Simple Python Scheduler

This is simple python scheduler for a multi-user multi-GPU server. As of now,
this scheduler works purely based on trust that all users will use this to run
their scripts. This script is **NOT INTENDED** for complex environments that
needs security.

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

Current job at GPU will be at `/var/sps/gpuX`

# Job file

Jobs will be named 

`<user>-<type>-<time/pid>`

Time will be from Python module `time.time()`


Each job file will have inside a structure as the following:

```
shell command_to_run (for interactive jobs, blank)
life time
number of gpu
start time (written by the scheduler)
end time (written by the scheduler)
```




