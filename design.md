
# Flow of the Local Method Runner

1. Input job id and njs_service URL
1. Look to see cgroup this process is placed in
1. Get hostname of system
Check to see KB_AUTH_TOKEN is defined
Setup logger to log to NJS DB
Check to see if the job was run before or canceled already. If so, log it
Set up callback server vars
Get job inputs from njs db
Validate token
Update job as started and log it
Check to see for existence of /mnt/awe/condor
Mkdir workdir/tmp
Create an input.json from the job params and write to file
Log the worker node / client group 
Get the image version from the catalog
Grab that image from dockerhub using that image version and tag
Check to see if that image exists, and if refdata exists
Set up a log flushing thread
Use admin token to get volume mounts 
Get secure params (e.g. username and password)
Write out config file with all kbase endpoints / secure params
 Run cancellation / finish job checker
Find a free port and Start up callback server with paths to tmp dirs, refdata, volume mounts/binds
Set up labels used for job administration purposes
Run docker or shifter	and keep a record of container id and subjob container ids
Run a thread to check for expired token
Run a thread for 7 day max job runtime
Run a job shutdown hook
Check to see if job completes and returns too much data
Attempt to read output file and see if it is well formed
Throw errors if not
Remove shutdown hook and running threads
Attempt to clean up any running docker containers (if something crashed, for example)

Major subsections
- Job launcher - The part that interfaces to Docker or other container runtimes
- Callback server - The service that can recieve and process calls from jobs
- Logging - The part that handles sending logs to NJS
- Job init - The part that gets the parameters and other info

