# Introduction

This project provides the API for [LuciusWeb](https://github.com/data-intuitive/LuciusWeb) to talk to. The API is a [Spark Jobserver](https://github.com/spark-jobserver/spark-jobserver) project. It needs to be compiled and the resulting `jar` has to be uploaded to the Spark-Jobserver.

There's still a lot of work to be done on this (version numbers don't reflect everything).

# API

The documentation of the (__old version__) of the API is available in [postman](https://www.getpostman.com/) and [can be found here](https://www.getpostman.com/collections/cf537f6cae9b82c35034).

# Data

Public data is not available yet.

# Local Deployment

The easist approach is to spin up a [Docker](https://www.docker.com/) container containing a full Spark Jobserver stack. We have done some preparations, so that should be easy...

## Setup: Word count

In order to test the setup, we will run an example provided by Spark Jobserver. Please follow the guide available [here](https://github.com/data-intuitive/spark-jobserver): <https://github.com/data-intuitive/spark-jobserver>.

The short version is this:

```bash
docker run -d -p 8090:8090 -v /tmp/api/data:/app/data tverbeiren/jobserver
```

If this step is working, you can proceed to the next one.

## Lucius API

Let's assume you've started the docker container as described earlier.

Start by downloading the latest assembly jar and store it under `/tmp/api`:

```bash
wget http://dl.bintray.com/tverbeiren/maven/com/data-intuitive/luciusapi_2.11/1.5.0/luciusapi_2.11-1.5.0-assembly.jar
mv luciusapi_2.11-1.5.0-assembly.jar /tmp/api/
```

Now, in order to make life easy, some scripts are available. Two configuration files define the behavior of these scripts: `config/settings.sh` and `config/initialize-docker.conf`. Take a look at both to see what is going on. They are configured to work with the procedure described in this manual.

Start Lucius API by running the initialiation script:

```bash
scripts/initialize.sh
```

Please note that the data needs to be present under `/tmp/api/data` for this to work. Looking at the configuration file, the following need to be available:

- The preprocessed dataset, in binary Object Format.
- The gene annotations file

Example output of the initialization step:

```

Initializing LuciusAPI API...

{
  "status": "ERROR",
  "result": "context luciusapi not found"
}{
  "status": "SUCCESS",
  "result": "Jar uploaded"
}{
  "status": "SUCCESS",
  "result": "Context initialized"
}{
  "duration": "Job not done yet",
  "classPath": "com.dataintuitive.luciusapi.initialize",
  "startTime": "2016-11-20T20:32:22.016Z",
  "context": "luciusapi",
  "status": "STARTED",
  "jobId": "008bd94e-7154-4089-849a-353404b68793"
}%
```

If the data is not yet in preprocessed form, a preprocessing endpoint is available. The usage of this is explained later.

We currently provide only one script. The other scripts will be made available once a public test dataset can be uploaded.

```bash
scripts/query-statistics.sh
scripts/query-checkSignature.sh
```

The first scripts should result (depending on the dataset used) in output like this:

```
{
  "jobId": "c35e8929-9d1e-4a9e-9953-e78816a5a1c9",
  "result": {
    "info": "General statistics about the data",
    "header": ["statistic", "value"],
    "data": [["samples", 1344], ["genes", 978], ["compounds", 739]]
  }
}%
```

> Please drop a note if you're interested in using/extending/... this project!