# Introduction

This project provides the API for [LuciusWeb](https://github.com/data-intuitive/LuciusWeb). The API is a [Spark Jobserver](https://github.com/spark-jobserver/spark-jobserver) project. It needs to be compiled and the resulting `jar` has to be uploaded to the Spark-Jobserver.

There's still a lot of work to be done on this (version numbers don't reflect everything).

# Dependencies

| LuciusAPI | LuciusCore | Spark Jobserver | Spark |
|-----------|------------|-----------------|-------|
| 5.0.0     | 4.0.10     | 0.11.1          | 2.4.7 |
| 5.0.1     | 4.0.11     | 0.11.1          | 2.4.7 |
| 5.1.0     | 4.1.1      | 0.11.1          | 2.4.7 |
| 5.1.1     | 4.1.1      | 0.11.1          | 2.4.7 |
| 5.1.2     | 4.1.1      | 0.11.1          | 2.4.7 |
| 5.1.3     | 4.1.1      | 0.11.1          | 2.4.7 |
| 5.1.4     | 4.1.2      | 0.11.1          | 2.4.7 |

# API Documentation

The documentation of the (__old version__) of the API is available in [postman](https://www.getpostman.com/) and [can be found here](https://www.getpostman.com/collections/cf537f6cae9b82c35034).

# Data

Public data is not available (yet).

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

Let's assume you've started Spark Jobserver (in any way), then:

1. Clone the LuciusAPI repo

2. Download the [latest assembly jar](https://github.com/data-intuitive/LuciusAPI/releases/tag/v3.3.7) and store it under `jars` inside the LuciusAPI repo:

```sh
mkdir jars
cd jars
wget https://github.com/data-intuitive/LuciusAPI/releases/download/v3.3.7/LuciusAPI-assembly-3.3.7.jar
```

Now, in order to make life easy, some scripts are available. Two configuration files define the behavior of these scripts: `config/settings.sh` and `config/initialize-docker.conf`. Take a look at both to see what is going on. They are configured to work with the procedure described in this manual.

Start Lucius API by running the initialization script:

```bash
utils/initialize.sh \
  -v 3.3.7 \
  -s localhost:8090 \
  -c pointer_to_config_file.conf \
  -j jars
```

This assumes that config file is present. This is how such a config file could look like (for a locally available dataset):

```
{
  db.uri = "file:///.../results.parquet"
  geneAnnotations = "file:///.../lm_gene_info.txt"
  storageLevel = "MEMORY_ONLY_1"
  partitions = 4
  geneFeatures {
    pr_gene_id = "probesetid"
    pr_gene_symbol = "symbol"
    pr_is_lm = "dataType"
  }
}
```

Please note that the data needs to be present: `results.parquet` and the target data `lm_gene_info.txt`.

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

# New Spark Jobserver API

Since version 0.8.0, a new job API is available in Spark Jobserver. This allows (among other things) for properly parsing the parameters provided via the `POST` request in the `validate` step. This, in turn, allows us to have the output of the `validate` function to be the input of the respective `functions` with the implementation details. As a matter of fact, it renders those functions unneccessary.

As a bonus, default values for optional parameters are filled in already in the `validate` step/function, and not later. This makes everything much more clear from the outset.



> Please drop a note if you're interested in using/extending/... this project!
