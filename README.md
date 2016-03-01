# Introduction

This project provides the API for LuciusWeb to talk to. The API is a Spark-Jobserver project. It needs to be compiled and the resulting `jar` has to be uploaded to the Spark-Jobserver.

There's still a lot of work to be done on this (version numbers don't reflect everything).

# API

The documentation of the API itself is in [postman](https://www.getpostman.com/) and [can be found here](https://www.getpostman.com/collections/cf537f6cae9b82c35034).

# Important remarks

This version depends on LuciusBack (soon to be called LuciusCore) which contains the foundation for parsing and processing the types of files required here. Currently, the required `jar` is bundled in the LuciusAPI `jar` for deployment to the Spark-Jobserver. This could be done in other ways as well.

Please drop a note if you're interested in using/extending/... this project!