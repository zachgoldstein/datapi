          ██████╗  █████╗ ████████╗ █████╗ ██████╗ ██╗
          ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔══██╗██║
          ██║  ██║███████║   ██║   ███████║██████╔╝██║
          ██║  ██║██╔══██║   ██║   ██╔══██║██╔═══╝ ██║
          ██████╔╝██║  ██║   ██║   ██║  ██║██║     ██║
          ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝     ╚═╝

# Datatoapi

## When would you use this?

- Your data is sitting in simple files (jsonfiles, csv, etc) on cloud storage
- You want to access a single data point or small subsets of your data
- There is too much data to read it all for access to a single object
- Access latency isn't critically important but must be reasonable
- You don't want to spend timing ingesting the data into a traditional database
- Your use case has you reading alot more than writing
- You don't want to spend time writing routing or serialisation code

## Installation

Just want a binary you can run?

- See XXX for releases, and download the one for your env

Running golang locally and want to compile the project?

- `go get XYZ`

This will download and install your binary.

## Usage

Pick your primary indexes and run!

```
datatoapi --index=id,createdAt,updatedAt --endpoint=s3XYZ --user=XYZ --pass=XYZ
```
Your data will have a root type of "Datatoapi", and the 


```
EXAMPLE OUPTUT OF RUNNING SERVER
```

Accessing your data on the command line with curl:
```

```

Or with a simple javascript graphql client:
```

```

and another client in python:
```

```

What to just see the server's schema?
See http://graphql.org/learn/introspection/

There will be one main "Type" with all the fields you expect your data to have



## Options

--host=example.com (default = localhost)
--port=9090 (default = 8080)

If you've already scanned your data and have an index file you want to use;
```
--index_file=.customIndexFile
```
By default, datatoapi will check this file with the name `.datatoapiIndex` in the home directory, `~`

If params are not used, we look for environment variables with the same name and the `DATATOAPI` prefix:
DATATOAPI_ENDPOINT
DATATOAPI_USER
DATATOAPI_PASS
DATATOAPI_HOST
DATATOAPI_PORT

## Supported data formats

- jsonfiles
- csv (TODO)
- tsv (TODO)
- json (TODO)
- xml (TODO)

## Supported storage backends

- Amazon S3
- Azure (TODO)
- Digital Ocean Spaces (TODO)
- Openstack Swift storage (TODO)
- Ceph (TODO)

## What datatoapi is *not* good for

- Write heavy situations where your data set is rapidly changing
- You need some sort of authorisation scheme associated with the data. You'll have to build this functionality separately.

## More usage examples

### Simple usages:

Javascript
Python
Golang
Curl
  (curl and jq to do a basic aggregation)

### Complex use cases

pagination
limiting
date range


## How does this work

datatoapi will first look through your data and build your indexes for data access. These indexes are stored in a file called `.datatoapiIndex` and at startup datatoapi searches the local directory for an existing index file. The underlying indexes are stored with bbolt (https://github.com/coreos/bbolt), a simple b-tree based key/value store that operates in memory. This indexing process also creates a schema of your data, which we provide to graphql to expose to the user.

A graphql server then starts up, and exposes a single endpoint for you to access your data.

You can choose to use one of the many wonderful graphql libraries out there to now access the data, a simpler client using juse HTTP POST, or even simpler again via the command line and curl.

As your data changes, datatoapi will poll the cloud storage to update it's indexes. You've got a configurable time period that triggers this, but be aware that setting a very small time will eat up a good bit of performance. The primary use case here is not for datasets that are rapidly changing.