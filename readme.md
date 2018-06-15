
          ██████╗  █████╗ ████████╗ █████╗ ██████╗ ██╗
          ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔══██╗██║
          ██║  ██║███████║   ██║   ███████║██████╔╝██║
          ██║  ██║██╔══██║   ██║   ██╔══██║██╔═══╝ ██║
          ██████╔╝██║  ██║   ██║   ██║  ██║██║     ██║
          ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝     ╚═╝

<img align="right" width="200" src="https://i.imgur.com/JTHWibj.png">

# Datatoapi

## When would you use this?

- Your data is sitting in simple files (jsonfiles, csv, etc) on cloud storage
- You want to access a single data point or small subsets of your data
- There is too much data to read it all for access to a single object
- Access latency isn't critically important but must be reasonable
- You don't want to spend timing ingesting the data into a traditional database
- Your use case has you reading alot more than writing
- You don't want to spend time writing routing or serialisation code

## An example of how this would be done previously:

If you have flat files sitting in s3, you can retrieve the whole record and search without alot of effort, but it takes quite a bit of time.
```
aws s3 cp s3://datatoapi/data.jsonfiles - | jq 'select(.username == "wyman.maye")' -c  
```
Takes about 3.53 secs. (requires downloading the entire file).

How this would work if you're running datapi?
```
curl "http://127.0.0.1:8123/username/wyman.maye"
```
Took 0.757 secs.

Why is this faster? Datapi has built an index to find the record more quickly, and only downloads a small chunk of the file.

In this example, the file we're interested in is very small (890K), but when you're looking at larger files, the difference in performance will be much more significant.

## Installation

(TODO)

## Usage

(TODO)
```
go run main.go -storage "https://s3.amazonaws.com/datatoapi"
```

## Options

(TODO)

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
