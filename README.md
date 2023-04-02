# cloud-file-storage-system
cloud‐based file storage service with consistent hashing and RAFT consensus in Go lang
## Introduction
Basic function for client include: upload and download file from cloud. Support concurrent access by multiple users and avoid conflicts by “first-come first-served" principle. 
## Module
### File Storage system
Perform the above required function similar to Dropbox. 
### Consistent Hashing
Distributed storage of file data blocks on several servers by consistent hashing for fault tolerant.
### RAFT consensus
Implemented RAFT for meta data servers. Each time the client connect to the leader server and consistent state machine update is achived if majority followers agree.

