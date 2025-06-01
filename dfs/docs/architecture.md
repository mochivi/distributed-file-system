# Distributed file system architecture

This document lays out the architecture for the distributed file system.

## Coordinator Node

#### Server endpoints
- Upload
    - Validate if requested Path is available for upload
    - Idenfity candidate data nodes for each data chunk
    - Reply with where the client should upload each chunk (primary + replicas)

- Download
    - Validate required Path has some file

#### Client operations



#### Metadada


#### Coordinator distributed architecture

- Single leader architecture

## Data Node

## Client