# jb-test-assigment

This repository contains a `multifs` VFS implementation that keeps all information about files 
and their contents in a single file on local filesystem. To achieve thread-safety it uses 
handmade Read-Write mutex based on [this](https://github.com/Kotlin/kotlinx.coroutines/issues/94) 
discussion. Implementation supposes that no hardware errors can happen (and no one can unplug the 
computer while the storage file is changing). If one is interested in implementing quite durable 
storage, please check out my [repository](https://github.com/vsalavatov/bsse-storage-systems/) of 
the Storage Systems coursework. 

To maintain the structure of the filesystem, `SingleFileFS` uses two entities: node metadata records 
and references to these nodes. Because metadata and its size change during write operations, 
it is important to be able to quickly update it on disk. To achieve this, `SingleFileFS`
appends new metadata to the end of file and simply updates a fix-sized reference to that metadata 
(there is always exactly one reference for each metadata record), and periodically performs 
defragmentation.


