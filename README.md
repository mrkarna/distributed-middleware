# Consistent Distributed Key-Value Store

This repository builds a distributed data storage system with consistency requirements (total order multicast). It implements a simple distributed data storage system using a so-called NoSQL key-value system, Cassandra.
It has mainly two parts - a single-node data store wrapper based on Cassandra and a non-blocking client. The second part is a distributed wrapper (or middleware) to ensure total ordered writes on servers.
