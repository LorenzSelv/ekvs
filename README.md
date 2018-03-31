EKVS - Erlang Key Value Store
=============================

A highly-available, eventually-consistent, distributed, fault-tolerant key value store written in Erlang.

Keys are distributed uniformly among the nodes in the cluster using consistent hashing. 
The uniformity of the distribution can be tuned modifying the number of virtual nodes.

Replication of the keys on K nodes allows the system to tolerate the loss of nodes and network partitions. As the system favours availability over consistency kvs operations are allowed
in case of network partitions. Conflicts are resolved using causal payloads and timestamps when
the network partition is healed.


Prerequisites
-------------

* Erlang/OTP 20.2 (download tar from https://github.com/erlang/otp/releases)
* rebar3 (https://www.rebar3.org/)


Build
-----
    $ make build-[local|docker]


Eunit Testing
------
    $ make eunit


Docker Testing
------
    $ cd ekvs
    $ docker network create --subnet=10.0.0.0/24 ekvsnet
    $ make build-docker 
    $ pip install test/requirements.txt
    $ python test/test_HW4.py

