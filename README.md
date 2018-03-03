Lab4 KVS
=====

A REST-accessible distributed key-value store with dynamic key 
redistribution using Consistent Hashing with virtual nodes.


Prerequisites
-------------

* Erlang/OTP 20.2 (download tar from https://github.com/erlang/otp/releases)
* rebar3 (https://www.rebar3.org/)


Build
-----
    
    $ make build-[local|docker]


Docker Testing
------
    $ cd lab4kvs
    $ docker network create --subnet=10.0.0.0/24 lab4net
    $ make build-docker 
    $ python3 test/distribution_test.py
