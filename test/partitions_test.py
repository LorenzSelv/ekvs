import subprocess
import os
from time import sleep
import requests
import random

random.seed(100)

import matplotlib.pyplot as plt
import numpy as np

VERBOSE = False

DOCKER_RUN      = 'docker run -p %d:8080 --net=lab4net --ip=%s -e K=%d -e              "ip_port"="%s:8080" -e TOKENSPERPARTITION="%d" lab4erlang'
DOCKER_RUN_INIT = 'docker run -p %d:8080 --net=lab4net --ip=%s -e K=%d -e VIEW="%s" -e "ip_port"="%s:8080" -e TOKENSPERPARTITION="%d" lab4erlang'

URL = 'http://127.0.0.1:%d/'

K = 3  ## ReplicasPerPartitions
TOKENS_PER_PARTITION = 5

NODES = [] 

NEXT_IDX = 0


def get_current_view():
    return ','.join(node['ipport'] for node in NODES)


def run_new_node(view=None):
    global NEXT_IDX
    i = NEXT_IDX 
    NEXT_IDX += 1

    ip = '10.0.0.%d' % (20+i)
    ipport = ip + ':8080'
    logfile = open('test/log/node%d'%i, 'w')
    localport = 8080 + i
    url = URL % localport
    
    docker_run = None
    ## view is used only during initialization
    if view:
        docker_run = DOCKER_RUN_INIT % (localport, ip, K, view, ip, TOKENS_PER_PARTITION) 
    else:
        docker_run = DOCKER_RUN      % (localport, ip, K,       ip, TOKENS_PER_PARTITION) 

    print(docker_run)

    subprocess.Popen(docker_run, shell=True, universal_newlines=True, stdout=logfile)

    sleep(2)
    docker_id = subprocess.check_output("docker ps -l -q", shell=True)
    docker_id = docker_id.decode('ascii').strip('\n')
    
    node = {'localport': localport,
            'url': url,
            'ipport': ipport,
            'logfile': logfile,
            'docker_id': docker_id}
    # print(node)
    NODES.append(node)

    return node


def kill_node(idx):
    print('Killing %d' % NODES[idx]['localport'])
    
    ## close the logfile
    NODES[idx]['logfile'].close()
    
    ## kill the node
    docker_kill = 'docker kill %s' % NODES[idx]['docker_id'] 
    out = subprocess.check_output(docker_kill, shell=True)
    
    node = NODES[idx]
    del NODES[idx]

    return node
    

def init_cluster(view):
    for _ in view.split(','):
        run_new_node(view)


def kill_nodes():
    N = len(NODES)
    [kill_node(0) for _ in range(N)]
    # docker_kill = 'docker kill $(docker ps -q)'
    # out = subprocess.check_output(docker_kill, shell=True)
    # [f.close() for f in LOGFILES]
    # print(out)
    print('Killed all NODES')


def inspect_nodes():
    snapshot = []
    for i, node in enumerate(NODES):
        res = requests.get(node['url'] + 'kvs/debug')
        data = res.json()
        snapshot.append('===============')
        snapshot.append('NODE %s' % node['ipport'])
        snapshot.append(data['view'])
        snapshot.append(data['kvs'])
        snapshot.append('===============')
    if VERBOSE: 
        print('\n'.join(snapshot))
    return snapshot


def get_key(node, key):
    res = requests.get(node['url'] + 'kvs?key=%s' % key)
    data = res.json()
    if VERBOSE:
        print('GET %s' % key)
        print(data)
    return data['value']


def put_key(node, key, value):
    res = requests.put(node['url'] + 'kvs', data={'key': key, 'value': value})
    data = res.json()
    if VERBOSE:
        print('PUT %s %s' % (key, value))
        print(data)


def del_key(node, key):
    res = requests.delete(node['url'] + 'kvs?key=%s' % key)
    data = res.json()
    if VERBOSE:
        print('DEL %s' % key)
        print(data)


def view_update(change_type, node_to_remove_idx=None):
    
    node_to_ask = rnode()

    node = None
    idx  = -1
    if change_type == 'add':
        node = run_new_node()
    else:
        idx = node_to_remove_idx
        if idx is None:
            idx = rnodeidx()
        node = NODES[idx]

    ipport = node['ipport']
    data = {'type': change_type, 'ip_port': ipport}
    url = node_to_ask['url'] + 'kvs/view_update' 
    print((url, data))

    res = requests.put(url, data) 
    
    assert res.status_code == 200

    if VERBOSE:
        print('VIEW_UPDATE %s %s' % (change_type, ipport))
        print(res.json())

    if change_type == 'remove':
        kill_node(idx) 

    return node


def get_numkey(node, i):
    res = requests.get(node['url'] + 'kvs/get_number_of_keys')
    data = res.json()
    if VERBOSE:
        print('NUMKEYS NODE %d' % i)
        print(data)
    return data['count']


def get_totnumkey():
    return sum(get_numkey(node, i) for i, node in enumerate(NODES))


def get_keydistribution():
    return [get_numkey(node, i) for i, node in enumerate(NODES)]


def rnode():
    return random.choice(NODES)


def rnodeidx():
    return random.randrange(len(NODES))


def populate(num_key):
    for i in range(num_key):
        key = 'key%d' % i
        val = 'val%d' % i
        put_key(rnode(), key, val)


def RYW(num_key):
    for i in range(num_key):
        key = 'key%d' % i
        val = 'val%d' % i
        assert val == get_key(rnode(), key)


def delete_keyrange(start, end):
    for i in range(start, end):
        key = 'key%d' % i
        del_key(rnode()['url'], key)


def compare_snapshots(snap1, snap2):
    if not len(snap1) == len(snap2):
        print("Different lens %d %d" % (len(snap1), len(snap2)))
        return

    diff = 0
    for line1, line2 in zip(snap1, snap2):
        if not line1 == line2:
            diff += 1
            print("---------")
            print("Different lines:")
            print(line1)
            print(line2)
            print("---------")

    if diff > 0:
        print("Comparison FAILED #line=%d")
        return False
    return True


def snapshot_to_file(filename):
    with open('test/snapshots/%s.snap' % filename, "w") as f:
        f.write('\n'.join(inspect_nodes()))


def gen_view(num_nodes):
    return ','.join("10.0.0.%d:8080" % (20+i) for i in range(num_nodes))


def get_partition_ids(node):
    res = requests.get(node['url'] + 'kvs/get_all_partition_ids')
    data = res.json()
    if VERBOSE:
        print(data)
    return data['partition_id_list']


def test_partitions():
    global TOKENS_PER_PARTITION
    global K

    num_nodes = 5
    num_keys  = 30

    TOKENS_PER_PARTITION = 1
    K = 2

    init_cluster(gen_view(num_nodes))

    # populate(num_keys)
    # assert ???num_keys == get_totnumkey()
    # RYW(num_keys)

    snapshot_to_file('init')
    
    partition_ids1 = get_partition_ids(rnode())
    print(partition_ids1)

    view_update('add')
    snapshot_to_file('add1')

    partition_ids2= get_partition_ids(rnode())
    print(partition_ids2)

    ## No new node should be added
    assert partition_ids1 == partition_ids2

    view_update('add')
    snapshot_to_file('add2')

    partition_ids = get_partition_ids(rnode())
    print(partition_ids)

    view_update('remove', node_to_remove_idx=0)
    snapshot_to_file('remove3')

    partition_ids = get_partition_ids(rnode())
    print(partition_ids)

    view_update('remove', node_to_remove_idx=0)
    snapshot_to_file('remove4')

    partition_ids = get_partition_ids(rnode())
    print(partition_ids)

    view_update('remove')
    snapshot_to_file('remove5')

    partition_ids = get_partition_ids(rnode())
    print(partition_ids)

    view_update('remove')
    snapshot_to_file('remove6')

    partition_ids = get_partition_ids(rnode())
    print(partition_ids)

    kill_nodes()



if __name__ == '__main__':
    test_partitions()

