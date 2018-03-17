import subprocess
import os
from time import sleep
import requests
import random

random.seed(100)

import matplotlib.pyplot as plt
import numpy as np

HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKGREEN = '\033[92m'
FAIL = '\033[91m'
ENDC = '\033[0m'

VERBOSE = True

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
            'ip': ip,
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
        # print(node)
        try: ## Node might be disconnected
            res = requests.get(node['url'] + 'kvs/debug')
            data = res.json()
            snapshot.append('===============')
            snapshot.append('NODE %s' % node['ipport'])
            snapshot.append(data['view'])
            snapshot.append(data['kvs'])
            snapshot.append('===============')
        except requests.exceptions.ConnectionError:
            print('[DEBUG] node %d is DISCONNECTED' % i)

    # if VERBOSE: 
        # print('\n'.join(snapshot))
    return snapshot

def cp_to_dot_vc(cp):
    nodes = cp.split(',')
    clocks = [node.split(':')[1] for node in nodes]
    return ','.join(clocks)

def get_key(node, key, cp=''):
    if VERBOSE:
        print('GET %s cp=%s' % (key, cp))
    res = requests.get(node['url'] + 'kvs?key=%s&causal_payload=%s' % (key, cp))
    # print(res)
    data = res.json()
    val, cp = data['value'], data['causal_payload']
    if VERBOSE:
        print(OKGREEN + 'Result: %s cp=%s' % (val, cp_to_dot_vc(cp)) + ENDC)
    return data['value'], data['causal_payload']


def put_key(node, key, value, cp):
    if VERBOSE:
        print('PUT node=%s key=%s value=%s cp=%s' % (node['ipport'], key, value, cp))
    res = requests.put(node['url'] + 'kvs', data={'key': key, 'value': value, 'causal_payload': cp})
    # print(res)
    data = res.json()
    if VERBOSE:
        print('Result: ', data)
    return data['causal_payload']


def del_key(node, key):
    if VERBOSE:
        print('DEL %s' % key)
    res = requests.delete(node['url'] + 'kvs?key=%s' % key)
    data = res.json()
    if VERBOSE:
        print(data)


def view_update(change_type, node_to_remove_idx=None):
    
    node_to_ask = rnode()

    node = None
    idx  = -1
    if change_type == 'add':
        node = run_new_node()
    else:
        # idx = node_to_remove_idx
        # if idx is None:
            # idx = rnodeidx()
        ## Make sure the node to be deleted is not the only one in the partition
        for i, curnode in enumerate(NODES):
            if curnode == node_to_ask:
                continue
            pid = get_partition_id(curnode)
            members = get_partition_members(rnode(), pid)
            if len(members) > 1:
                idx, node = i, curnode
                break
        kill_node(idx)

    ipport = node['ipport']

    print('VIEW_UPDATE %s %s' % (change_type, ipport))
    # if VERBOSE:
        # print('VIEW_UPDATE %s %s' % (change_type, ipport))

    data = {'type': change_type, 'ip_port': ipport}
    url = node_to_ask['url'] + 'kvs/view_update' 
    print((url, data))

    res = requests.put(url, data) 
    
    assert res.status_code == 200

    if VERBOSE:
        print('Result: ', res.json())

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


def populate(num_key, cp=''):
    for i in range(num_key):
        key = 'key%d' % i
        val = 'val%d' % i
        cp = put_key(rnode(), key, val, cp)
    return cp


def RYW(num_key, cp=''):
    for i in range(num_key):
        key = 'key%d' % i
        val = 'val%d' % i
        get_val, cp = get_key(rnode(), key, cp)
        assert val == get_val
    return cp


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


def get_partition_id(node):
    res = requests.get(node['url'] + 'kvs/get_partition_id')
    # print(res)
    data = res.json()
    if VERBOSE:
        print(data)
    return data['partition_id']


def get_partition_ids(node):
    res = requests.get(node['url'] + 'kvs/get_all_partition_ids')
    # print(res)
    data = res.json()
    if VERBOSE:
        print(data)
    return data['partition_id_list']


def get_partition_members(node, partition_id):
    print('Asking partition members of %d to %s' % (partition_id, node['ipport']))
    res = requests.get(node['url'] + 'kvs/get_partition_members?partition_id=%d' % partition_id)
    # print(res)
    data = res.json()
    # if VERBOSE:
        # print(data)
    return data['partition_members']

def disconnect_node(node):
    if VERBOSE:
        print('DISCONNECT node ' + node['ipport'])
    docker_disconnect = "docker network disconnect lab4net " + node['docker_id']
    print(docker_disconnect)
    # out = subprocess.check_output(docker_disconnect, stderr=subprocess.STDOUT, shell=True)
    os.system(docker_disconnect)
    # print(out)

def connect_node(node):
    if VERBOSE:
        print('CONNECT node ' + node['ipport'])
    docker_connect = "docker network connect lab4net --ip=%s %s" % (node['ip'], node['docker_id'])
    print(docker_connect)
    # out = subprocess.check_output(docker_connect, stderr=subprocess.STDOUT, shell=True)
    os.system(docker_connect)
    # print(out)

def get_partitions():
    partitions = {}
    for node in NODES:
        pid = get_partition_id(node)
        partitions.setdefault(pid, [])
        partitions[pid].append(node['ipport'])
    print(partitions)
    return partitions


def test_7_TA():
    global TOKENS_PER_PARTITION
    global K

    num_nodes = 2
    num_keys  = 5 

    TOKENS_PER_PARTITION = 1
    K = 2

    init_cluster(gen_view(num_nodes))

    snapshot_to_file('0init')

    populate(num_keys)
    # assert ???num_keys == get_totnumkey()
    cp = RYW(num_keys)

    snapshot_to_file('1populated')

    snapshot_to_file('4before_numkey')

    real = get_totnumkey()
    
    def change(t, i):
        ## VIEW CHANGE
        print('='*30)
        print('CHANGE %d' % i)
        view_update(t)
        tot = get_totnumkey()
        get_partitions()
        print(tot)
        print('='*30)
        snapshot_to_file('%d%s' % (i, t))
        RYW(num_keys)

    change('add', 5)
    change('add', 6)
    change('add', 7)
    change('add', 8)
    change('remove', 9)
    change('remove', 10)
    change('remove', 11)
     
    kill_nodes()

def test_kvsop():
    global TOKENS_PER_PARTITION
    global K

    num_nodes = 3
    num_keys  = 10 

    TOKENS_PER_PARTITION = 1
    K = 2

    init_cluster(gen_view(num_nodes))

    snapshot_to_file('0init')

    populate(num_keys)
    # assert ???num_keys == get_totnumkey()
    RYW(num_keys)

    snapshot_to_file('1populated')
     
    kill_nodes()


def test_partitions_info():
    global TOKENS_PER_PARTITION
    global K

    num_nodes = 5
    num_keys  = 30

    TOKENS_PER_PARTITION = 1
    K = 2

    init_cluster(gen_view(num_nodes))
    snapshot_to_file('0init')
    
    ids = get_partition_ids(NODES[0])
    print('ids', ids)

    for id_ in ids:
        print('part %d' % id_, get_partition_members(rnode(), id_))
        print('-'*10)

    for i, node in enumerate(NODES):
        print('node%d' % i, get_partition_id(node))
        print('-'*10)

    kill_nodes()


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

    snapshot_to_file('0init')
    
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
    # test_partitions_info()
    # test_partitions()
    # test_kvsop()
    test_7_TA()


