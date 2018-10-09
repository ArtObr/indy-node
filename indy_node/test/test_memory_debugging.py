import json
import types
from collections import OrderedDict
from operator import itemgetter
from typing import Any

import sys

from plenum.common.constants import STEWARD_STRING
from plenum.common.util import randomString
from plenum.common.messages.node_messages import Commit

from plenum.server.node import Node, get_size, get_max
from plenum.test.pool_transactions.helper import sdk_add_new_nym, prepare_nym_request
from plenum.test.helper import sdk_json_to_request_object


def dont_send_commit(self, msg: Any, *rids, signer=None, message_splitter=None):
    if isinstance(msg, (Commit)):
        if rids:
            rids = [rid for rid in rids if rid not in self.nodestack.getRemote(self.ignore_node_name).uid]
        else:
            rids = [self.nodestack.getRemote(name).uid for name
                    in self.nodestack.remotes.keys() if name not in self.ignore_node_name]
    self.old_send(msg, *rids, signer=signer, message_splitter=message_splitter)


def dont_send_commit_to(nodes, ignore_node_name):
    for node in nodes:
        if not hasattr(node, 'ignore_node_name'):
            node.ignore_node_name = []
        node.ignore_node_name.append(ignore_node_name)
        node.old_send = types.MethodType(Node.send, node)
        node.send = types.MethodType(dont_send_commit, node)


def reset_sending(nodes):
    for node in nodes:
        node.send = types.MethodType(Node.send, node)


def test_memory_debugging(looper,
                          nodeSet,
                          sdk_wallet_trust_anchor,
                          sdk_pool_handle):
    # Settings
    unordered_requests_count = 10
    file_name = 'memory_data.txt'

    # Sets for emulating commits problems
    set1 = list(nodeSet)
    set1.remove(nodeSet[0])
    set2 = list(nodeSet)
    set2.remove(nodeSet[1])
    set3 = list(nodeSet)
    set3.remove(nodeSet[2])
    primary = nodeSet[0]

    memory_data = OrderedDict()
    memory_data['After starting'] = get_max(primary)

    # Emulate commit sending problems
    dont_send_commit_to(set1, nodeSet[0].name)
    dont_send_commit_to(set2, nodeSet[1].name)
    dont_send_commit_to(set3, nodeSet[2].name)

    # Sending requests until nodes generate `unordered_requests_count` 3pc batches
    while primary.master_replica.lastPrePrepareSeqNo < unordered_requests_count:
        sdk_add_new_nym(looper, sdk_pool_handle, sdk_wallet_trust_anchor)

    memory_data['After {} unordered'.format(unordered_requests_count)] = get_max(primary)

    # Remove commit problems
    reset_sending(set1)
    reset_sending(set2)
    reset_sending(set3)

    # primary ask for commits
    for i in range(primary.master_replica.last_ordered_3pc[1], primary.master_replica.lastPrePrepareSeqNo):
        primary.replicas._replicas.values()[0]._request_commit((0, i))
    for i in range(primary.replicas._replicas.values()[1].last_ordered_3pc[1],
                   primary.replicas._replicas.values()[1].lastPrePrepareSeqNo):
        primary.replicas._replicas.values()[1]._request_commit((0, i))
    looper.runFor(5)

    memory_data['After {} ordered'.format(unordered_requests_count)] = get_max(primary)

    # primary clear queues
    primary.replicas._replicas.values()[0]._gc(primary.replicas._replicas.values()[0].last_ordered_3pc)
    primary.replicas._replicas.values()[1]._gc(primary.replicas._replicas.values()[1].last_ordered_3pc)

    memory_data['After _gc called'] = get_max(primary)

    # Emulate problems again
    dont_send_commit_to(set1, nodeSet[0].name)
    dont_send_commit_to(set2, nodeSet[1].name)
    dont_send_commit_to(set3, nodeSet[2].name)

    while primary.master_replica.lastPrePrepareSeqNo < unordered_requests_count * 2:
        sdk_add_new_nym(looper, sdk_pool_handle, sdk_wallet_trust_anchor)

    memory_data['After {} unordered again'.format(unordered_requests_count)] = get_max(primary)

    for i in range(primary.master_replica.last_ordered_3pc[1], primary.master_replica.lastPrePrepareSeqNo):
        primary.replicas._replicas.values()[0]._request_commit((0, i))
    for i in range(primary.replicas._replicas.values()[1].last_ordered_3pc[1],
                   primary.replicas._replicas.values()[1].lastPrePrepareSeqNo):
        primary.replicas._replicas.values()[1]._request_commit((0, i))
    looper.runFor(5)

    memory_data['After {} ordered again'.format(unordered_requests_count)] = get_max(primary)

    primary.replicas._replicas.values()[0]._gc(primary.replicas._replicas.values()[0].last_ordered_3pc)
    primary.replicas._replicas.values()[1]._gc(primary.replicas._replicas.values()[1].last_ordered_3pc)

    memory_data['After _gc called again'] = get_max(primary)

    assert len(memory_data) == 7

    file = open(file_name, 'w')
    for k, v in memory_data.items():
        file.write(k)
        file.write('\n')
        file.write(str(sum(v.values()) / 1024 / 1024) + ' mbs')
        file.write('\n')
        # Sort by value
        for k, v in sorted(v.items(), key=itemgetter(1), reverse=True):
            file.write(str(v / 1024 / 1024) + ' mbs       ')
            file.write(str(k))
            file.write('\n')
        file.write('\n\n\n')


def test_requests_collection_debugging(looper,
                                       nodeSet,
                                       sdk_wallet_trustee):
    primary = nodeSet[0]

    seed = randomString(32)
    alias = randomString(5)
    wh, _ = sdk_wallet_trustee
    nym_request, new_did = looper.loop.run_until_complete(
        prepare_nym_request(sdk_wallet_trustee, seed,
                            alias, STEWARD_STRING))

    nym_request = json.loads(nym_request)
    a = sys.getsizeof(primary.requests)

    mas = []
    for _ in range(50000):
        req = sdk_json_to_request_object(nym_request)
        req.reqId = randomString(32)
        mas.append(req)
        primary.requests.add_propagate(req, 'asd')
        primary.requests.mark_as_forwarded(req, 2)
        primary.requests.set_finalised(req)

    b = sys.getsizeof(primary.requests)
    lb = len(primary.requests)

    for req in mas:
        primary.requests.mark_as_executed(req)
        primary.requests.free(req.key)
        primary.requests.free(req.key)

    c = sys.getsizeof(primary.requests)
    lc = len(primary.requests)

    for _ in range(100000):
        req = sdk_json_to_request_object(nym_request)
        req.reqId = randomString(32)
        mas.append(req)
        primary.requests.add_propagate(req, 'asd')
        primary.requests.mark_as_forwarded(req, 2)
        primary.requests.set_finalised(req)

    d = sys.getsizeof(primary.requests)
    ld = len(primary.requests)

    print(a)
    print(b, lb)
    print(c, lc)
    print(d, ld)
