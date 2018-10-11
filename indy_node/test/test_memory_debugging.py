import json
import types
from collections import OrderedDict
from operator import itemgetter
from typing import Any

import sys

from plenum.common.perf_util import get_memory_usage

from plenum.common.constants import STEWARD_STRING
from plenum.common.util import randomString
from plenum.common.messages.node_messages import Commit

from plenum.server.node import Node
from plenum.test.pool_transactions.helper import sdk_add_new_nym, prepare_nym_request, \
    sdk_sign_and_send_prepared_request
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


def sdk_add_new_nym_without_waiting(looper, sdk_pool_handle, creators_wallet,
                                    alias=None, role=None, seed=None,
                                    dest=None, verkey=None, skipverkey=False):
    seed = seed or randomString(32)
    alias = alias or randomString(5)
    wh, _ = creators_wallet

    # filling nym request and getting steward did
    # if role == None, we are adding client
    nym_request, new_did = looper.loop.run_until_complete(
        prepare_nym_request(creators_wallet, seed,
                            alias, role, dest, verkey, skipverkey))

    # sending request using 'sdk_' functions
    request_couple = sdk_sign_and_send_prepared_request(looper, creators_wallet,
                                                        sdk_pool_handle, nym_request)

    return wh, new_did


def get_max(obj, seen=None, now_depth=0, path=str()):
    """Recursively finds size of objects"""
    if now_depth > 5:
        return {}
    dictionary = {(path, type(obj)): sys.getsizeof(obj)}
    path += str(type(obj)) + ' ---> '
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return {}
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        vpath = path + 'value ---> '
        for d in [get_max(v, seen, now_depth + 1, vpath) for v in obj.values()]:
            updater(dictionary, d)
        kpath = path + 'key ---> '
        for d in [get_max(k, seen, now_depth + 1, kpath) for k in obj.keys()]:
            updater(dictionary, d)
    elif hasattr(obj, '__dict__'):
        dpath = path + '__dict__ ---> '
        d = get_max(obj.__dict__, seen, now_depth + 1, dpath)
        updater(dictionary, d)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        ipath = path + '__iter__ ---> '
        for d in [get_max(i, seen, now_depth + 1, ipath) for i in obj]:
            updater(dictionary, d)
    return dictionary


def updater(store_d, new_d):
    for k in new_d.keys():
        if k in store_d:
            store_d[k] += int(new_d[k])
        else:
            store_d[k] = new_d[k]


def test_memory_debugging(looper,
                          nodeSet,
                          sdk_wallet_trust_anchor,
                          sdk_pool_handle):
    # Settings
    unordered_requests_count = 80
    file_name = 'memory_data.txt'

    # Sets for emulating commits problems
    set1 = list(nodeSet)
    set1.remove(nodeSet[0])
    set2 = list(nodeSet)
    set2.remove(nodeSet[1])
    set3 = list(nodeSet)
    set3.remove(nodeSet[2])
    primary = nodeSet[0]
    # memory_data = OrderedDict()
    pypmpler = []

    pypmpler.append(get_memory_usage(primary, True, get_only_non_empty=True))

    while primary.master_replica.lastPrePrepareSeqNo < unordered_requests_count:
        sdk_add_new_nym(looper, sdk_pool_handle, sdk_wallet_trust_anchor)

    pypmpler.append(get_memory_usage(primary, True, get_only_non_empty=True))



    file = open(file_name, 'w')
    for p in pypmpler:
        file.write('\n\n')
        file.write(str(p[0]) + '  ;   ')
        p[1].sort(key=lambda elem: elem[-1], reverse=True)
        file.write(str(p[1]))
    file.close()


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
