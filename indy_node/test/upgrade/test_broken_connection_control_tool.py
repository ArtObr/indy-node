import asyncio
from functools import partial

import dateutil
import dateutil.tz
import pytest

from datetime import datetime, timedelta

from indy_common.constants import START

from indy_node.test.upgrade.conftest import patch_packet_mgr_output, EXT_PKT_NAME, EXT_PKT_VERSION

from indy_common.config_helper import NodeConfigHelper

from indy_node.server.upgrader import Upgrader, UpgradeMessage
from indy_node.test.helper import TEST_INITIAL_NODE_VERSION
from indy_node.test.upgrade.helper import sdk_ensure_upgrade_sent, bumpedVersion
from plenum.test.helper import randomText

from plenum.test.test_node import ensureElectionsDone, checkNodesConnected
from stp_core.common.log import getlogger

delta = 2
logger = getlogger()


@pytest.fixture(scope="module")
def tconf(tconf):
    old_delta = tconf.MinSepBetweenNodeUpgrades
    tconf.MinSepBetweenNodeUpgrades = delta
    yield tconf
    tconf.MinSepBetweenNodeUpgrades = old_delta


@pytest.fixture(scope='function')
def validUpgrade(nodeIds, tconf, monkeypatch):
    schedule = {}
    unow = datetime.utcnow().replace(tzinfo=dateutil.tz.tzutc())
    startAt = unow + timedelta(seconds=100)
    acceptableDiff = tconf.MinSepBetweenNodeUpgrades + 1
    for i in nodeIds:
        schedule[i] = datetime.isoformat(startAt)
        startAt = startAt + timedelta(seconds=acceptableDiff + 3)

    patch_packet_mgr_output(monkeypatch, EXT_PKT_NAME, EXT_PKT_VERSION)

    return dict(name='upgrade-{}'.format(randomText(3)), version=bumpedVersion(EXT_PKT_VERSION),
                action=START, schedule=schedule, timeout=1, package=EXT_PKT_NAME,
                sha256='db34a72a90d026dae49c3b3f0436c8d3963476c77468ad955845a1ccf7b03f55')


@pytest.fixture(scope='function')
def skip_functions():
    # Do this to prevent exceptions because of node_control_tool absence
    old_get_version = Upgrader.getVersion
    old_update = Upgrader._update_action_log_for_started_action
    Upgrader.getVersion = lambda self, pkg: '1.6.0'
    Upgrader._update_action_log_for_started_action = lambda self: 1
    yield
    Upgrader.getVersion = old_get_version
    Upgrader._update_action_log_for_started_action = old_update


async def _sendUpgradeRequest(self, when, version, upgrade_id, failTimeout, pkg_name):
    retryLimit = self.retry_limit
    if retryLimit:
        self.version = version
    while retryLimit:
        try:
            msg = UpgradeMessage(version=version, pkg_name=pkg_name).toJson()
            logger.info("Sending message to control tool: {}".format(msg))
            break
        except Exception as ex:
            logger.warning("Failed to communicate to control tool: {}".format(ex))
            asyncio.sleep(self.retry_timeout)
            retryLimit -= 1
    if not retryLimit:
        self._unscheduleAction()
    else:
        logger.info("Waiting {} minutes for upgrade to be performed".format(failTimeout))
        timesUp = partial(self._declareTimeoutExceeded, when, version, upgrade_id)
        self._schedule(timesUp, self.get_timeout(failTimeout))


@pytest.fixture(scope='function')
def replace_send_upgrade():
    old_send = Upgrader._sendUpgradeRequest
    Upgrader._sendUpgradeRequest = _sendUpgradeRequest
    yield
    Upgrader._sendUpgradeRequest = old_send


def test_node_doesnt_retry_upgrade(
        looper, nodeSet, validUpgrade, nodeIds,
        sdk_pool_handle, sdk_wallet_trustee, tconf,
        replace_send_upgrade):
    bad_node = nodeSet[1]

    # Making upgrade sooner
    schedule = {}
    unow = datetime.utcnow().replace(tzinfo=dateutil.tz.tzutc())
    startAt = unow + timedelta(seconds=delta)
    for i in nodeIds:
        schedule[i] = datetime.isoformat(startAt)
        startAt = startAt + timedelta(seconds=delta)
    validUpgrade['schedule'] = schedule

    # Emulating connection problems
    bad_node.upgrader.retry_limit = 0

    # Send upgrade
    sdk_ensure_upgrade_sent(looper, sdk_pool_handle,
                            sdk_wallet_trustee, validUpgrade)
    looper.runFor(len(nodeIds) * delta)

    # Every node except bad_node upgraded
    for node in nodeSet:
        if node is not bad_node:
            assert node.upgrader.version == validUpgrade['version']
    assert bad_node.upgrader.version == TEST_INITIAL_NODE_VERSION

    # Every node, including bad_node, tried to upgrade only once
    for node in nodeSet:
        assert node.upgrader.spylog.count(Upgrader.processLedger.__name__) == 1


def test_node_upgrades_after_restart(looper, nodeSet, testNodeClass, tconf,
                                     tdir, allPluginsPath, validUpgrade, skip_functions,
                                     replace_send_upgrade):
    bad_node = nodeSet[1]

    # Restart bad_node
    name = bad_node.name
    nodeSet.remove(bad_node)
    bad_node.cleanupOnStopping = False
    looper.removeProdable(bad_node)
    bad_node.stop()
    del bad_node
    config_helper = NodeConfigHelper(name, tconf, chroot=tdir)
    node = testNodeClass(name, config_helper=config_helper,
                         config=tconf, pluginPaths=allPluginsPath)
    looper.add(node)
    nodeSet.append(node)
    looper.run(checkNodesConnected(nodeSet))
    ensureElectionsDone(looper=looper, nodes=nodeSet, retryWait=1)

    bad_node = nodeSet[-1]

    # Connection problems resolved
    assert bad_node.upgrader.retry_limit == 3

    # Node upgraded after restart
    for node in nodeSet:
        assert node.upgrader.version == validUpgrade['version']
