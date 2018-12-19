from indy_common.state import domain

from indy_common.constants import ATTRIB
from plenum.common.constants import DOMAIN_LEDGER_ID, RAW, ENC, HASH, TARGET_NYM
from plenum.common.exceptions import InvalidClientRequest, UnauthorizedClientRequest

from plenum.common.request import Request
from plenum.common.txn_util import get_type
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler


class AttributeHandler(WriteRequestHandler):

    def __init__(self, config, database_manager: DatabaseManager):
        super().__init__(config, database_manager, ATTRIB, DOMAIN_LEDGER_ID)

    def static_validation(self, request: Request):
        identifier, req_id, operation = request.identifier, request.reqId, request.operation

        if not self._validate_attrib_keys(operation):
            raise InvalidClientRequest(identifier, req_id,
                                       '{} should have one and only one of '
                                       '{}, {}, {}'
                                       .format(ATTRIB, RAW, ENC, HASH))

    def dynamic_validation(self, request: Request):
        identifier, req_id, operation = request.identifier, request.reqId, request.operation

        if not (not operation.get(TARGET_NYM) or
                self.hasNym(operation[TARGET_NYM], isCommitted=False)):
            raise InvalidClientRequest(identifier, req_id,
                                       '{} should be added before adding '
                                       'attribute for it'.
                                       format(TARGET_NYM))

        if operation.get(TARGET_NYM) and operation[TARGET_NYM] != identifier and \
                not self.idrCache.getOwnerFor(operation[TARGET_NYM],
                                              isCommitted=False) == identifier:
            raise UnauthorizedClientRequest(
                identifier,
                req_id,
                "Only identity owner/guardian can add attribute "
                "for that identity")

    def gen_txn_path(self, txn):
        path = domain.prepare_attr_for_state(txn, path_only=True)
        return path.decode()

    def _updateStateWithSingleTxn(self, txn, isCommitted=False) -> None:
        """
        The state trie stores the hash of the whole attribute data at:
            the did+attribute name if the data is plaintext (RAW)
            the did+hash(attribute) if the data is encrypted (ENC)
        If the attribute is HASH, then nothing is stored in attribute store,
        the trie stores a blank value for the key did+hash
        """
        assert get_type(txn) == ATTRIB
        attr_type, path, value, hashed_value, value_bytes = domain.prepare_attr_for_state(txn)
        self.state.set(path, value_bytes)
        if attr_type != HASH:
            self.attributeStore.set(hashed_value, value)

    def hasNym(self, nym, isCommitted: bool = True):
        return self.idrCache.hasNym(nym, isCommitted=isCommitted)

    @staticmethod
    def _validate_attrib_keys(operation):
        dataKeys = {RAW, ENC, HASH}.intersection(set(operation.keys()))
        return len(dataKeys) == 1

    @property
    def idrCache(self):
        return self.database_manager.get_store('idr')

    @property
    def attributeStore(self):
        return self.database_manager.get_store('attrib')