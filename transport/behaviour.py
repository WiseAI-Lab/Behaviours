import importlib
import time
from collections import namedtuple
from typing import Union, Optional

from littletable import Table

from wise_agent.config import ConfigHandler
from wise_agent.acl import AID, ACLMessage
from wise_agent.behaviours import InternalBehaviour

MessageInfo = namedtuple(
    'MessageInfo', ['name', 'info', 'status', 'last_time', 'is_agent', 'is_pub'])


# - status: if using
# - is_agent: if Agent

# -------------------Table----------------
class TransportTable:
    """
        TransportTable to record the information about the connector.
    """

    def __init__(self, config):
        self.config = config
        self._table = self.init_table()

    def init_table(self):
        """
            Initial table by config.
        Returns:

        """
        table = Table("messages")
        return table

    # ---------------------Basic Func--------------------
    def whole(self) -> Table:
        """
            Return the whole table for a dict.
        """
        return self._table.info()

    def add(self, info: MessageInfo):
        """
            Add to table.
        """
        self._table.insert(info)

    def get(self, *args):
        """
            Get the information.
        """
        return self._table.where(*args).obs

    def update(self, name: str, info: MessageInfo):
        """
            Update the user's info.
        """
        obs_info = self._table.where(name=name).obs
        if 0 < len(obs_info) < 2:
            self._table.remove(obs_info[0])
            self._table.insert(info)
        else:
            raise ValueError("Much same info in table...")

    def in_table(self, *args) -> bool:
        """
            Check a name whether in table.
        """
        return True if self._table.where(*args).obs else False


class MessageQueueTransportTable(TransportTable):
    def __init__(self, config: ConfigHandler):
        super(MessageQueueTransportTable, self).__init__(config)

    def init_table(self):
        """
        Returns:

        """
        cur_table = super(MessageQueueTransportTable, self).init_table()
        mq_config = self.config.get("mq_config")

        def extract_from_config(mq_info):
            table = []
            for name, data in mq_info.items():
                is_pub = False
                status = True
                is_agent = data.pop("is_agent")
                last_time = time.time()
                message_info = MessageInfo(
                    name=name,
                    info=data,
                    status=status,
                    last_time=last_time,
                    is_agent=is_agent,
                    is_pub=is_pub)
                table.append(message_info)
            return table

        pub_info = mq_config.get("pub_info")
        pub_data = extract_from_config(pub_info)
        cur_table.insert_many(pub_data)

        sub_info = mq_config.get("sub_info")
        sub_info = extract_from_config(sub_info)
        cur_table.insert_many(sub_info)
        return cur_table

    def get_pubs(self):
        pubs = self._table.where(is_pub=True, status=True).obs
        return [pub.info for pub in pubs]

    def get_subs(self):
        subs = self._table.where(is_pub=False, status=True).obs
        return [sub.info for sub in subs]

    # -------------------Transport Behaviour------------


class TransportBehaviour(InternalBehaviour):
    """
        A transport behaviour to manage message to
    """
    _table: Optional[TransportTable] = None

    def __init__(self, agent):
        super(TransportBehaviour, self).__init__(agent)
        self.is_daemon = True

    def push(self, message, is_agent=True):
        """
            Send the message
        """

    def pull(self, *args, **kwargs):
        """
            Receive the message
        """

    def execute(self, message: ACLMessage):
        """
            Receive a task from BrainBehaviour(in pool) to send a message.
        """
        self.push(message)

    def step(self, *args, **kwargs):
        """
            Receive a message and compress the data(MemoryPiece) to Queue(Agent.memory_piece)
        """
        # try:
        #     msg = self.pull()
        #     # TODO 1. Check the msg's type
        #     # TODO 2. Compress it to a MemoryPiece and place it to Agent.memory_piece
        # except TimeoutError:
        #     pass

    def run(self, *args, **kwargs):
        """
            start a loop to receive the data
            Example:
        """

    def on_start(self, *args, **kwargs):
        """
            Start a transport behaviour
        """
        table_config = self.behaviour_config.get("table", False)
        if not table_config:
            table_config = "TransportTable"
        import_path = "transport.behaviour"
        basic_behaviour = importlib.import_module(import_path)
        table_class = getattr(basic_behaviour, table_config)
        self._table = table_class(self.agent.config_handler.get("configuration"))
        if self._table is None:
            raise ValueError("You should define a table in your transport behaviour.")
