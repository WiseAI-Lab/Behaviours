import time
from collections import namedtuple
from typing import Union, Optional

from wise_agent.acl import AID, ACLMessage
from wise_agent.base_types import AgentState
from wise_agent.behaviours import InternalBehaviour
from wise_agent.config import ConfigHandler

AgentInfo = namedtuple(
    'AgentInfo', ['server_host', 'topic', 'status', 'last_time'])

BasicInfo = namedtuple(
    'BasicInfo', ['server_host', 'topic'])


# -------------------Table----------------
class TransportTable:
    """
        TransportTable to record the information about the connector.
        0 name: (server, topic)

        Example:
            Loop Table
            1.For
            table = TransportTable()
            for name, info in table:
                ...
            2.Iter
            table = iter(TransportTable)
            next(table)
    """

    def __init__(self):
        self._table = {}

    # ---------------------Basic Func--------------------

    def whole(self) -> dict:
        """
            Return the whole table for a dict.
        """
        return self._table

    def filter_as_sub(self):
        """

        Returns:

        """
        receivers = {}
        for name, info in self._table.items():
            server_host = info.server_host
            topic = info.topic
            if server_host in receivers.keys():
                if topic not in receivers[server_host]:
                    receivers[server_host].append(topic)
            else:
                receivers[server_host] = [topic]
        return receivers

    def add(self, name, info):
        """
            Add a user to table.
        """
        if info in list(self._table.values()):
            raise ValueError("current info:{} have existed in table".format(info))
        self._table[name] = info

    def get(self, name):
        """
            Get the information by name.
        """
        if name in self._table.keys():
            return self._table[name]
        else:
            raise KeyError("Input user's name not exist.")

    def update(self, name: str, info):
        """
            Update the user's info.
        """
        self._table[name] = info

    def in_table(self, name) -> bool:
        """
            Check a name whether in table.
        """
        if name in self._table.keys():
            return True
        else:
            return False

    # -----------------------Magic Func--------------------
    def __iter__(self):
        return iter(self._table.items())

    def __next__(self):
        return next(self)


class AgentTable(TransportTable):
    """
        AgentTable: Literal
        --------------------------
        0 name: (server, topic, status, time)
        1 name: (server, topic, status, time)
        2 name: (server, topic, status, time)
        3 name: (server, topic, status, time)
        ...
        E.g: "local@localhost:0000@topic" : ("localhost:0000", "topic", ALIVE, 15611165165)
        ...
        --------------------------
        Example:
            Loop Table
            1.For
            table = AgentTable()
            for name, info in table:
                ...
            2.Iter
            table = iter(AgentTable)
            next(table)

    """

    def __init__(self):
        super(AgentTable, self).__init__()
        self.main_name: Optional[str] = None  # ensure a main system.
        self._init()

    # ---------------------Basic Func--------------------
    def _init(self):
        config_content = ConfigHandler().read()
        # info
        mq_config = config_content.mq_config
        system_name = mq_config.get('system_name')
        system_address = mq_config.get('system_address')
        system_port = mq_config.get('system_port')
        system_topic = mq_config.get('system_topic')
        name = f"{system_name}@{system_address}:{system_port}@{system_topic}"
        # congregate
        info = AgentInfo(server_host=f"{system_address}:{system_port}", topic=system_topic,
                         status=AgentState.ALIVE, last_time=int(time.time()))
        self.add(name, info)
        self.main_name = name

    def add(self, name: Union[AID, str], info: AgentInfo):
        """
            Add a user to table.
        """
        if isinstance(name, AID):
            name = str(name)
        super(AgentTable, self).add(name, info)

    def get(self, name: Union[AID, str]) -> AgentInfo:
        """
            Get the information by name.
        """
        if isinstance(name, AID):
            name = str(name)
        return super(AgentTable, self).get(name)

    def in_table(self, name: Union[AID, str]) -> bool:
        """
            Check a name whether in table.
        """
        if isinstance(name, AID):
            name = str(name)
        return super(AgentTable, self).in_table(name)


# -------------------Transport Behaviour------------
class TransportBehaviour(InternalBehaviour):
    """
        A transport behaviour to manage message to
    """
    _table = None

    def __init__(self, agent):
        super(TransportBehaviour, self).__init__(agent)
        self.is_daemon = True
        self._table: Optional[AgentTable] = None

    def push(self, message):
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

    async def run(self, *args, **kwargs):
        """
            start a loop to receive the data
            Example:
               while True:
                    self.step()
        """

    def on_start(self, *args, **kwargs):
        """
            Start a transport behaviour
        """
        if self._table is None:
            raise ValueError("You should define a table in your transport behaviour.")
