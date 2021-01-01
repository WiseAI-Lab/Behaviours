import time
from collections import namedtuple
from typing import Union, Optional

from wgent.acl import AID, ACLMessage
from wgent.base_types import AgentState
from wgent.behaviours import InternalBehaviour
from wgent.config import ConfigHandler

AgentInfo = namedtuple(
    'AgentInfo', ['server_host', 'topic', 'status', 'last_time'])


class AgentTable:
    """
        AgentTable: Literal
        --------------------------
        0 name: (server, topic, status, time)
        1 name: (server, topic, status, time)
        2 name: (server, topic, status, time)
        3 name: (server, topic, status, time)
        E.g
        4 "local@localhost:0000@topic" : ("localhost:0000", "topic", ALIVE, 15611165165)
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
        self._table = {}
        self.main_name: Optional[str] = None  # ensure a main system.
        self._init()

    # ---------------------Basic Func--------------------
    def _init(self):
        config_content = ConfigHandler().read()
        # info
        system_name = config_content.system_name
        system_address = config_content.system_address
        system_port = config_content.system_port
        system_topic = config_content.system_topic
        name = f"{system_name}@{system_address}:{system_port}@{system_topic}"
        # congregate
        info = AgentInfo(server_host=f"{system_address}:{system_port}", topic=system_topic,
                         status=AgentState.ALIVE, last_time=int(time.time()))
        self.add(name, info)
        self.main_name = name

    def whole(self) -> dict:
        """
            Return the whole table for a dict.
        """
        return self._table

    def return_as_sub(self):
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

    def add(self, name: Union[AID, str], info: AgentInfo):
        """
            Add a user to table.
        """
        if isinstance(name, AID):
            name = str(name)
        if info in list(self._table.values()):
            raise ValueError("current info:{} have existed in table".format(info))
        self._table[name] = info

    def get(self, name: Union[AID, str]) -> AgentInfo:
        """
            Get the information by name.
        """
        if isinstance(name, AID):
            name = str(name)
        if name in self._table.keys():
            return self._table[name]
        else:
            raise KeyError("Input user's name not exist.")

    def update(self, name: str, info: AgentInfo):
        """
            Update the user's info.
        """
        self._table[name] = info

    def in_table(self, name: Union[AID, str]) -> bool:
        """
            Check a name whether in table.
        """
        if isinstance(name, AID):
            name = str(name)
        if name in self._table.keys():
            return True
        else:
            return False

    # -----------------------Magic Func--------------------
    def __iter__(self):
        return iter(self._table.items())

    def __next__(self):
        return next(self)


# -------------------Transport------------
class TransportBehaviour(InternalBehaviour):
    """
        A transport behaviour to manage message to
    """

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

    def on_start(self):
        """
            Start a transport behaviour
        """
        self._table = AgentTable()
