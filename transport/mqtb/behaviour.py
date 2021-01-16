import abc
import asyncio
from typing import Union, Dict, List, Optional

from wise_agent.acl import ACLMessage
from wise_agent.acl.messages import MessageType
from wise_agent.behaviours.transport import TransportBehaviour, MessageQueueTransportTable
from wise_agent.utility import start_task, logger


class MessageTransportBehaviour(TransportBehaviour):
    """
        After Message Transport start, the consumer will auto-receive the message from topics which are subscribed
        and the producer wait for agent send message.
        The AgentTable had been recording the state about current agent and other agent from the message.
    """

    def __init__(self, agent):
        super(MessageTransportBehaviour, self).__init__(agent)
        self.priority = 5  # Transport priority from external information
        # message queue init
        self._producers = {}  # Record a producer
        self.consumers = {}  # consumers from diff server.
        self._running_tasks = {}
        self._table: Optional[MessageQueueTransportTable] = None

    @staticmethod
    def _confirm_the_address(table, message: Union[ACLMessage, None]) -> List:
        """
            Confirm the address that agent easily dispatch.
        Args:
            message: Union[ACLMessage, None]

        Returns: Dict[str, List[str]]

        """
        receivers = list()

        if message is None:
            receivers = table.get_pubs()
        else:
            if message.receivers is not None:
                for r in message.receivers:  # It's the name as key.
                    if table.in_table({"name": r}):
                        memory_info = table.get({"name": r})
                        receivers.append(memory_info)
            else:
                receivers = table.get_pubs()
        return receivers

    # ---------------Main Func-----------------
    def push(self, message, is_agent=False):
        # Add to the pool task.
        self._running_tasks = start_task(self._running_tasks,
                                         self._tasks_pool,
                                         self._produce,
                                         message)

    def _dispatch_consume_message(self, msg: Union[str, bytes], **kwargs):
        """
        Analysis the msg from consumer:
            if `msg` is json and encode from ACLMessage that parse to a ACLMessage object.
            elif `msg` is unknown that will not be dropped out, but to define as unknown type.
        Args:
            msg: Union[str, bytes]
            **kwargs:
        Returns:

        """
        # Decode the message
        priority = kwargs.get('priority', 5)  # Set a default value
        if isinstance(msg, bytes):
            msg = msg.decode("utf8")
        acl = ACLMessage()
        try:
            acl.decode(msg)
        except Exception as e:
            logger.info(f"{e} ===> Message:{msg} is not a ACLMessage Type")
            acl.performative = MessageType.NOT_UNDERSTOOD
            acl.content = msg
        # Compress to a task piece
        memory_piece = self.agent.memory_handler.generate_memory_from_message(acl, priority=priority)
        # Push to Queue
        self.agent.memory_handler.wait_and_put(self.agent.memory_pieces_queue, memory_piece)

    def run(self):
        """
            start a loop to receive the data
        """
        # 1.start all consumers.
        self._subscribe()
        # 2.Starting to wait for agent send the message.
        self.step()
        logger.info("Transport Started.")

    # ---------------Abc Func------------------
    @abc.abstractmethod
    def _subscribe(self, message: Union[ACLMessage, None] = None):
        """
            Subscribe  the topic depend on the message's receivers
        """

    @abc.abstractmethod
    def _produce(self, message):
        """
            Send message to topic which describe in message.receivers.
        """
