import abc
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Union, Dict, List

from wise_agent.acl import ACLMessage
from wise_agent.acl.messages import MessageType
from wise_agent.behaviours.transport.behaviour import TransportBehaviour
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
        # config defined
        config = self.agent.config_handler.read()
        if config.is_process_pool:
            self._pool_executor: Union[ProcessPoolExecutor] = ProcessPoolExecutor(
                max_workers=config.pool_size)
        else:
            self._pool_executor: Union[ThreadPoolExecutor] = ThreadPoolExecutor(
                max_workers=config.pool_size, thread_name_prefix='transport_')
        self.config = config

    # -----------------About Table------------------------
    def _confirm_the_address(self, message: Union[ACLMessage, None]) -> Dict[str, List[str]]:
        """
            Confirm the address that agent easily dispatch.
        Args:
            message: Union[ACLMessage, None]

        Returns: Dict[str, List[str]]

        """
        receivers: Dict[str, List[str]] = {}

        if message is None:
            receivers = self._table.filter_as_sub()
        else:
            if message.receivers is not None:
                for r in message.receivers:  # It's the name as key.
                    if self._table.in_table(r):
                        info = self._table.get(r)
                        server_host = info.server_host
                        cur_topic = info.topic
                        if server_host in receivers.keys():
                            if cur_topic not in receivers[server_host]:
                                receivers[server_host].append(cur_topic)
                            else:
                                continue
                        else:
                            receivers[server_host] = [cur_topic]
            else:
                receivers = self._table.filter_as_sub()
        return receivers

    # ---------------Main Func-----------------
    def push(self, message):
        # Add to the pool task.
        self._running_tasks = start_task(self._running_tasks, self._pool_executor, self._produce, message)

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

    async def run(self):
        """
            start a loop to receive the data
        """
        # 1.start all consumers.
        self._subscribe()
        # 2.Starting to wait for agent send the message.
        self.step()
        logger.info("Transport Starting.")

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
