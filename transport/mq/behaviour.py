import abc
import asyncio
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Union, Dict, List

from wgent.acl import ACLMessage, AID
from wgent.base_types import TaskPiece
from wgent.behaviours.transport.behaviour import TransportBehaviour, AgentTable
from wgent.memory import MemoryHandler
from wgent.utility import start_task, logger


class MessageTransportBehaviour(TransportBehaviour):
    def __init__(self, agent):
        super(MessageTransportBehaviour, self).__init__(agent)
        self.priority = 5  # Transport priority from external information
        # Broker info
        self._producers = {}  # Record a producer
        self._pool_executor: Union[ThreadPoolExecutor] = ThreadPoolExecutor(
            max_workers=self.agent.config_handler.read().pool_size, thread_name_prefix='consumer_')
        self.consumers = {}  # consumers from diff server.
        self._running_tasks = {}

    # ---------------Main Func-----------------
    def push(self, message):
        # Add to the pool task.
        self._running_tasks = start_task(self._running_tasks, self._pool_executor, self._send_process, message)

    def pull(self, consumer):
        """
            Waiting the data and set the timeout to avoid the block in async.
        """
        while True:
            records = consumer.poll()
            if records is None:
                continue
            try:
                records = records.value()
            except AttributeError as e:
                logger.exception(f"records have attribute error: {e}")
            self._running_tasks = start_task(self._running_tasks, self._pool_executor,
                                             self._dispatch_consume_message, records)

    def _dispatch_consume_message(self, msg: Union[bytes]):
        """
            Take it into Queue.
        """
        # Decode the message
        if isinstance(msg, bytes):
            msg = msg.decode("utf8")
        acl = ACLMessage()
        acl.decode(msg)
        # Compress to a task piece
        memory_piece = self.agent.memory_handler.generate_memory_from_message(acl, priority=5)
        task_piece = TaskPiece(memory_piece=memory_piece, executors=None)
        # Push to Queue
        self.agent.memory_handler.wait_and_put(self.agent.memory_pieces_queue, task_piece)

    def step(self):
        """
            Receive a message and compress the data(MemoryPiece) to Queue(Agent.memory_piece)
        """
        for _, consumer in self.consumers.items():
            try:
                # Receive from lots of topics.
                self._running_tasks = start_task(self._running_tasks, self._pool_executor, self.pull, consumer)
                # TODO 1. Check the msg's type
                # TODO 2. Compress it to a MemoryPiece and place it to Agent.memory_piece
            except TimeoutError:
                continue

    async def run(self):
        """
            start a loop to receive the data
        """
        self._subscribe()
        self.step()
        logger.info("Transport Starting.")

    @staticmethod
    def _filter_receivers(message, table: AgentTable) -> tuple:
        """
            It is a Iterable function to filter the data
        """
        if not message.receivers:
            message.receivers.append(table.main_name)
        iter_r = iter(message.receivers)
        while True:
            try:
                receiver = next(iter_r)
                # Check AID
                if isinstance(receiver, AID):
                    if table.in_table(receiver):
                        addr = table.get(receiver)
                    else:
                        logger.info("Receiver AID not in tables")
                        continue
                elif isinstance(receiver, str):
                    if table.in_table(receiver):
                        addr = table.get(receiver)
                    else:
                        logger.info("Receiver AID not in tables")
                        continue
                else:
                    logger.info("Cannot identify the receiver format.")
                    continue
                yield tuple([receiver, addr[0], addr[1]])
            except StopIteration:
                break

    def _ensure_the_receivers_address(self, message: Union[ACLMessage, None]) -> Dict[str, List[str]]:
        # receivers = {'server_host': ['topics']}
        receivers: Dict[str, List[str]] = {}

        if message is None:
            receivers = self._table.return_as_sub()
        else:
            if message.receivers is not None:
                for r in message.receivers:
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
                receivers = self._table.return_as_sub()
        return receivers

    def _subscribe(self, message: Union[ACLMessage, None] = None):
        """
            Subscribe  the topic depend on the message's receivers
        """
        receivers = self._ensure_the_receivers_address(message)
        for server_host, topics in receivers.items():
            consumer_name = "{}".format(server_host)
            if consumer_name not in self.consumers.keys():
                _consumer = self._new_consumer(server_host)
                self.consumers[consumer_name] = _consumer
            else:
                _consumer = self.consumers[consumer_name]
            _consumer.subscribe(topics=topics)

    # ---------------Abc Func------------------
    @abc.abstractmethod
    def _send_process(self, message):
        """
            Send message to topic.
        """

    @abc.abstractmethod
    def _new_consumer(self, server_host: str):
        """
            Create a new consumer
        :param server_host:
        :return: KafkaConsumer
        """
