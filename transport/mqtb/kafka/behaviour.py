import abc
from abc import ABC
from typing import Union, Dict, List

from wise_agent.acl import AID, ACLMessage
from wise_agent.acl.messages import MessageType
from wise_agent.behaviours.transport.mqtb.behaviour import MessageTransportBehaviour
from wise_agent.utility import logger, start_task


class KafkaMessageTransportBehaviour(MessageTransportBehaviour, ABC):
    def __init__(self, agent):
        super(KafkaMessageTransportBehaviour, self).__init__(agent)

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

    def _subscribe(self, message: Union[ACLMessage, None] = None):
        """
            Subscribe  the topic depend on the message's receivers
        """
        receivers = self._confirm_the_address(message)
        for server_host, topics in receivers.items():
            consumer_name = "{}".format(server_host)
            if consumer_name not in self.consumers.keys():
                _consumer = self._new_consumer(server_host)
                self.consumers[consumer_name] = _consumer
            else:
                _consumer = self.consumers[consumer_name]
            _consumer.subscribe(topics=topics)

    def on_start(self, *args, **kwargs):
        """
            Remember define a table here.
        """

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

    @abc.abstractmethod
    def _new_consumer(self, *args, **kwargs):
        """
            Create a new consumer
        """

    def _new_producer(self, *args, **kwargs):
        """
            Create a new publisher
        """
