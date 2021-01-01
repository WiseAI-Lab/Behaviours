import abc
from typing import Union, List, Dict

from wise_agent.acl import ACLMessage
from wise_agent.acl.messages import MessageType
from wise_agent.behaviours.transport.mqtb.behaviour import MessageTransportBehaviour
from wise_agent.utility import logger, start_task


class MQTTMessageTransportBehaviour(MessageTransportBehaviour):
    def __init__(self, agent):
        super(MQTTMessageTransportBehaviour, self).__init__(agent)
        self._clients = {}
        config = agent.config_handler.read()
        mq_config = config.mq_config
        self._username = mq_config.get('username')
        self._password = mq_config.get('password')

    def pull(self, userdata, message):
        """
            It's a callback function in MQTT `on_message` that it will set a task in `pool_executor`
        Args:
            userdata:
            message:

        Returns:

        """
        self._running_tasks = start_task(self._running_tasks, self._pool_executor,
                                         self._dispatch_consume_message, message)

    def _subscribe(self, message: Union[ACLMessage, None] = None):
        """
            Subscribe a topics from the message
        Args:
            message: Union[ACLMessage, None]
        """
        addresses = self._confirm_the_address(message)
        for server_host, topics in addresses.items():
            _client = self._new_client(server_host)
            _client.subscribe(topic=topics)

    def _produce(self, message: ACLMessage):
        """
            Produce a message to broker.
        Args:
            message: ACLMessage

        """
        addresses = self._confirm_the_address(message)
        for server_host, topics in addresses.items():
            _client = self._new_client(server_host)
            _client.publish(topic=topics, payload=message.encode("json"))

    @abc.abstractmethod
    def _new_client(self, server, *args):
        """
            New a MQTT Client and connect to the server.
        Args:
            server: host:port
            *args:

        Returns: MQTT Client

        """
