import abc
from typing import Union, List, Dict

from wise_agent.acl import ACLMessage
from wise_agent.behaviours.transport.mqtb.behaviour import MessageTransportBehaviour
from wise_agent.utility import logger, start_task


class MQTTMessageTransportBehaviour(MessageTransportBehaviour):
    def __init__(self, agent):
        super(MQTTMessageTransportBehaviour, self).__init__(agent)
        self._clients = {}

    def pull(self, message):
        """
            It's a callback function in MQTT `on_message` that it will set a task in `pool_executor`
        Args:
            message:

        Returns:

        """
        self._running_tasks = start_task(self._running_tasks, self._tasks_pool,
                                         self._dispatch_consume_message, message)

    def _subscribe(self, message: Union[ACLMessage, None] = None):
        """
            Subscribe a topics from the message
        Args:
            message: Union[ACLMessage, None]
        """
        subs = self._table.get_subs()
        for message_info in subs:
            host = message_info.get("host")
            port = message_info.get("port")
            topics = [tuple(topic) for topic in message_info.get("topics")]
            userdata = {
                "username": message_info.get("username"),
                "password": message_info.get("password"),
            }
            _client = self._new_client(host, port, **userdata)
            _client.subscribe(topic=topics)
            self._running_tasks = start_task(self._running_tasks, self._tasks_pool,
                                             _client.loop_forever)

    def _produce(self, message: ACLMessage):
        """
            Produce a message to broker.
        Args:
            message: ACLMessage

        """
        pubs = self._confirm_the_address(self._table, message)
        for message_info in pubs:
            host = message_info.get("host")
            port = message_info.get("port")
            topics = [tuple(topic) for topic in message_info.get("topics")]
            userdata = {
                "username": message_info.get("username"),
                "password": message_info.get("password"),
            }
            _client = self._new_client(host, port, **userdata)
            _client.publish(topic=topics, payload=message.encode("json"))
            self._running_tasks = start_task(self._running_tasks, self._tasks_pool,
                                             _client.loop_forever)

    @abc.abstractmethod
    def _new_client(self, host, port, *args, **kwargs):
        """
            New a MQTT Client and connect to the server.
        Args:
            server: host:port
            *args:

        Returns: MQTT Client

        """
