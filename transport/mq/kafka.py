import json
import time

from kafka import KafkaProducer, KafkaConsumer

from wgent.behaviours.transport.behaviour import AgentTable
from wgent.behaviours.transport.mq.behaviour import MessageTransportBehaviour
from wgent.utility import logger


class KafkaTransportBehaviour(MessageTransportBehaviour):
    """
        Implement from the kafka-python lib.
    """

    def __init__(self, agent):
        super(KafkaTransportBehaviour, self).__init__(agent)
        self.server_dict = None

    def _send_process(self, message):
        """
            Send the message to topic.
            :param message:
            :return: is success
        """
        # If not receiver that is own.
        iter_receivers = self._filter_receivers(message, self._table)
        for receiver, server_host, topic in iter_receivers:
            name = f"{server_host}@{topic}"
            if name not in self._producers.keys():
                _producer = KafkaProducer(
                    bootstrap_servers=server_host,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                self._producers[name] = _producer
            else:
                _producer = self._producers[name]
            future = _producer.send(topic, message.encode("json"))
            logger.info(f"TRANSPORT---------<{receiver}: {future.succeeded()}>---------TRANSPORT")
            _producer.flush()

    def _new_consumer(self, server_host: str):
        # group_id = f"Agent_{server_host}_{int(time.time())}"
        consumer = KafkaConsumer(
            bootstrap_servers=server_host,
            # group_id=group_id
        )
        return consumer
