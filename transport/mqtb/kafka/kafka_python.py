import json

from kafka import KafkaProducer, KafkaConsumer

from wise_agent.behaviours.transport.mqtb.kafka.behaviour import KafkaMessageTransportBehaviour


class PythonKafkaMessageTransportBehaviour(KafkaMessageTransportBehaviour):
    """
        Inherit from the kafka-python lib.
    """

    def __init__(self, agent):
        super(PythonKafkaMessageTransportBehaviour, self).__init__(agent)
        self.server_dict = None

    def _produce(self, message):
        """
            Send the message to topic.
            :param message:
            :return: is success
        """
        # If not receiver that is own.
        addresses = self._confirm_the_address(message)
        for server, topics in addresses.items():
            if server not in self._producers.keys():
                _producer = KafkaProducer(
                    bootstrap_servers=server,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                self._producers[server] = _producer
            else:
                _producer = self._producers[server]
            for t in topics:
                _producer.send(t, message.encode("json"))
                _producer.flush()

    def _new_consumer(self, server_host: str):
        # group_id = f"Agent_{server_host}_{int(time.time())}"
        consumer = KafkaConsumer(
            bootstrap_servers=server_host,
            # group_id=group_id
        )
        return consumer
