import time

from confluent_kafka import Producer as KafkaProducer, Consumer as KafkaConsumer

from wise_agent.behaviours.transport.mqtb.kafka.behaviour import KafkaMessageTransportBehaviour
from wise_agent.utility import logger, random_string


class ConfluentKafkaMessageTransportBehaviour(KafkaMessageTransportBehaviour):
    """
        Implement from the confluent-kafka lib.
    """

    def __init__(self, agent):
        super(ConfluentKafkaMessageTransportBehaviour, self).__init__(agent)

    def _produce(self, message):
        """
        Send the message to topic.
        :param message:
        """
        addresses = self._confirm_the_address(message)
        # TODO: Diff table will return diff value here: receiver is not common.
        for server, topics in addresses.items():
            if server not in self._producers.keys():
                _producer = KafkaProducer(
                    {
                        'bootstrap.servers': server,
                        'client.id': f"{self.name()}_{random_string(10)}_{int(time.time())}"
                    }
                )
                self._producers[server] = _producer
            else:
                _producer = self._producers[server]
            mes = None
            for t in topics:
                try:
                    _producer.produce(t, message.encode("json"), callback=self.delivery_report)
                except BufferError:
                    mes = "The internal producer message queue is full (``queue.buffering.max.messages`` exceeded)"
                except NotImplementedError:
                    mes = "Timestamp is specified without underlying library support."
                finally:
                    if mes:
                        logger.exception(mes)
                _producer.flush()

    def _new_consumer(self, server_host: str):
        group_id = f"Agent_{server_host}_{int(time.time())}"
        consumer = KafkaConsumer({
            'bootstrap.servers': server_host,
            'group.id': group_id,
        })
        return consumer

    @staticmethod
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.info('Message fail to: {}'.format(err))
        else:
            logger.info('Message success to {} [{}]'.format(
                msg.topic(), msg.partition()))
