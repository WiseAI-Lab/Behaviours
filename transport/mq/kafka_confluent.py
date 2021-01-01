import time

from confluent_kafka import Producer as KafkaProducer, Consumer as KafkaConsumer

from wgent.behaviours.transport.behaviour import AgentTable
from wgent.behaviours.transport.mq.behaviour import MessageTransportBehaviour
from wgent.utility import logger, random_string


class ConfluentKafkaTransportBehaviour(MessageTransportBehaviour):
    """
        Implement from the confluent-kafka lib.
    """

    def __init__(self, agent):
        super(ConfluentKafkaTransportBehaviour, self).__init__(agent)

    def _send_process(self, message):
        """
        Send the message to topic.
        :param message:
        """
        for receiver, server_host, topic in self._filter_receivers(message, self._table):
            name = f"{server_host}@{topic}"
            if name not in self._producers.keys():
                _producer = KafkaProducer(
                    {
                        'bootstrap.servers': server_host,
                        'client.id': f"{self.name()}_{random_string(10)}_{int(time.time())}"
                    }
                )
                self._producers[name] = _producer
            else:
                _producer = self._producers[name]
            mes = None
            try:
                _producer.produce(topic, message.encode("json"), callback=self.delivery_report)
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
