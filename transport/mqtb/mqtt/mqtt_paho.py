import time

import paho.mqtt.client as mqtt

from wise_agent.behaviours.transport.mqtb.mqtt.behaviour import MQTTMessageTransportBehaviour


class PahoMQTTMessageTransportBehaviour(MQTTMessageTransportBehaviour):
    def __init__(self, agent):
        super(PahoMQTTMessageTransportBehaviour, self).__init__(agent)

    def _new_client(self, server, *args):
        """
            New a MQTT Client and connect to the server.
        Args:
            server: host:port
            *args:

        Returns: MQTT Client

        """
        client_name = "{}".format(server)
        if client_name not in self._clients.keys():
            client_id = f"Agent_mqtt_client_{int(time.time())}"
            _client = mqtt.Client(client_id=client_id,
                                  clean_session=False,
                                  userdata=args)
            _client.username_pw_set(self._username, self._password)
            host, port = server.split(':')
            _client.connect(host, port)
            _client.on_message = self.pull
            self._clients[client_name] = _client
        else:
            _client = self._clients[client_name]
        return _client
