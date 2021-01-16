import time

import paho.mqtt.client as mqtt

from wise_agent.behaviours.transport.mqtb.mqtt.behaviour import MQTTMessageTransportBehaviour


class PahoMQTTMessageTransportBehaviour(MQTTMessageTransportBehaviour):
    def __init__(self, agent):
        super(PahoMQTTMessageTransportBehaviour, self).__init__(agent)

    def _new_client(self, host, port, *args, **kwargs):
        """
            New a MQTT Client and connect to the server.
        Args:
            server: host:port
            *args:

        Returns: MQTT Client

        """

        def on_connect(client, userdata, flags, rc):
            print("PahoMQTTMessageTransportBehaviour connected with result code " + str(rc))

        def on_message(client, userdata, msg):
            self.pull(msg.payload)

        super(PahoMQTTMessageTransportBehaviour, self)._new_client(host, port, *args, **kwargs)
        client_name = f"{host}:{port}"
        username = kwargs.get("username")
        password = kwargs.get("password")
        if client_name not in self._clients.keys():
            client_id = f"Agent_mqtt_client_{int(time.time())}"
            _client = mqtt.Client(client_id=client_id,
                                  clean_session=False)
            _client.username_pw_set(username, password)
            _client.on_message = on_message
            _client.on_connect = on_connect
            _client.connect(host, port, 60)
            self._clients[client_name] = _client
        else:
            _client = self._clients[client_name]
        return _client
