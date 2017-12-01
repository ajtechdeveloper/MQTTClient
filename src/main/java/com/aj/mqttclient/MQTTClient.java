package com.aj.mqttclient;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MQTTClient {

    private static final Logger logger = LoggerFactory.getLogger(MQTTClient.class);

    public static void main(String[] args) {
        MqttClient mqttClient;
        String tmpDir = System.getProperty("java.io.tmpdir");
        String subscribeTopicName = "echo";
        String publishTopicName = "thing";
        String payload;
        MqttDefaultFilePersistence dataStore = new MqttDefaultFilePersistence(tmpDir);
        try {
            mqttClient = new MqttClient("tcp://localhost:1883", "thing1", dataStore);
            MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
            mqttConnectOptions.setUserName("/:guest");
            mqttConnectOptions.setPassword("guest".toCharArray());
            mqttConnectOptions.setCleanSession(false);
            mqttClient.connect(mqttConnectOptions);
            logger.info("Connected to Broker");
            mqttClient.subscribe(subscribeTopicName);
            logger.info(mqttClient.getClientId() + " subscribed to topic: {}", subscribeTopicName);
            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    logger.info("Connection lost to MQTT Broker");
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    logger.info("-------------------------------------------------");
                    logger.info("| Received ");
                    logger.info("| Topic: {}", topic);
                    logger.info("| Message: {}", new String(message.getPayload()));
                    logger.info("| QoS: {}", message.getQos());
                    logger.info("-------------------------------------------------");

                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                    logger.info("Delivery Complete");
                }
            });
            MqttMessage message = new MqttMessage();
            for (int i = 1; i < 6; i++) {
                payload = "Message " + i + " from Thing";
                message.setPayload(payload
                        .getBytes());
                logger.info("Set Payload: {}", payload);
                logger.info(mqttClient.getClientId() + " published to topic: {}", publishTopicName);
                //Qos 1
                mqttClient.publish(publishTopicName, message);
            }
        } catch (MqttException me) {
            logger.error("reason: {}", me.getReasonCode());
            logger.error("msg: {}", me.getMessage());
            logger.error("loc: {} ", me.getLocalizedMessage());
            logger.error("cause: {}", me.getCause());
            logger.error("excep: {}", me);
            me.printStackTrace();
        }
    }
}
