const { Kafka } = require('kafkajs');

function generateTimestamp() {
    const timestamp = new Date().toISOString();
    return timestamp;
  }
  
function generateFapiInteractionId() {
  const fapiInteractionId = Math.random().toString(36).substring(2, 45);
  return fapiInteractionId;
}

async function produceMessage() {
  const kafka = new Kafka({
    clientId: 'notifications_group-0',
    brokers: ['localhost:9092'],
  });

  const producer = kafka.producer();

  await producer.connect();

  const topic = 'queue_kafka';
  
  const message = {
    timestamp: generateTimestamp(),
    fapi_interaction_id: generateFapiInteractionId(),
    endpoint: 'POST/queue/api-node/victor-api',
    additional_info: {
      consent_id: '123',
      person_type: 'pf',
    },
    status_code: 201,
    http_method: 'POST',
    client_org_id: '123',
    server_org_id: '123',
    process_timespan: 10,
    client_ssid: '123',
    endpoint_uri_prefix: '/POST/:id/queue',
  };
  
  await producer.send({
    topic,
    messages: [
      {
        value: JSON.stringify(message),
      },
    ],
  });

  await producer.disconnect();

  console.log('Message successfully sent to Kafka.');
}

produceMessage().catch((error) => {
  console.error('Error sending message to Kafka:', error);
});