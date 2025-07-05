    
    const { kafka } = require('./client');

    async function init() {
    const producer = kafka.producer();
    await producer.connect();
    console.log("Connected to Kafka Producer");

    // Send messages
    await producer.send({
        topic: 'college-1',
        messages: [
            { value: 'Hello KafkaJS user!'  },
            { value: 'This is a message for college-1' },
            { value: 'Another message for college-1' },
        ],
    });
    console.log("Message sent to 'college-1'");

    await producer.send({
        topic: 'college-2',
        messages: [
            { value: 'Hello KafkaJS user!' },
            { value: 'This is a message for college-2' },
            { value: 'Another message for college-2' },
            { value: 'Yet another message for college-2' },
            { value: 'Final message for college-2' },
        ],
    });
    console.log("Message sent to 'college-2'");

    await producer.disconnect();
    console.log("Disconnected from Kafka Producer");
}

init().catch(console.error).finally(() => process.exit(0));
