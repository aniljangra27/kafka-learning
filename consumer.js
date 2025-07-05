const {kafka} = require('./client');

async function init() {
    console.log("Initializing Kafka consumer...");
    const consumer = kafka.consumer({ groupId: 'college-group' });
    
    console.log("Connecting to Kafka...");
    await consumer.connect();
    console.log("Connected to Kafka Consumer");

    // Subscribe to topics
    console.log("Subscribing to topics...");
    await consumer.subscribe({ topic: 'college-1', fromBeginning: true });
    console.log("Subscribed to college-1");
    
    await consumer.subscribe({ topic: 'college-2', fromBeginning: true });
    console.log("Subscribed to college-2");

    // Consume messages
    console.log("Consumer is running and listening for messages...");
    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            try {
                console.log("=== MESSAGE RECEIVED ===");
                console.log(`Topic: ${topic}`);
                console.log(`Partition: ${partition}`);
                console.log(`Offset: ${message.offset}`);
                console.log(`Key: ${message.key ? message.key.toString() : 'null'}`);
                console.log(`Value: ${message.value.toString()}`);
                console.log(`Timestamp: ${message.timestamp}`);
                console.log("========================");
                
                // Call heartbeat to ensure we don't timeout
                await heartbeat();
            } catch (error) {
                console.error('Error processing message:', error);
            }
        },
    });

    // Keep the consumer running - don't exit immediately
    console.log("Consumer setup complete. Press Ctrl+C to stop.");
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('Received SIGINT. Graceful shutdown...');
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('Received SIGTERM. Graceful shutdown...');
    process.exit(0);
});

init().catch((error) => {
    console.error('Consumer error:', error);
    process.exit(1);
});
