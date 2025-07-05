const {kafka} = require('./client');

async function init() {
    console.log("🚀 Initializing Kafka consumer...");
    const consumer = kafka.consumer({ 
        groupId: 'college-group',
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
    });
    
    // Add event listeners for debugging
    consumer.on('consumer.heartbeat', () => {
        console.log('💓 Consumer heartbeat');
    });

    consumer.on('consumer.connect', () => {
        console.log('🔗 Consumer connected');
    });

    consumer.on('consumer.disconnect', () => {
        console.log('🔌 Consumer disconnected');
    });

    consumer.on('consumer.group_join', (event) => {
        console.log('👥 Consumer joined group:', event);
    });

    consumer.on('consumer.crash', (event) => {
        console.log('💥 Consumer crashed:', event);
    });

    try {
        console.log("🔌 Connecting to Kafka...");
        await consumer.connect();
        console.log("✅ Connected to Kafka Consumer");

        // Subscribe to topics
        console.log("📡 Subscribing to topics...");
        await consumer.subscribe({ topic: 'college-1', fromBeginning: true });
        console.log("✅ Subscribed to college-1");
        
        await consumer.subscribe({ topic: 'college-2', fromBeginning: true });
        console.log("✅ Subscribed to college-2");

        // Consume messages
        console.log("👂 Consumer is running and listening for messages...");
        await consumer.run({
            eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
                try {
                    console.log("📨 === MESSAGE RECEIVED ===");
                    console.log(`📋 Topic: ${topic}`);
                    console.log(`🔢 Partition: ${partition}`);
                    console.log(`📍 Offset: ${message.offset}`);
                    console.log(`🔑 Key: ${message.key ? message.key.toString() : 'null'}`);
                    console.log(`💬 Value: ${message.value.toString()}`);
                    console.log(`⏰ Timestamp: ${message.timestamp}`);
                    console.log(`📊 Headers:`, message.headers ? JSON.stringify(message.headers) : 'none');
                    console.log("========================");
                    
                    // Call heartbeat to ensure we don't timeout
                    await heartbeat();
                } catch (error) {
                    console.error('❌ Error processing message:', error);
                }
            },
        });

        // Keep the consumer running - don't exit immediately
        console.log("🟢 Consumer setup complete. Press Ctrl+C to stop.");
        
    } catch (error) {
        console.error('💥 Error in consumer initialization:', error);
        throw error;
    }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
    console.log('🛑 Received SIGINT. Graceful shutdown...');
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('🛑 Received SIGTERM. Graceful shutdown...');
    process.exit(0);
});

// Add a keepalive to prevent the process from exiting
const keepAlive = setInterval(() => {
    console.log('⏰ Consumer is still running...', new Date().toISOString());
}, 30000); // Log every 30 seconds

init().catch((error) => {
    console.error('💥 Consumer error:', error);
    clearInterval(keepAlive);
    process.exit(1);
});
