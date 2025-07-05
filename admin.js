const {kafka} = require('./client');
async function init(){
    const admin = kafka.admin();
    console.log("Admin connecting...");
    await admin.connect();
    console.log("Connected to Kafka Admin");
    
    // List all topics
    const topics = await admin.listTopics();
    console.log("Topics:", topics);
    
    // Create a new topic
    await admin.createTopics({
        topics: [
            { topic: 'college-1', numPartitions: 2, replicationFactor: 1 },
            { topic: 'college-2', numPartitions: 3, replicationFactor: 1 }
        ]
    });
    console.log("Created topic 'college-1' and 'college-2'");

    // Delete a topic
    //await admin.deleteTopics({ topics: ['old-topic'] });
    //console.log("Deleted topic 'old-topic'");
    console.log("Disconnecting admin...");
    await admin.disconnect();
}
init().catch(console.error).finally(() => process.exit(0));