const { kafka } = require("./client")

async function init(){
    const consumer = kafka.consumer({ groupId: "user-1"});
    await consumer.connect();

    await consumer.subscribe({topics: ["rider-updates"], fromBeginning: true});

    await consumer.run({
        eachMessage: async({topic, partition, message}) => {
            const parsed = JSON.parse(message.value.toString());
            console.log(`[${topic}]: PART: ${partition}: message:` , parsed)
        }
    })


}
init();