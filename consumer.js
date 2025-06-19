const { kafka } = require("./client")

const groupId = process.argv[2] || "group-1";

async function init(){
    const consumer = kafka.consumer({ groupId: groupId});
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