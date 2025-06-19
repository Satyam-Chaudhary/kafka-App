const {kafka} = require('./client')


async function init() {
    const producer = kafka.producer();

    console.log("Connecting Producer");
    
}