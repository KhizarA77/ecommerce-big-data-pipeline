const { Kafka } = require('kafkajs');
const fs = require('fs');
const csvParser = require('csv-parser');
const { workerData, parentPort } = require('worker_threads');

const { start, end, transactionsFile, topic, bootstrapServer } = workerData;

const kafka = new Kafka({
  clientId: `worker-${start}-${end}`,
  brokers: [bootstrapServer],
});

const producer = kafka.producer();

const sendToKafka = async (message) => {
  try {
    await producer.send({
      topic: topic,
      messages: [{ value: JSON.stringify(message) }],
    });
  } catch (error) {
    console.error(`Error sending message:`, error);
  }
};

const processCSV = async () => {
  await producer.connect();

  let messageCount = 0;
  let rowNumber = 0;

  fs.createReadStream(transactionsFile)
    .pipe(csvParser())
    .on('data', async (row) => {
      rowNumber++;
      if (rowNumber >= start && rowNumber <= end) {
        await sendToKafka(row);
        messageCount++;
      }
    })
    .on('end', async () => {
      console.log(`Worker processed ${messageCount} messages.`);
      await producer.disconnect();
      parentPort.postMessage(messageCount);
    })
    .on('error', (error) => {
      console.error('Error reading CSV:', error);
    });
};

processCSV().catch(console.error);
