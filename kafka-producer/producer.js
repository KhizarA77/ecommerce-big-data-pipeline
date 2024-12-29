const { Worker } = require('worker_threads');
const path = require('path');
const os = require('os');
const readline = require('readline');
const fs = require('fs');

const transactionsFile = path.join(__dirname, '..', 'ecommerce_data.csv');
const topic = 'transactions';
const bootstrapServer = 'localhost:29092';

const numCPUs = os.cpus().length;

// Function to create worker threads
const createWorker = (start, end) => {
  console.log(`Starting worker for rows ${start} to ${end}`);
  return new Promise((resolve, reject) => {
    const worker = new Worker(path.join(__dirname, 'worker.js'), {
      workerData: { start, end, transactionsFile, topic, bootstrapServer },
    });

    worker.on('message', resolve);
    worker.on('error', reject);
    worker.on('exit', (code) => {
      if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
    });
  });
};

const countLines = async (filePath) => {
    return new Promise((resolve, reject) => {
      let lineCount = 0;
      const reader = readline.createInterface({
        input: fs.createReadStream(filePath),
        crlfDelay: Infinity,
      });
  
      reader.on('line', () => {
        lineCount++;
      });
  
      reader.on('close', () => {
        resolve(lineCount);
      });
  
      reader.on('error', reject);
    });
  };
  
  // Main function
  const run = async () => {
    const totalLines = await countLines(transactionsFile); // Dynamically count lines
    const numCPUs = os.cpus().length;
    const linesPerThread = Math.ceil(totalLines / numCPUs);
    const workers = [];
  
    console.log(`Using ${numCPUs} threads to process the CSV.`);
    console.log(`Total lines in CSV: ${totalLines}`);
  
    for (let i = 0; i < numCPUs; i++) {
      const start = i * linesPerThread + 1;
      const end = Math.min(start + linesPerThread - 1, totalLines);
  
      console.log(`Assigning rows ${start} to ${end} to worker ${i}`);
      if (start <= totalLines) {
        workers.push(createWorker(start, end));
      }
    }
  
    const results = await Promise.all(workers);
    const totalMessagesSent = results.reduce((sum, count) => sum + count, 0);
  
    console.log(`Finished processing. Total messages sent: ${totalMessagesSent}`);
  };
  
  run().catch(console.error);