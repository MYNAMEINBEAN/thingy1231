const fs = require('fs');
const path = require('path');
const express = require('express');
const multer = require('multer');
const { parser } = require('stream-json');
const { streamObject } = require('stream-json/streamers/StreamObject');
const { streamArray } = require('stream-json/streamers/StreamArray');
const { Chain } = require('stream-chain');

const app = express();
// Configure multer to store files temporarily in the 'uploads/' directory
const upload = multer({ dest: path.join(__dirname, 'uploads/') }); 

const PORT = process.env.PORT || 10000;

// --- Utility Functions (Combined from converter.js) ---

const KEY_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-=";

/** Converts an integer ID to a short key string. */
function intToKey(n) {
    if (!Number.isInteger(n) || n < 0) return "";
    if (n === 0) return KEY_CHARS[0];
    const base = KEY_CHARS.length;
    let out = [];
    let tempN = n;
    while (tempN > 0) {
        out.push(KEY_CHARS[tempN % base]);
        tempN = Math.floor(tempN / base);
    }
    return out.reverse().join("");
}

/** Checks if a string looks like a valid key. */
function looksLikeKey(s) {
    return typeof s === "string" && /^[A-Za-z0-9\-=:]+$/.test(s);
}

/** Checks if a value looks like a valid index tuple [icon, name, cost]. */
function looksLikeIndexTuple(v) {
    return Array.isArray(v) && v.length >= 3 && typeof v[0] === "string" && typeof v[1] === "string" && Number.isFinite(Number(v[2]));
}

/** Checks if a string looks like the compressed data string (e.g., A,B,1;C,D,2). */
function looksLikeRecipeString(s) {
    if (typeof s !== "string") return false;
    return /^(?:[A-Za-z0-9\-==]+,[A-Za-z0-9\-==]+,[A-Za-z0-9\-==]+)(?:;[A-Za-z0-9\-==]+,[A-Za-z0-9\-==]+,[A-Za-z0-9\-==]+)*$/.test(s.trim());
}

// --- Streaming Conversion Logic ---

/**
 * Converts a large JSON file using streaming.
 * @param {string} inputFile - Path to the input JSON file.
 * @param {string} outputFile - Path where the converted JSON will be written.
 */
function streamConvert(inputFile, outputFile) {
    return new Promise((resolve, reject) => {
        const index = {};
        let dataStr = "";
        let n = 0;
        
        // Use a temp file for output since the process is asynchronous
        const tempOutputFile = outputFile + '.tmp';

        // Ensure the directory exists for converted files (optional but safer)
        const outputDir = path.dirname(outputFile);
        if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir, { recursive: true });
        }

        // Helper to define the actual conversion process once root type is determined
        const processStream = (pipeline) => {
            pipeline.on('error', (err) => {
                console.error(`STREAM PARSING FAILED: ${err.message}`);
                reject(new Error(`JSON parsing failed (check file format): ${err.message}`));
            });

            pipeline.on('data', ({ key, value }) => {
                // Case 1: Checking for a 'data' string field (like in recipe format)
                if (key === 'data' && typeof value === 'string' && looksLikeRecipeString(value)) {
                    dataStr = value.trim();
                } 
                // Case 2: Checking for an 'index' entry [icon, name, cost]
                else if (looksLikeKey(key) && looksLikeIndexTuple(value)) {
                    index[key] = [value[0], value[1], Number(value[2])];
                } 
                // Case 3: Checking for an array element (like in list-of-items format)
                else if (value && Array.isArray(value) && value.length >= 4) {
                    const icon = value[1];
                    const name = value[2];
                    const cost = Number(value[3]);
                    if (typeof icon === "string" && typeof name === "string" && Number.isFinite(cost)) {
                        const key = intToKey(n++);
                        index[key] = [icon, name, cost];
                    }
                }
            });

            pipeline.on('end', () => {
                try {
                    // Final output: compact JSON (null, 0) is CRITICAL for file size and speed
                    const outputData = { index, data: dataStr };
                    const jsonStr = JSON.stringify(outputData, null, 0); 
                    
                    fs.writeFileSync(tempOutputFile, jsonStr, 'utf8');
                    fs.renameSync(tempOutputFile, outputFile); // Rename on success
                    resolve();
                } catch (e) {
                    reject(e);
                }
            });
        };

        // First pass: Detect the root structure (Object or Array) to select the correct streamer
        const rootDetector = new Chain([fs.createReadStream(inputFile), parser()]);
        
        rootDetector.on('data', (chunk) => {
            rootDetector.destroy(); // Stop detecting after the first token
            
            let processor;
            if (chunk.name === 'startObject') {
                processor = new Chain([fs.createReadStream(inputFile), parser(), streamObject()]);
            } else if (chunk.name === 'startArray') {
                processor = new Chain([fs.createReadStream(inputFile), parser(), streamArray()]);
            } else {
                reject(new Error("File does not appear to be a JSON object or array at the root."));
                return;
            }
            
            // Start the main processing stream
            processStream(processor);
        });

        rootDetector.on('error', reject);
    });
}

// --- Express Routing and Cleanup ---

// Health check endpoint
app.get('/', (req, res) => {
    res.send('JSON Streaming Converter API is running.');
});

// Conversion endpoint
app.post('/convert', upload.single('jsonFile'), async (req, res) => {
    if (!req.file) {
        return res.status(400).send('No file uploaded.');
    }

    const inputPath = req.file.path;
    const outputPath = path.join(__dirname, 'converted', path.basename(inputPath) + '.out.json');
    
    // Set a high timeout for the request, as 100MB files take time to process
    req.setTimeout(5 * 60 * 1000); // 5 minutes

    const cleanup = () => {
        // Function to safely delete temporary files
        if (fs.existsSync(inputPath)) fs.unlink(inputPath, () => {});
        if (fs.existsSync(outputPath)) fs.unlink(outputPath, () => {});
    };

    try {
        await streamConvert(inputPath, outputPath);
        
        // Send the converted file back to the client
        res.download(outputPath, 'converted_output.json', (err) => {
            if (err) console.error('Error sending file:', err.message);
            cleanup(); // Clean up on download complete or error
        });

    } catch (error) {
        cleanup(); // Clean up on conversion failure
        console.error('Conversion Error:', error);
        res.status(500).send(`Conversion failed: ${error.message}`);
    }
});

// Start the server
app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});
