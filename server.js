/**
 * Simple file storage server:
 * - Stores actual files on the local filesystem under STORAGE_DIR
 * - Stores file metadata (including the absolute disk path) in MongoDB Atlas
 *
 * IMPORTANT: read the comments in routes to see EXACTLY where uploaded files are written
 * and how the server reads them back for downloads.
 */

import express from 'express';
import multer from 'multer';
import fs from 'fs/promises';
import fsSync from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { MongoClient, ObjectId } from 'mongodb';
import { v4 as uuidv4 } from 'uuid';
import mime from 'mime-types';
import dotenv from 'dotenv';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';

dotenv.config();
// Resolve __dirname in ES Modules
// https://stackoverflow.com/questions/46745014/what-is-the-es6-equivalent-of-dirname
// https://nodejs.org/api/esm.html#importmetadirname
const __filename = fileURLToPath(import.meta.url); // import.meta.url is file://...
const __dirname = path.dirname(__filename);

// Load config from environment (.env)
const PORT = Number(process.env.PORT || 3000);
const BASE_URL = process.env.BASE_URL || `http://localhost:${PORT}`;

// STORAGE_DIR must be an absolute path - Provide a default to ./data/storage relative to this server.js file
const STORAGE_DIR = process.env.STORAGE_DIR
  ? path.resolve(process.env.STORAGE_DIR)
  : path.resolve(__dirname, 'data', 'storage');

const MONGODB_URI = process.env.MONGODB_URI;
const MONGODB_DB = process.env.MONGODB_DB || 'file-storage-service';

const ADMIN_KEY = process.env.ADMIN_KEY || 'admin-placeholder';
const CLIENT_KEY = process.env.CLIENT_KEY || 'client-placeholder';

const MAX_FILE_BYTES = Number(process.env.MAX_FILE_BYTES || 50 * 1024 * 1024); // default 50MB

if (!MONGODB_URI) {
  console.error(
    'MONGODB_URI not set in .env — metadata requires MongoDB Atlas'
  );
  process.exit(1);
}
/**
 * check if the storage directory exists
 */
try {
  // This will create STORAGE_DIR if it doesn't exist.
  // If it does exist, nothing will happen, and no error is thrown.
  await fs.mkdir(STORAGE_DIR, { recursive: true });
  console.log('Directory exists or was created.');
  // then check if system has access to this dir
  await fs.access(STORAGE_DIR);
} catch (err) {
  console.error(`Storage directory ${STORAGE_DIR} is not accessible:`, err);
  process.exit(1);
}

/**
 * MongoDB connection and collections
 */
const mongoClient = new MongoClient(MONGODB_URI, {});

try {
  await mongoClient.connect();
  console.log('Connected to MongoDB Atlas');
} catch (err) {
  console.error('Failed to connect to MongoDB Atlas:', err);
  process.exit(1);
}

const mongoDb = mongoClient.db(MONGODB_DB);
const filesCol = mongoDb.collection('files'); // metadata for files - files collection
const keysCol = mongoDb.collection('apiKeys'); // store API keys (admin + client) - apiKeys collection

/**
 * Ensure necessary API keys exist in DB (if not, they can be seeded using data from .env)
 */
async function seedKeys() {
  // on the 'key' field - check for existing ADMIN_KEY, if not existing, create one
  if (!(await keysCol.findOne({ key: ADMIN_KEY }))) {
    await keysCol.insertOne({
      key: ADMIN_KEY,
      name: 'admin',
      createdAt: new Date(),
    });
    console.log('Seeded admin key into apiKeys collection.');
  }
  // on the 'key' field - check for existing CLIENT_KEY, if not existing, create one
  if (!(await keysCol.findOne({ key: CLIENT_KEY }))) {
    await keysCol.insertOne({
      key: CLIENT_KEY,
      name: 'client',
      createdAt: new Date(),
    });
    console.log('Seeded client key into apiKeys collection.');
  }
}

try {
  await seedKeys();
  console.log('successfully seeded keys');
} catch (err) {
  console.error('Failed to connect to MongoDB Atlas:', err);
  process.exit(1);
}

/**
 * Multer setup
 * - destination: STORAGE_DIR (absolute path)
 * - filename: uuid + original extension
 *
 * When multer finishes handling the upload, the file will be written to:
 *   <STORAGE_DIR>/<generatedFilename>
 * Example:
 *   STORAGE_DIR = /home/pi/pi-file-storage/data/storage
 *   generatedFilename = 1a2b3c4d-... .jpg
 *   saved file path = /home/pi/pi-file-storage/data/storage/1a2b3c4d-....jpg
 *
 * We store that absolute path in MongoDB metadata so we can retrieve it later.
 */
const storage = multer.diskStorage({
  destination: function (_req, _file, cb) {
    // multer will write the file here (absolute path)
    cb(null, STORAGE_DIR);
  },
  filename: function (_req, file, cb) {
    const id = uuidv4();
    // Keep original file extension if present, otherwise infer from mimetype
    const ext =
      path.extname(file.originalname) ||
      `.${mime.extension(file.mimetype) || 'bin'}`;
    const filename = `${id}${ext}`;
    cb(null, filename);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: MAX_FILE_BYTES },
}); // now req.file is populated

const app = express();
app.use(helmet());
// Allow frontend dev origin (change if needed)
app.use(cors({ origin: ['http://localhost:5123', 'http://localhost:3002'] }));
app.use(express.json());

// Basic rate limit (tune for your use) windowMs: 1 minute, max 300 requests per IP
app.use(rateLimit({ windowMs: 60_000, max: 300 }));

/**
 * Middleware: check API key (x-api-key)
 * - For upload, listing, delete endpoints we require a valid API key.
 * - The client-side key (CLIENT_KEY) is intended for uploads from your React frontend.
 */
async function checkApiKey(req, res, next) {
  const key = req.header('x-api-key');
  if (!key) return res.status(401).json({ error: 'Missing x-api-key' });
  const k = await keysCol.findOne({ key });
  if (!k) return res.status(403).json({ error: 'Invalid x-api-key' });
  req.apiUser = k;
  next();
}

/**
 * POST /upload
 * - Protected with x-api-key (use CLIENT_KEY from .env when uploading from React)
 * - Accepts multipart/form-data field "file"
 *
 * Where the file is saved:
 *  - multer writes file to: <STORAGE_DIR>/<generatedFilename>
 *  - Example saved path (absolute): /home/pi/pi-file-storage/data/storage/1a2b3c4d-xxxx.jpg
 *
 * What gets saved in MongoDB:
 *  - A document in `files` collection with fields:
 *      {
 *         _id: ObjectId(...),
 *         filename: "<generatedFilename>",
 *         originalName: "<original filename from client>",
 *         mimeType: "<detected mime>",
 *         size: <bytes>,
 *         path: "<absolute path on Pi filesystem to the saved file>",
 *         uploader: "<x-api-key name>",
 *         createdAt: ISODate(...)
 *      }
 *
 * Example response contains `id` (stringified ObjectId) and `downloadUrl`.
 */
app.post('/upload', checkApiKey, upload.single('file'), async (req, res) => {
  try {
    if (!req.file)
      return res
        .status(400)
        .json({ error: 'No file uploaded (use field name "file")' });

    // Compute the absolute path where multer saved the file.
    // multer sets req.file.filename to the generated filename, and we know STORAGE_DIR used above,
    // so absolutePath = path.join(STORAGE_DIR, filename)
    const absolutePath = path.resolve(STORAGE_DIR, req.file.filename);

    const doc = {
      filename: req.file.filename,
      originalName: req.file.originalname,
      mimeType: req.file.mimetype,
      size: req.file.size,
      // <-- IMPORTANT: store the exact absolute file path on disk so we can locate the file later
      path: absolutePath,
      uploader: req.apiUser.name || null,
      createdAt: new Date(),
    };

    const result = await filesCol.insertOne(doc);

    // Public download URL
    const id = result.insertedId.toString();
    const downloadUrl = `${BASE_URL}/files/${id}`;

    return res.json({ id, downloadUrl, meta: doc });
  } catch (err) {
    console.error('Upload error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * GET /files
 * - Protected (x-api-key)
 * - Returns metadata list (paginated via ?limit & ?offset)
 *
 * Metadata returned includes the MongoDB _id (string) that you can use to build download links:
 *   GET /files/<id>
 */
app.get('/files', checkApiKey, async (req, res) => {
  const limit = Math.min(100, Number(req.query.limit || 50));
  const offset = Number(req.query.offset || 0);

  const files = await filesCol
    .find({})
    .sort({ createdAt: -1 })
    .skip(offset)
    .limit(limit)
    .toArray();

  // Convert _id to string and remove the disk path or keep it (we keep disk path if you want)
  const result = files.map((f) => ({
    id: f._id.toString(),
    filename: f.filename,
    originalName: f.originalName,
    mimeType: f.mimeType,
    size: f.size,
    // Note: path is absolute disk path on the Pi. You might not want to reveal this to end-users.
    path: f.path,
    uploader: f.uploader,
    createdAt: f.createdAt,
  }));

  const total = await filesCol.countDocuments();
  res.json({ total, files: result });
});

/**
 * GET /files/:id
 * - Public by default (no api-key required) so you can embed returned downloadUrl into web pages.
 * - Streams the file to the client, supports Range header for partial downloads/streaming.
 *
 * Retrieval process (exact steps):
 *   1) We look up the file metadata document in MongoDB by ObjectId(id).
 *   2) The metadata document contains the absolute path on disk (field `path`) e.g.:
 *        /home/pi/pi-file-storage/data/storage/1a2b3c4d-xxxx.jpg
 *   3) We use fs.stat() on that `path` to get file size and to implement Range requests.
 *   4) We create a read stream from that exact path and pipe it to the response.
 *
 * So the client gets bytes streamed directly from the file stored on the Pi at the saved path.
 */
app.get('/files/:id', async (req, res) => {
  const id = req.params.id;
  let meta;
  try {
    meta = await filesCol.findOne({ _id: new ObjectId(id) });
  } catch (err) {
    return res.status(400).send('Invalid file id');
  }
  if (!meta) return res.status(404).send('File not found');

  const filePath = meta.path; // <-- absolute path on the Pi where the file is stored
  if (!filePath) return res.status(500).send('File metadata missing path');

  try {
    // Ensure file exists
    await fs.access(filePath);

    const stat = await fs.stat(filePath);
    const total = stat.size;

    // Set headers
    res.setHeader('Content-Type', meta.mimeType || 'application/octet-stream');
    // Suggest a download filename (original name)
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="${meta.originalName || meta.filename}"`
    );
    res.setHeader('Accept-Ranges', 'bytes');

    const range = req.headers.range;
    if (range) {
      // Parse range "bytes=start-end"
      const match = /bytes=(\d*)-(\d*)/.exec(range);
      if (!match) return res.status(416).send('Invalid Range');

      const start = match[1] === '' ? 0 : parseInt(match[1], 10);
      const end = match[2] === '' ? total - 1 : parseInt(match[2], 10);
      if (start >= total || end >= total)
        return res.status(416).send('Range Not Satisfiable');

      res.status(206);
      res.setHeader('Content-Range', `bytes ${start}-${end}/${total}`);
      res.setHeader('Content-Length', String(end - start + 1));

      const stream = fsSync.createReadStream(filePath, { start, end });
      return stream.pipe(res);
    } else {
      res.setHeader('Content-Length', String(total));
      const stream = fsSync.createReadStream(filePath);
      return stream.pipe(res);
    }
  } catch (err) {
    console.error('Error streaming file:', err);
    return res.status(500).send('Error streaming file');
  }
});

/**
 * DELETE /files/:id
 * - Protected with x-api-key
 * - Deletes metadata document and removes file from disk
 *
 * Steps:
 *  1) Look up metadata by id => contains field `path` (absolute disk path)
 *  2) Delete the metadata doc from MongoDB
 *  3) Delete the file at that `path` from the Pi filesystem (fs.unlink)
 */
app.delete('/files/:id', checkApiKey, async (req, res) => {
  const id = req.params.id;
  let meta;
  try {
    const r = await filesCol.findOneAndDelete({ _id: new ObjectId(id) });
    meta = r.value;
    if (!meta) return res.status(404).json({ error: 'Not found' });
  } catch (err) {
    return res.status(400).json({ error: 'Invalid id' });
  }

  // Remove the file from disk (path from metadata)
  if (meta.path) {
    try {
      await fs.unlink(meta.path);
    } catch (err) {
      // If file deletion fails, log but still return success for metadata removal
      console.error(`Failed to delete file at ${meta.path}:`, err.message);
    }
  }

  res.json({ success: true });
});

/**
 * Health endpoint
 */
app.get('/health', (_req, res) => res.json({ ok: true }));

/**
 * Start server
 */
app.listen(PORT, () => {
  console.log(`File server listening on port ${PORT}`);
  console.log(`Storage dir (on disk): ${STORAGE_DIR}`);
  console.log(`Base URL: ${BASE_URL}`);
});

/**
 * Graceful shutdown: close Mongo connection on SIGINT/SIGTERM
 */
process.on('SIGINT', async () => {
  console.log('Shutting down...');
  await mongoClient.close();
  process.exit(0);
});
process.on('SIGTERM', async () => {
  console.log('Shutting down...');
  await mongoClient.close();
  process.exit(0);
});
