package com.server;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.ringoram.*;








/**
 * Asynchronous storage server for ORAM metadata, block reads, bucket updates,
 * path prefetching, and batched single-block eviction.
 */
public class Server {

    private final MathUtility math;
    private final ServerStorage storage;
    private final ByteSerialize seria;



    private final Lock initLock = new ReentrantLock();

    public Server() {
        this.math = new MathUtility();
        this.storage = new ServerStorage();
        this.seria = new ByteSerialize();
    }

    public void run() {
        try {

            AsynchronousChannelGroup threadGroup =
                    AsynchronousChannelGroup.withFixedThreadPool(
                            Configs.THREAD_FIXED,
                            Executors.defaultThreadFactory());

            AsynchronousServerSocketChannel channel =
                    AsynchronousServerSocketChannel.open(threadGroup)
                            .bind(new InetSocketAddress(Configs.SERVER_PORT));

            System.out.println("server wait for connection on port=" + Configs.SERVER_PORT);

            channel.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
                @Override
                public void completed(AsynchronousSocketChannel serveClientChannel, Void att) {

                    channel.accept(null, this);


                    Runnable procedure = () -> serveClient(serveClientChannel);
                    new Thread(procedure).start();
                }

                @Override
                public void failed(Throwable exc, Void att) {
                    exc.printStackTrace();
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("rawtypes")
    /** Processes framed RPC requests from one client connection in arrival order. */
    public void serveClient(AsynchronousSocketChannel mChannel) {
        try {

            mChannel.setOption(StandardSocketOptions.TCP_NODELAY,
                    Boolean.parseBoolean(System.getProperty("roost.tcpNoDelay", "true")));
            mChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            while (true) {


                ByteBuffer messageTypeAndSize = ByteBuffer.allocate(8);
                readFully(mChannel, messageTypeAndSize);
                messageTypeAndSize.flip();

                int[] typeAndSize = MessageUtility.parseTypeAndLength(messageTypeAndSize);
                int type = typeAndSize[0];
                int size = typeAndSize[1];
                messageTypeAndSize = null;


                ByteBuffer message = null;
                if (size > 0) {
                    message = ByteBuffer.allocate(size);
                    readFully(mChannel, message);
                    message.flip();
                }

                byte[] serializedResponse;
                byte[] responseHeader;



                if (type == MessageUtility.ORAM_INIT) {
                    serializedResponse = handleInit(message);
                    responseHeader = MessageUtility.createMessageHeaderBytes(
                            MessageUtility.ORAM_INIT, serializedResponse.length);

                    System.out.println("server INIT successful!");
                    System.out.println();

                } else if (type == MessageUtility.ORAM_GETMETA) {
                    serializedResponse = handleGetMeta(message);
                    responseHeader = MessageUtility.createMessageHeaderBytes(
                            MessageUtility.ORAM_GETMETA, serializedResponse.length);

                } else if (type == MessageUtility.ORAM_READBLOCK) {
                    serializedResponse = handleReadBlock(message);
                    responseHeader = MessageUtility.createMessageHeaderBytes(
                            MessageUtility.ORAM_READBLOCK, serializedResponse.length);

                } else if (type == MessageUtility.ORAM_READBLOCKS_PREFETCH) {
                    serializedResponse = handleReadBlocksPrefetch(message);
                    responseHeader = MessageUtility.createMessageHeaderBytes(
                            MessageUtility.ORAM_READBLOCKS_PREFETCH, serializedResponse.length);

                } else if (type == MessageUtility.ORAM_READBUCKET) {
                    serializedResponse = handleReadBucket(message);
                    responseHeader = MessageUtility.createMessageHeaderBytes(
                            MessageUtility.ORAM_READBUCKET, serializedResponse.length);

                } else if (type == MessageUtility.ORAM_WRITEBUCKET) {
                    serializedResponse = handleWriteBucket(message);
                    responseHeader = MessageUtility.createMessageHeaderBytes(
                            MessageUtility.ORAM_WRITEBUCKET, serializedResponse.length);

                } else if (type == MessageUtility.ORAM_BATCH_SINGLE_EVICT) {
                    serializedResponse = handleBatchSingleEvict(message);
                    responseHeader = MessageUtility.createMessageHeaderBytes(
                            MessageUtility.ORAM_BATCH_SINGLE_EVICT, serializedResponse.length);

                } else {

                    System.out.println("server received unknown message type: " + type);
                    serializedResponse = new byte[] { 0 };
                    responseHeader = MessageUtility.createMessageHeaderBytes(type, serializedResponse.length);
                }


                ByteBuffer responseMessage = ByteBuffer.wrap(Bytes.concat(responseHeader, serializedResponse));
                writeFully(mChannel, responseMessage);
            }
        } catch (Exception e) {
            try {
                mChannel.close();
            } catch (IOException ignored) {
            }
        }
    }


    private static void readFully(AsynchronousSocketChannel channel, ByteBuffer buffer)
            throws Exception {
        while (buffer.hasRemaining()) {
            Integer bytesRead = channel.read(buffer).get();
            if (bytesRead == null || bytesRead < 0) {
                throw new EOFException("Client closed the connection while sending a request");
            }
        }
    }

    private static void writeFully(AsynchronousSocketChannel channel, ByteBuffer buffer)
            throws Exception {
        while (buffer.hasRemaining()) {
            Integer bytesWritten = channel.write(buffer).get();
            if (bytesWritten == null || bytesWritten < 0) {
                throw new EOFException("Client closed the connection while receiving a response");
            }
        }
    }

    private byte[] handleInit(ByteBuffer message) {

        initLock.lock();
        try {

            if (message != null && message.remaining() >= Long.BYTES) {
                math.setSeed(message.getLong());
            }
            initServer();
        } finally {
            initLock.unlock();
        }
        return new byte[] { 1 };
    }

    private byte[] handleGetMeta(ByteBuffer message) throws IOException {
        System.out.println("server processes GETMETA request.");

        if (message == null || message.remaining() < 4) {
            return new byte[] { 0 };
        }

        byte[] pos_bytes = new byte[4];
        message.get(pos_bytes);
        int pos = Ints.fromByteArray(pos_bytes);


        BucketMetadata[] meta_list = new BucketMetadata[Configs.HEIGHT];
        byte[][] meta_2d_bytes = new byte[Configs.HEIGHT][];

        int index = 0;
        for (int pos_run = pos; pos_run >= 0; pos_run = (pos_run - 1) >> 1) {
            Bucket bucket = storage.get_bucket(pos_run);
            meta_list[index] = bucket.getBucket_meta();
            meta_2d_bytes[index] = seria.metadataSerialize(meta_list[index]);
            index++;
            if (pos_run == 0) break;
        }


        byte[] meta_bytes = meta_2d_bytes[0];
        for (int i = 1; i < Configs.HEIGHT; i++) {
            meta_bytes = Bytes.concat(meta_bytes, meta_2d_bytes[i]);
        }
        return meta_bytes;
    }

    /** Reads one requested or dummy slot from each level and returns their XOR aggregate. */
    private byte[] handleReadBlock(ByteBuffer message) throws IOException {
        System.out.println("server processes READBLOCK request.");

        if (message == null || message.remaining() < (4 + Configs.HEIGHT * 4)) {
            return new byte[] { 0 };
        }

        byte[] pos_bytes = new byte[4];
        message.get(pos_bytes);
        int position = Ints.fromByteArray(pos_bytes);

        byte[] read_offset_bytes = new byte[Configs.HEIGHT * 4];
        message.get(read_offset_bytes);

        int[] read_offset = new int[Configs.HEIGHT];
        for (int i = 0; i < Configs.HEIGHT; i++) {
            read_offset[i] = Ints.fromByteArray(
                    Arrays.copyOfRange(read_offset_bytes, i * 4, (i + 1) * 4));
        }


        byte[] responseBytes = new byte[Configs.BLOCK_DATA_LEN];

        for (int pos_run = position, i = 0; pos_run >= 0; pos_run = (pos_run - 1) >> 1, i++) {
            Bucket bucket = storage.get_bucket(pos_run);


            if (read_offset[i] < 0 || read_offset[i] >= Configs.Z) {
                System.out.println("[WARN] READBLOCK offset out of range: offset=" + read_offset[i]
                        + " (Z=" + Configs.Z + ") pos=" + pos_run + " level=" + i);

            } else {
                byte[] block_data = bucket.getBlock(read_offset[i]);


                bucket.reset_valid_bits(read_offset[i]);
                bucket.add_read_counter();

                storage.set_bucket(pos_run, bucket);

                for (int j = 0; j < Configs.BLOCK_DATA_LEN; j++) {
                    responseBytes[j] ^= block_data[j];
                }
            }

            if (pos_run == 0) break;
        }

        return responseBytes;
    }

    /** Returns and invalidates every valid real block on the requested leaf-to-root path. */
    private byte[] handleReadBlocksPrefetch(ByteBuffer message) throws IOException {
        if (message == null || message.remaining() < 4) {
            return new byte[0];
        }

        byte[] posBytes = new byte[4];
        message.get(posBytes);
        int position = Ints.fromByteArray(posBytes);



        java.io.ByteArrayOutputStream response = new java.io.ByteArrayOutputStream(
                Configs.HEIGHT * Configs.REAL_BLOCK_COUNT * Configs.BLOCK_DATA_LEN);
        for (int posRun = position, i = 0; posRun >= 0; posRun = (posRun - 1) >> 1, i++) {
            Bucket bucket = storage.get_bucket(posRun);
            BucketMetadata meta = bucket.getBucket_meta();
            int[] blockIndexes = meta.get_block_index();
            int[] offsets = meta.get_offset();
            byte[] validBits = meta.getValid_bits();
            for (int slot = 0; slot < Configs.REAL_BLOCK_COUNT; slot++) {
                int offset = offsets[slot];
                if (blockIndexes[slot] >= 0 && offset >= 0 && offset < Configs.Z
                        && validBits[offset] == (byte) 1) {
                    response.write(bucket.getBlock(offset));
                    bucket.reset_valid_bits(offset);
                }
            }
            bucket.add_read_counter();
            storage.set_bucket(posRun, bucket);
            if (posRun == 0) break;
        }
        return response.toByteArray();
    }

    private byte[] handleReadBucket(ByteBuffer message) throws IOException {


        if (message == null || message.remaining() < 4) {
            return new byte[] { 0 };
        }

        byte[] bucket_id_bytes = new byte[4];
        message.get(bucket_id_bytes);
        int bucket_id = Ints.fromByteArray(bucket_id_bytes);

        Bucket bucket = storage.get_bucket(bucket_id);
        return seria.bucketSerialize(bucket);
    }

    private byte[] handleWriteBucket(ByteBuffer message) throws IOException {

        int bucket_bytes_len = 4 + Configs.Z * Configs.BLOCK_DATA_LEN + Configs.METADATA_BYTES_LEN;

        if (message == null || message.remaining() < bucket_bytes_len) {
            return new byte[] { 0 };
        }

        byte[] bucket_bytes = new byte[bucket_bytes_len];
        message.get(bucket_bytes);

        Bucket bucket = seria.bucketFromSerialize(bucket_bytes);
        storage.set_bucket(bucket.getId(), bucket);

        return new byte[] { 1 };
    }

    /** Attempts multiple independent stash write-backs while using a single RPC. */
    private byte[] handleBatchSingleEvict(ByteBuffer message) throws IOException {


        if (message == null || message.remaining() < 4) {
            return Bytes.concat(Ints.toByteArray(0), Ints.toByteArray(0), Ints.toByteArray(0));
        }

        int candidateCount = message.getInt();
        int candidateBytes = 12 + Configs.BLOCK_DATA_LEN;
        if (candidateCount < 0
                || candidateCount > Configs.MAX_BATCH_EVICT_BLOCKS_PER_ACCESS
                || message.remaining() != candidateCount * candidateBytes) {
            return Bytes.concat(Ints.toByteArray(0), Ints.toByteArray(0), Ints.toByteArray(0));
        }

        int successMask = 0;
        int bucketReads = 0;
        int bucketWrites = 0;
        for (int candidateIndex = 0; candidateIndex < candidateCount; candidateIndex++) {
            int blockId = message.getInt();
            int leafId = message.getInt();
            int clusterRootId = message.getInt();
            byte[] blockData = new byte[Configs.BLOCK_DATA_LEN];
            message.get(blockData);
            if (blockId < 0 || blockId >= Configs.BLOCK_COUNT
                    || leafId < Configs.LEAF_START || leafId >= Configs.BUCKET_COUNT) {
                continue;
            }

            int[] result = trySingleEvict(blockId, leafId, clusterRootId, blockData);
            bucketReads += result[1];
            bucketWrites += result[2];
            if (result[0] == 1) {
                successMask |= (1 << candidateIndex);
            }
        }
        return Bytes.concat(
                Ints.toByteArray(successMask),
                Ints.toByteArray(bucketReads),
                Ints.toByteArray(bucketWrites));
    }

    /**
     * Places one block on its assigned path, searching from the cluster root toward
     * the leaf first and then toward the global root.
     */
    private int[] trySingleEvict(
            int blockId, int leafId, int clusterRootId, byte[] blockData) throws IOException {
        int[] pathLeafToRoot = new int[Configs.HEIGHT];
        int pathLength = 0;
        for (int bucketId = leafId; bucketId >= 0; bucketId = (bucketId - 1) >> 1) {
            pathLeafToRoot[pathLength++] = bucketId;
            if (bucketId == 0) break;
        }

        int clusterRootIndex = -1;
        for (int i = 0; i < pathLength; i++) {
            if (pathLeafToRoot[i] == clusterRootId) {
                clusterRootIndex = i;
                break;
            }
        }
        if (clusterRootIndex < 0) {
            return new int[] { 0, 0, 0 };
        }

        int bucketReads = 0;

        int[] searchOrder = new int[pathLength];
        int orderLength = 0;
        for (int i = clusterRootIndex; i >= 0; i--) {
            searchOrder[orderLength++] = pathLeafToRoot[i];
        }
        for (int i = clusterRootIndex + 1; i < pathLength; i++) {
            searchOrder[orderLength++] = pathLeafToRoot[i];
        }

        for (int i = 0; i < orderLength; i++) {
            int bucketId = searchOrder[i];
            Bucket bucket = storage.get_bucket(bucketId);
            bucketReads++;
            BucketMetadata meta = bucket.getBucket_meta();
            int[] blockIndexes = meta.get_block_index();
            int[] offsets = meta.get_offset();
            byte[] validBits = meta.getValid_bits();


            for (int slot = 0; slot < Configs.REAL_BLOCK_COUNT; slot++) {
                int offset = offsets[slot];
                if (blockIndexes[slot] == blockId && validBits[offset] == (byte) 1) {
                    writeBlockToBucketData(bucket.getBucket_data(), offset, blockData);
                    storage.set_bucket(bucketId, bucket);
                    return new int[] { 1, bucketReads, 1 };
                }
            }


            for (int slot = 0; slot < Configs.REAL_BLOCK_COUNT; slot++) {
                int offset = offsets[slot];
                if (blockIndexes[slot] < 0 || validBits[offset] == (byte) 0) {
                    writeBlockToBucketData(bucket.getBucket_data(), offset, blockData);
                    meta.set_blockIndex_bit(slot, blockId);
                    validBits[offset] = (byte) 1;
                    meta.setValid_bits(validBits);
                    storage.set_bucket(bucketId, bucket);
                    return new int[] { 1, bucketReads, 1 };
                }
            }
        }
        return new int[] { 0, bucketReads, 0 };
    }

    private void writeBlockToBucketData(byte[] bucketData, int offset, byte[] blockData) {
        int start = offset * Configs.BLOCK_DATA_LEN;
        Arrays.fill(bucketData, start, start + Configs.BLOCK_DATA_LEN, (byte) 0);
        System.arraycopy(blockData, 0, bucketData, start,
                Math.min(blockData.length, Configs.BLOCK_DATA_LEN));
    }

    public void initServer() {

        for (int i = 0; i < Configs.BUCKET_COUNT; i++) {
            Bucket bucket = new Bucket();
            bucket.getBucket_meta().init_block_index();
            bucket.getBucket_meta().set_offset(math.get_random_permutation(Configs.Z));
            try {
                storage.set_bucket(i, bucket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Server server = new Server();
        server.run();
    }
}
