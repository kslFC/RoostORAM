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
 * ORAMサーバ。
 *
 * クライアントから受け取ったメッセージ種別に応じて、bucket初期化、metadata取得、
 * block読み出し、bucket読み書きを行う。サーバは論理ブロックの意味を理解せず、
 * bucketファイルを保存・返却する役割に徹する。
 */
public class Server {

    private final MathUtility math;
    private final ServerStorage storage;
    private final ByteSerialize seria;


    // 初期化要求が複数来ても、bucket初期化が二重に走らないようにする。
    private final Lock initLock = new ReentrantLock();

    public Server() {
        this.math = new MathUtility();
        this.storage = new ServerStorage();
        this.seria = new ByteSerialize();
    }

    public void run() {
        try {
            // 非同期サーバソケットを立ち上げ、クライアント接続ごとに処理スレッドを作る。
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
                    // accept next
                    channel.accept(null, this);

                    // serve this connection in a new thread
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
    public void serveClient(AsynchronousSocketChannel mChannel) {
        try {
            // クライアントと同じTCP条件に揃え、小さい応答の送信待ちを抑える。
            mChannel.setOption(StandardSocketOptions.TCP_NODELAY,
                    Boolean.parseBoolean(System.getProperty("roost.tcpNoDelay", "true")));
            mChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            while (true) {

                // メッセージは先頭8byteに type と payload size を持つ。
                ByteBuffer messageTypeAndSize = ByteBuffer.allocate(8);
                readFully(mChannel, messageTypeAndSize);
                messageTypeAndSize.flip();

                int[] typeAndSize = MessageUtility.parseTypeAndLength(messageTypeAndSize);
                int type = typeAndSize[0];
                int size = typeAndSize[1];
                messageTypeAndSize = null;

                // payloadがある場合だけ、指定サイズぶんを読み切る。
                ByteBuffer message = null;
                if (size > 0) {
                    message = ByteBuffer.allocate(size);
                    readFully(mChannel, message);
                    message.flip();
                }

                byte[] serializedResponse;
                byte[] responseHeader;

                // typeごとにORAM操作を分岐し、payloadのみをhandlerへ渡す。

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

                } else {
                    // 未対応 type の安全策（null送信防止）
                    System.out.println("server received unknown message type: " + type);
                    serializedResponse = new byte[] { 0 }; // error
                    responseHeader = MessageUtility.createMessageHeaderBytes(type, serializedResponse.length);
                }

                // responseも同じく header(type + size) + payload の形式で返す。
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

    /** TCPフレームが分割されても、指定されたメッセージ長まで確実に送受信する。 */
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
        // 各実験を独立させるため、ORAM_INITのたびに全bucketを空状態へ戻す。
        initLock.lock();
        try {
            // クライアントからseedが渡された場合は、初期bucketのoffset順列も再現する。
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

        // 指定leafからrootまでのpath上にあるbucket metadataを順に返す。
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

        // concat meta bytes
        byte[] meta_bytes = meta_2d_bytes[0];
        for (int i = 1; i < Configs.HEIGHT; i++) {
            meta_bytes = Bytes.concat(meta_bytes, meta_2d_bytes[i]);
        }
        return meta_bytes;
    }

    private byte[] handleReadBlock(ByteBuffer message) throws IOException {
        System.out.println("server processes READBLOCK request.");

        if (message == null || message.remaining() < (4 + Configs.HEIGHT * 4)) {
            return new byte[] { 0 };
        }

        byte[] pos_bytes = new byte[4];
        message.get(pos_bytes);
        int position = Ints.fromByteArray(pos_bytes); // path root->leaf index

        byte[] read_offset_bytes = new byte[Configs.HEIGHT * 4];
        message.get(read_offset_bytes);

        int[] read_offset = new int[Configs.HEIGHT];
        for (int i = 0; i < Configs.HEIGHT; i++) {
            read_offset[i] = Ints.fromByteArray(
                    Arrays.copyOfRange(read_offset_bytes, i * 4, (i + 1) * 4));
        }

        // Ring ORAMの通常READ: path上の各bucketから1offsetを読み、XORして返す。
        byte[] responseBytes = new byte[Configs.BLOCK_DATA_LEN]; // XOR accumulator

        for (int pos_run = position, i = 0; pos_run >= 0; pos_run = (pos_run - 1) >> 1, i++) {
            Bucket bucket = storage.get_bucket(pos_run);

            // out-of-range guard（デバッグしやすくする）
            if (read_offset[i] < 0 || read_offset[i] >= Configs.Z) {
                System.out.println("[WARN] READBLOCK offset out of range: offset=" + read_offset[i]
                        + " (Z=" + Configs.Z + ") pos=" + pos_run + " level=" + i);
                // ここでは何もしない（dummy扱い）
            } else {
                byte[] block_data = bucket.getBlock(read_offset[i]);

                // 読んだoffsetは次回shuffleまで再利用できないようvalid bitを落とす。
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

    private byte[] handleReadBlocksPrefetch(ByteBuffer message) throws IOException {
        if (message == null || message.remaining() < (4 + Configs.HEIGHT * 4 + 8)) {
            return new byte[2 * Configs.BLOCK_DATA_LEN];
        }

        byte[] posBytes = new byte[4];
        message.get(posBytes);
        int position = Ints.fromByteArray(posBytes);

        int[] readOffsets = new int[Configs.HEIGHT];
        for (int i = 0; i < Configs.HEIGHT; i++) {
            byte[] offsetBytes = new byte[4];
            message.get(offsetBytes);
            readOffsets[i] = Ints.fromByteArray(offsetBytes);
        }

        byte[] targetLevelBytes = new byte[4];
        byte[] prefetchLevelBytes = new byte[4];
        message.get(targetLevelBytes);
        message.get(prefetchLevelBytes);
        int targetLevel = Ints.fromByteArray(targetLevelBytes);
        int prefetchLevel = Ints.fromByteArray(prefetchLevelBytes);

        // targetと類似候補（最大1件）の2枠だけを固定長で返す。
        // path上では各bucketの指定offsetを1つずつ消費するが、全bucket内容は送信しない。
        byte[] response = new byte[2 * Configs.BLOCK_DATA_LEN];
        for (int posRun = position, i = 0; posRun >= 0; posRun = (posRun - 1) >> 1, i++) {
            Bucket bucket = storage.get_bucket(posRun);
            int offset = readOffsets[i];
            if (offset >= 0 && offset < Configs.Z) {
                byte[] blockData = bucket.getBlock(offset);
                if (i == targetLevel) {
                    System.arraycopy(blockData, 0, response, 0, Configs.BLOCK_DATA_LEN);
                } else if (i == prefetchLevel) {
                    System.arraycopy(blockData, 0, response,
                            Configs.BLOCK_DATA_LEN, Configs.BLOCK_DATA_LEN);
                }
                bucket.reset_valid_bits(offset);
                bucket.add_read_counter();
                storage.set_bucket(posRun, bucket);
            }
            if (posRun == 0) break;
        }
        return response;
    }

    private byte[] handleReadBucket(ByteBuffer message) throws IOException {
        // bucket全体を返す。path/single evict、dummy refresh、初期配置で使用する。
        // 現行LSH prefetchは専用READBLOCKS_PREFETCHを使うため、通常READでは呼ばれない。
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
        // クライアントが再構成したbucketを、そのままストレージへ保存する。
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

    public void initServer() {
        // すべてのbucketを空bucketとして作成し、real block枠は-1で初期化する。
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
