package com.client;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.ringoram.*;
import com.ringoram.Configs.OPERATION;

import com.example.LSH;










/**
 * ORAM client supporting the Ring ORAM baseline and the LSH-based cache mode.
 * It owns the position map and stash, selects new leaves, and coordinates reads
 * and eviction requests with the storage server.
 */
public class Client implements ClientInterface {

    private static int requestID = 0;





    private static volatile ExperimentMetrics lastExperimentMetrics;

    private static final class ExperimentMetrics {
        final String mode;
        final boolean useLsh;
        final boolean useCache;
        final long accessSeed;
        final long oramSeed;
        final int transitions;
        final int totalReads;
        final int stashHits;
        final int serverReads;
        final double totalReadMs;
        final double meanReadMs;
        final double medianReadMs;
        final double p95ReadMs;
        final double p99ReadMs;
        final double stashHitRate;
        final double hitMeanMs;
        final double missMeanMs;
        final long measuredPhaseNs;
        final long otherNs;
        final int[] requestCounts;
        final int bucketFileReads;
        final int bucketFileWrites;
        final int[] groupReadCounts;
        final int[] groupStashHitCounts;
        final double[] groupHitRates;

        ExperimentMetrics(String mode, boolean useLsh, boolean useCache,
                long accessSeed, long oramSeed, int transitions,
                int totalReads, int stashHits, int serverReads,
                double totalReadMs, double meanReadMs, double medianReadMs,
                double p95ReadMs, double p99ReadMs, double stashHitRate,
                double hitMeanMs, double missMeanMs, long measuredPhaseNs, long otherNs,
                int[] requestCounts, int bucketFileReads, int bucketFileWrites,
                int[] groupReadCounts, int[] groupStashHitCounts, double[] groupHitRates) {
            this.mode = mode;
            this.useLsh = useLsh;
            this.useCache = useCache;
            this.accessSeed = accessSeed;
            this.oramSeed = oramSeed;
            this.transitions = transitions;
            this.totalReads = totalReads;
            this.stashHits = stashHits;
            this.serverReads = serverReads;
            this.totalReadMs = totalReadMs;
            this.meanReadMs = meanReadMs;
            this.medianReadMs = medianReadMs;
            this.p95ReadMs = p95ReadMs;
            this.p99ReadMs = p99ReadMs;
            this.stashHitRate = stashHitRate;
            this.hitMeanMs = hitMeanMs;
            this.missMeanMs = missMeanMs;
            this.measuredPhaseNs = measuredPhaseNs;
            this.otherNs = otherNs;
            this.requestCounts = Arrays.copyOf(requestCounts, requestCounts.length);
            this.bucketFileReads = bucketFileReads;
            this.bucketFileWrites = bucketFileWrites;
            this.groupReadCounts = Arrays.copyOf(groupReadCounts, groupReadCounts.length);
            this.groupStashHitCounts = Arrays.copyOf(groupStashHitCounts, groupStashHitCounts.length);
            this.groupHitRates = Arrays.copyOf(groupHitRates, groupHitRates.length);
        }
    }

    protected InetSocketAddress serverAddress;
    protected AsynchronousChannelGroup mThreadGroup;
    protected AsynchronousSocketChannel mChannel;

    private int evict_count;
    private int evict_g;
    private int[] position_map;

    public Stash stash;
    public ByteSerialize seria;
    public MathUtility math;


    private LSH lsh;
    private final int vectorDim;
    private final int leafBits;
    private final boolean useLshLeafAssignment;
    private final boolean useCache;
    private final long oramSeed;
    private final long serverOramSeed;
    private int readCount;
    private int stashHitCount;
    private int serverReadCount;
    private boolean lastAccessStashHit;
    private String[] lshLeafMedoids;
    private int[] lshClusterLeafStarts;
    private int[] lshClusterLeafCounts;
    private int lshClusterPrefixBits;
    private final String[] blockLshHashes;
    private long prefetchReadNanos;
    private long leafAssignmentNanos;
    private long delayedEvictNanos;
    private long refreshCheckNanos;
    private long baselineReadPathNanos;
    private long baselinePathEvictNanos;
    private long baselineEarlyReshuffleNanos;
    private int baselinePathEvictCount;
    private int baselineEarlyReshuffleBucketCount;
    private int batchEvictAttempts;
    private int batchEvictSuccesses;
    private int batchEvictedBlocks;
    private int batchEvictBucketReads;
    private int batchEvictBucketWrites;
    private int bucketRefreshAttempts;
    private int bucketRefreshSuccesses;
    private int fallbackEvictCount;
    private final int[] requestCounts = new int[8];

    @SuppressWarnings("rawtypes")
    public Client() {
        this(true, true, new SecureRandom().nextLong());
    }

    @SuppressWarnings("rawtypes")
    public Client(boolean useLshLeafAssignment) {
        this(useLshLeafAssignment, true, new SecureRandom().nextLong());
    }

    @SuppressWarnings("rawtypes")
    public Client(boolean useLshLeafAssignment, boolean useCache) {
        this(useLshLeafAssignment, useCache, new SecureRandom().nextLong());
    }

    @SuppressWarnings("rawtypes")
    public Client(boolean useLshLeafAssignment, boolean useCache, long oramSeed) {
        this.useLshLeafAssignment = useLshLeafAssignment;
        this.useCache = useCache;
        this.oramSeed = oramSeed;
        this.serverOramSeed = mixSeed(oramSeed ^ 0x3C6EF372FE94F82AL);
        this.evict_count = 0;
        this.evict_g = 0;
        this.position_map = new int[Configs.BLOCK_COUNT];
        this.readCount = 0;
        this.stashHitCount = 0;
        this.serverReadCount = 0;
        this.lastAccessStashHit = false;
        this.lshLeafMedoids = null;
        this.lshClusterLeafStarts = null;
        this.lshClusterLeafCounts = null;
        this.lshClusterPrefixBits = 0;
        this.blockLshHashes = new String[Configs.BLOCK_COUNT];

        this.stash = new Stash(mixSeed(oramSeed ^ 0xBB67AE8584CAA73BL));
        this.seria = new ByteSerialize();
        this.math = new MathUtility(mixSeed(oramSeed ^ 0x6A09E667F3BCC909L));


        this.vectorDim = Configs.BLOCK_DATA_LEN / 4;


        this.leafBits = calcLeafBits(Configs.LEAF_COUNT);


        try {
            int numBits = Math.max(256, leafBits);
            long seed = 42L;
            this.lsh = useLshLeafAssignment ? new LSH(numBits, vectorDim, seed) : null;
            if (useLshLeafAssignment) {
                System.out.println("LSH initialized. vectorDim=" + vectorDim + ", numBits=" + numBits);
            } else {
                System.out.println("Baseline Ring ORAM mode. READ reassignment uses random leaves.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("WARNING: LSH init failed. Fallback to random leaf.");
            this.lsh = null;
        }
        System.out.println("Cache mode: " + (useCache ? "ENABLED" : "DISABLED"));


        for (int i = 0; i < Configs.BLOCK_COUNT; i++) {
            this.position_map[i] = math.getRandomLeaf() + Configs.LEAF_START;
        }


        try {
            serverAddress = new InetSocketAddress(Configs.SERVER_HOSTNAME, Configs.SERVER_PORT);
            mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(
                    Configs.THREAD_FIXED,
                    Executors.defaultThreadFactory());
            mChannel = AsynchronousSocketChannel.open(mThreadGroup);

            mChannel.setOption(StandardSocketOptions.TCP_NODELAY,
                    Boolean.parseBoolean(System.getProperty("roost.tcpNoDelay", "true")));
            mChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            Future connection = mChannel.connect(serverAddress);
            connection.get();
            System.out.println("client connect to server successful!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getReadCount() {
        return readCount;
    }

    public int getStashHitCount() {
        return stashHitCount;
    }

    public int getServerReadCount() {
        return serverReadCount;
    }

    public long getPrefetchReadNanos() { return prefetchReadNanos; }
    public long getLeafAssignmentNanos() { return leafAssignmentNanos; }
    public long getDelayedEvictNanos() { return delayedEvictNanos; }
    public long getRefreshCheckNanos() { return refreshCheckNanos; }
    public long getBaselineReadPathNanos() { return baselineReadPathNanos; }
    public long getBaselinePathEvictNanos() { return baselinePathEvictNanos; }
    public long getBaselineEarlyReshuffleNanos() { return baselineEarlyReshuffleNanos; }
    public int getBaselinePathEvictCount() { return baselinePathEvictCount; }
    public int getBaselineEarlyReshuffleBucketCount() { return baselineEarlyReshuffleBucketCount; }
    public int getBatchEvictAttempts() { return batchEvictAttempts; }
    public int getBatchEvictSuccesses() { return batchEvictSuccesses; }
    public int getBatchEvictedBlocks() { return batchEvictedBlocks; }
    public int getBatchEvictBucketReads() { return batchEvictBucketReads; }
    public int getBatchEvictBucketWrites() { return batchEvictBucketWrites; }
    public int getBucketRefreshAttempts() { return bucketRefreshAttempts; }
    public int getBucketRefreshSuccesses() { return bucketRefreshSuccesses; }
    public int getFallbackEvictCount() { return fallbackEvictCount; }
    public int getRequestCount(int messageType) {
        return messageType >= 0 && messageType < requestCounts.length ? requestCounts[messageType] : 0;
    }


    public void resetExperimentState() {
        readCount = 0;
        stashHitCount = 0;
        serverReadCount = 0;
        lastAccessStashHit = false;
        evict_count = 0;
        evict_g = 0;
        prefetchReadNanos = 0L;
        leafAssignmentNanos = 0L;
        delayedEvictNanos = 0L;
        refreshCheckNanos = 0L;
        baselineReadPathNanos = 0L;
        baselinePathEvictNanos = 0L;
        baselineEarlyReshuffleNanos = 0L;
        baselinePathEvictCount = 0;
        baselineEarlyReshuffleBucketCount = 0;
        batchEvictAttempts = 0;
        batchEvictSuccesses = 0;
        batchEvictedBlocks = 0;
        batchEvictBucketReads = 0;
        batchEvictBucketWrites = 0;
        bucketRefreshAttempts = 0;
        bucketRefreshSuccesses = 0;
        fallbackEvictCount = 0;
        Arrays.fill(requestCounts, 0);
    }

    public boolean wasLastAccessStashHit() {
        return lastAccessStashHit;
    }

    private static int calcLeafBits(int leafCount) {

        int bits = 0;
        int v = 1;
        while (v < leafCount) {
            v <<= 1;
            bits++;
        }
        return Math.max(bits, 1);
    }

    private static long mixSeed(long value) {

        value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
        value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
        return value ^ (value >>> 31);
    }

    private static long deriveOramSeed(long accessSeed) {
        return mixSeed(accessSeed ^ 0x9E3779B97F4A7C15L);
    }

    private static int hashToLeafByBandMajority(String hash, int leafBits, int leafCount) {
        if (hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is empty");
        }
        if (leafBits <= 0) {
            throw new IllegalArgumentException("leafBits must be > 0");
        }

        int leafLocal = 0;
        for (int bit = 0; bit < leafBits; bit++) {
            int ones = 0;
            int total = 0;
            for (int i = bit; i < hash.length(); i += leafBits) {
                if (hash.charAt(i) == '1') {
                    ones++;
                }
                total++;
            }
            if (ones * 2 >= total) {
                leafLocal |= (1 << bit);
            }
        }
        return leafLocal % leafCount;
    }

    private static int hammingDistance(String a, String b) {
        if (a == null || b == null || a.length() != b.length()) {
            throw new IllegalArgumentException("hash length mismatch");
        }
        int distance = 0;
        for (int i = 0; i < a.length(); i++) {
            if (a.charAt(i) != b.charAt(i)) {
                distance++;
            }
        }
        return distance;
    }

    /** Maps an LSH hash to a leaf inside the prefix range of its nearest cluster. */
    private int chooseLeafFromHash(String hash) {


        if (lshLeafMedoids == null || lshClusterLeafStarts == null
                || lshClusterLeafCounts == null || lshLeafMedoids.length == 0) {

            return hashToLeafByBandMajority(hash, leafBits, Configs.LEAF_COUNT) + Configs.LEAF_START;
        }

        int bestCluster = 0;
        int bestDistance = Integer.MAX_VALUE;
        for (int i = 0; i < lshLeafMedoids.length; i++) {
            int distance = hammingDistance(hash, lshLeafMedoids[i]);
            if (distance < bestDistance) {
                bestDistance = distance;
                bestCluster = i;
            }
        }

        int suffixBits = leafBits - lshClusterPrefixBits;
        int suffix = 0;
        for (int i = 0; i < suffixBits; i++) {
            suffix = (suffix << 1) | (i < hash.length() && hash.charAt(i) == '1' ? 1 : 0);
        }
        int rangeCount = lshClusterLeafCounts[bestCluster];
        int suffixMask = rangeCount - 1;
        int randomSuffix = math.getRandomLeaf() & suffixMask;
        int leafOffset = suffix ^ randomSuffix;
        int leaf = Configs.LEAF_START + lshClusterLeafStarts[bestCluster] + leafOffset;
        int rangeStart = Configs.LEAF_START + lshClusterLeafStarts[bestCluster];
        int rangeEnd = rangeStart + rangeCount - 1;
        if (leaf < rangeStart || leaf > rangeEnd) {
            throw new IllegalStateException("LSH prefix placement escaped cluster range: cluster="
                    + bestCluster + " leaf=" + leaf + " range=" + rangeStart + "-" + rangeEnd);
        }
        return leaf;
    }

    private static String binaryPrefix(int value, int bits) {
        String binary = Integer.toBinaryString(value);
        if (binary.length() >= bits) {
            return binary;
        }
        StringBuilder padded = new StringBuilder(bits);
        for (int i = binary.length(); i < bits; i++) {
            padded.append('0');
        }
        return padded.append(binary).toString();
    }

    private int chooseLeafFromHashOrRandom(String hash) {
        if (hash == null) {
            return math.getRandomLeaf() + Configs.LEAF_START;
        }
        return chooseLeafFromHash(hash);
    }

    private static final class HashClusterResult {
        final String[] medoids;
        final int[] assignment;

        HashClusterResult(String[] medoids, int[] assignment) {
            this.medoids = medoids;
            this.assignment = assignment;
        }
    }

    /** Groups block hashes with Hamming-distance k-medoids. */
    private HashClusterResult clusterHashes(String[] hashes, int clusterCount) {
        int hashCount = hashes.length;
        String[] medoids = new String[clusterCount];


        int firstMedoid = 0;
        int firstTotalDistance = Integer.MAX_VALUE;
        for (int candidate = 0; candidate < hashCount; candidate++) {
            int totalDistance = 0;
            for (int other = 0; other < hashCount; other++) {
                totalDistance += hammingDistance(hashes[candidate], hashes[other]);
            }
            if (totalDistance < firstTotalDistance) {
                firstTotalDistance = totalDistance;
                firstMedoid = candidate;
            }
        }
        medoids[0] = hashes[firstMedoid];


        for (int c = 1; c < clusterCount; c++) {
            int farthest = 0;
            int farthestDistance = -1;
            for (int i = 0; i < hashCount; i++) {
                int nearestDistance = Integer.MAX_VALUE;
                for (int j = 0; j < c; j++) {
                    nearestDistance = Math.min(nearestDistance,
                            hammingDistance(hashes[i], medoids[j]));
                }
                if (nearestDistance > farthestDistance) {
                    farthestDistance = nearestDistance;
                    farthest = i;
                }
            }
            medoids[c] = hashes[farthest];
        }

        int[] assignment = new int[hashCount];
        Arrays.fill(assignment, -1);

        for (int iter = 0; iter < 20; iter++) {
            boolean changed = false;
            for (int i = 0; i < hashCount; i++) {
                int bestCluster = 0;
                int bestDistance = Integer.MAX_VALUE;
                for (int c = 0; c < clusterCount; c++) {
                    int distance = hammingDistance(hashes[i], medoids[c]);
                    if (distance < bestDistance) {
                        bestDistance = distance;
                        bestCluster = c;
                    }
                }
                if (assignment[i] != bestCluster) {
                    assignment[i] = bestCluster;
                    changed = true;
                }
            }

            for (int c = 0; c < clusterCount; c++) {
                String bestMedoid = medoids[c];
                int bestTotalDistance = Integer.MAX_VALUE;
                for (int candidate = 0; candidate < hashCount; candidate++) {
                    if (assignment[candidate] != c) continue;
                    int totalDistance = 0;
                    for (int other = 0; other < hashCount; other++) {
                        if (assignment[other] == c) {
                            totalDistance += hammingDistance(hashes[candidate], hashes[other]);
                        }
                    }
                    if (totalDistance < bestTotalDistance) {
                        bestTotalDistance = totalDistance;
                        bestMedoid = hashes[candidate];
                    }
                }
                medoids[c] = bestMedoid;
            }
            if (!changed) break;
        }
        return new HashClusterResult(medoids, assignment);
    }

    private double silhouetteScore(String[] hashes, HashClusterResult result) {
        int hashCount = hashes.length;
        int clusterCount = result.medoids.length;
        double totalScore = 0.0;

        for (int i = 0; i < hashCount; i++) {
            int ownCluster = result.assignment[i];
            int ownCount = 0;
            double ownDistance = 0.0;
            for (int j = 0; j < hashCount; j++) {
                if (i != j && result.assignment[j] == ownCluster) {
                    ownDistance += hammingDistance(hashes[i], hashes[j]);
                    ownCount++;
                }
            }
            if (ownCount == 0) continue;
            double a = ownDistance / ownCount;

            double b = Double.POSITIVE_INFINITY;
            for (int c = 0; c < clusterCount; c++) {
                if (c == ownCluster) continue;
                int otherCount = 0;
                double otherDistance = 0.0;
                for (int j = 0; j < hashCount; j++) {
                    if (result.assignment[j] == c) {
                        otherDistance += hammingDistance(hashes[i], hashes[j]);
                        otherCount++;
                    }
                }
                if (otherCount > 0) {
                    b = Math.min(b, otherDistance / otherCount);
                }
            }
            if (Double.isFinite(b) && Math.max(a, b) > 0.0) {
                totalScore += (b - a) / Math.max(a, b);
            }
        }
        return totalScore / hashCount;
    }

    /** Selects the cluster count by silhouette score and assigns one leaf prefix per cluster. */
    private void buildLshLeafClustersAuto(String dataDir, int nBlocks) {
        if (lsh == null) {
            return;
        }


        String[] hashes = new String[nBlocks];
        int[] blockIds = new int[nBlocks];
        int hashCount = 0;
        for (int i = 0; i < nBlocks; i++) {
            String path = dataDir + "/" + i + ".bin";
            try {
                byte[] data = loadBinAsBlockData(path);
                String hash = computeHashFromData(data);
                if (hash != null) {
                    if (i < blockLshHashes.length) {
                        blockLshHashes[i] = hash;
                    }
                    hashes[hashCount] = hash;
                    blockIds[hashCount] = i;
                    hashCount++;
                }
            } catch (Exception e) {
                System.out.println("[LSH-CLUSTER] skip block=" + i + " path=" + path
                        + " reason=" + e.getMessage());
            }
        }
        if (hashCount < 3) {
            System.out.println("[LSH-AUTO-CLUSTER] insufficientHashes=" + hashCount
                    + " fallback=bandMajority");
            return;
        }

        hashes = Arrays.copyOf(hashes, hashCount);
        blockIds = Arrays.copyOf(blockIds, hashCount);

        int maxClusters = Math.min(Configs.LEAF_COUNT, hashCount - 1);
        HashClusterResult selected = null;
        double selectedScore = Double.NEGATIVE_INFINITY;

        System.out.println("[LSH-AUTO-CLUSTER-START] candidates=2-" + maxClusters
                + " source=hashOnly metric=hamming selection=silhouette");
        for (int clusterCount = 2; clusterCount <= maxClusters; clusterCount++) {
            HashClusterResult candidate = clusterHashes(hashes, clusterCount);
            double score = silhouetteScore(hashes, candidate);
            System.out.println("[LSH-AUTO-CLUSTER] k=" + clusterCount
                    + " silhouette=" + String.format(java.util.Locale.ROOT, "%.4f", score));
            if (score > selectedScore) {
                selectedScore = score;
                selected = candidate;
            }
        }

        if (selected == null) return;
        int clusterCount = selected.medoids.length;
        lshLeafMedoids = selected.medoids;
        lshClusterLeafStarts = new int[clusterCount];
        lshClusterLeafCounts = new int[clusterCount];
        lshClusterPrefixBits = calcLeafBits(clusterCount);
        int leavesPerCluster = 1 << (leafBits - lshClusterPrefixBits);
        for (int c = 0; c < clusterCount; c++) {


            lshClusterLeafStarts[c] = c * leavesPerCluster;
            lshClusterLeafCounts[c] = leavesPerCluster;
        }

        System.out.println("[LSH-AUTO-CLUSTER-SELECTED] clusters=" + clusterCount
                + " rootPrefixBits=" + lshClusterPrefixBits
                + " leavesPerCluster=" + leavesPerCluster
                + " usedLeaves=" + (clusterCount * leavesPerCluster)
                + " silhouette=" + String.format(java.util.Locale.ROOT, "%.4f", selectedScore));
        for (int c = 0; c < clusterCount; c++) {
            StringBuilder blocks = new StringBuilder();
            for (int i = 0; i < hashCount; i++) {
                if (selected.assignment[i] == c) {
                    if (blocks.length() > 0) {
                        blocks.append(",");
                    }
                    blocks.append(blockIds[i]);
                }
            }
            System.out.println("[LSH-CLUSTER] cluster=" + c
                    + " rootPrefix=" + binaryPrefix(c, lshClusterPrefixBits)
                    + " leafRange=" + (Configs.LEAF_START + lshClusterLeafStarts[c])
                    + "-" + (Configs.LEAF_START + lshClusterLeafStarts[c]
                            + lshClusterLeafCounts[c] - 1)
                    + " medoid=" + lshLeafMedoids[c]
                    + " blocks=[" + blocks + "]");
        }
        System.out.println("[LSH-AUTO-CLUSTER-END]");
    }

    private String computeHashFromData(byte[] data) {
        if (lsh == null || data == null) {
            return null;
        }
        int needBytes = vectorDim * 4;
        if (data.length < needBytes) {
            return null;
        }


        ByteBuffer bb = ByteBuffer.wrap(data, 0, needBytes).order(ByteOrder.LITTLE_ENDIAN);
        float[] vec = new float[vectorDim];
        for (int i = 0; i < vectorDim; i++) {
            vec[i] = bb.getFloat();
        }
        return lsh.computeHash(vec);
    }

    public void initServer() {
        byte[] seedBytes = ByteBuffer.allocate(Long.BYTES).putLong(serverOramSeed).array();
        byte[] header = MessageUtility.createMessageHeaderBytes(
                MessageUtility.ORAM_INIT, seedBytes.length);
        ByteBuffer request = ByteBuffer.wrap(Bytes.concat(header, seedBytes));
        byte[] responseBytes = sendAndGetMessage(request, MessageUtility.ORAM_INIT);
        System.out.println("client INIT server successful!" + responseBytes[0]);
    }

    public void close() {
        try {
            mChannel.close();
            mThreadGroup.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }





    private int chooseLeafFromData(byte[] data) {
        if (lsh == null || data == null) {
            return math.getRandomLeaf() + Configs.LEAF_START;
        }
        int needBytes = vectorDim * 4;
        if (data.length < needBytes) {
            return math.getRandomLeaf() + Configs.LEAF_START;
        }

        return chooseLeafFromHashOrRandom(computeHashFromData(data));
    }










    /**
     * Executes one logical ORAM access. Cache misses read the assigned path,
     * successful reads receive a fresh leaf, and the selected mode then runs
     * either delayed LSH eviction or periodic Ring ORAM path eviction.
     */
    public byte[] oblivious_access(int blockIndex, OPERATION op, byte[] newdata) {
        requestID++;
        lastAccessStashHit = false;

        Block block = null;
        byte[] readData = null;
        BucketMetadata[] lshReadMeta = null;
        if (op == OPERATION.ORAM_ACCESS_READ) {
            readCount++;
        }


        int position = position_map[blockIndex];


        int position_new_write = -1;
        if (op == OPERATION.ORAM_ACCESS_WRITE) {
            position_new_write = math.getRandomLeaf() + Configs.LEAF_START;
            position_map[blockIndex] = position_new_write;
        }


        block = stash.find_by_blockIndex(blockIndex);
        if (block != null && op == OPERATION.ORAM_ACCESS_READ && useCache) {
            lastAccessStashHit = true;
            stashHitCount++;
            System.out.println("[STASH-HIT] blockIndex=" + blockIndex
                    + " leaf=" + block.getLeaf_id());
        } else if (op == OPERATION.ORAM_ACCESS_READ) {
            if (block != null) {
                System.out.println("[CACHE-BYPASS] blockIndex=" + blockIndex
                        + " leaf=" + block.getLeaf_id());
            }
            serverReadCount++;
            if (useLshLeafAssignment) {

                long phaseStart = System.nanoTime();
                lshReadMeta = read_path_all_real(position);
                prefetchReadNanos += System.nanoTime() - phaseStart;
            } else {
                long phaseStart = System.nanoTime();
                if (useCache) {

                    read_path(position, blockIndex);
                } else {


                    read_path_all_real(position);
                }
                baselineReadPathNanos += System.nanoTime() - phaseStart;
            }
            block = stash.find_by_blockIndex(blockIndex);
        } else if (block == null) {
            read_path(position, blockIndex);
            block = stash.find_by_blockIndex(blockIndex);
        }

        if (op == OPERATION.ORAM_ACCESS_WRITE) {
            if (block == null) {
                block = new Block(blockIndex, position_new_write, newdata);
                stash.add(block);
            } else {
                block.setData(newdata);
                stash.update_leaf_id(blockIndex, position_new_write);
            }
            readData = block.getData();

        } else {
            if (block != null) {
                readData = block.getData();
                int position_new;
                if (useLshLeafAssignment) {
                    long phaseStart = System.nanoTime();

                    String readHash = blockIndex >= 0 && blockIndex < blockLshHashes.length
                            ? blockLshHashes[blockIndex] : null;
                    if (readHash == null) {
                        readHash = computeHashFromData(readData);
                    }
                    position_new = chooseLeafFromHashOrRandom(readHash);
                    leafAssignmentNanos += System.nanoTime() - phaseStart;
                    System.out.println("[LSH-READ] blockIndex=" + blockIndex
                            + " oldLeaf=" + position
                            + " hash=" + readHash
                            + " newLeaf=" + position_new);
                } else {
                    position_new = math.getRandomLeaf() + Configs.LEAF_START;
                    System.out.println("[BASELINE-READ] blockIndex=" + blockIndex
                            + " oldLeaf=" + position
                            + " newLeaf=" + position_new);
                }

                position_map[blockIndex] = position_new;
                stash.update_leaf_id(blockIndex, position_new);
            } else {
                readData = null;
            }
        }

        if (useLshLeafAssignment) {


            if (op == OPERATION.ORAM_ACCESS_READ) {

                if (!lastAccessStashHit && lshReadMeta != null) {
                    long refreshStart = System.nanoTime();
                    refresh_exhausted_buckets(position, lshReadMeta, true);
                    refreshCheckNanos += System.nanoTime() - refreshStart;
                }
                long phaseStart = System.nanoTime();


                delayed_batch_evict_after_access(blockIndex, !lastAccessStashHit);
                delayedEvictNanos += System.nanoTime() - phaseStart;
            } else {

                BucketMetadata[] meta_list = get_metadata(position);
                refresh_exhausted_buckets(position, meta_list, false);
            }
        } else {

            evict_count = (evict_count + 1) % Configs.SHUFFLE_RATE;
            if (evict_count == 0) {
                long phaseStart = System.nanoTime();
                evict_path(math.gen_reverse_lexicographic(evict_g, Configs.BUCKET_COUNT, Configs.HEIGHT));
                baselinePathEvictNanos += System.nanoTime() - phaseStart;
                baselinePathEvictCount++;
                evict_g = (evict_g + 1) % Configs.LEAF_COUNT;
            }


            long phaseStart = System.nanoTime();
            BucketMetadata[] meta_list = get_metadata(position);
            early_reshuffle(position, meta_list);
            baselineEarlyReshuffleNanos += System.nanoTime() - phaseStart;
        }

        return readData;
    }

    @Override
    public void read_path(int pathID, int blockIndex) {

        BucketMetadata[] meta_list = get_metadata(pathID);
        read_block(pathID, blockIndex, meta_list);
    }

    /** Fetches every valid real block on a path in one prefetch RPC and adds it to the stash. */
    public BucketMetadata[] read_path_all_real(int pathID) {


        BucketMetadata[] metaList = get_metadata(pathID);
        int[][] blockIds = new int[Configs.HEIGHT][Configs.REAL_BLOCK_COUNT];
        boolean[][] selected = new boolean[Configs.HEIGHT][Configs.REAL_BLOCK_COUNT];
        int selectedCount = 0;


        for (int i = 0; i < Configs.HEIGHT; i++) {
            BucketMetadata meta = metaList[i];
            for (int slot = 0; slot < Configs.REAL_BLOCK_COUNT; slot++) {
                int blockId = meta.get_block_index()[slot];
                int offset = meta.get_offset()[slot];
                blockIds[i][slot] = blockId;
                if (blockId >= 0 && blockId < position_map.length
                        && meta.getValid_bits()[offset] == (byte) 1) {
                    selected[i][slot] = true;
                    selectedCount++;
                }
            }
        }

        byte[] request = Ints.toByteArray(pathID);
        byte[] header = MessageUtility.createMessageHeaderBytes(
                MessageUtility.ORAM_READBLOCKS_PREFETCH, request.length);
        byte[] response = sendAndGetMessage(
                ByteBuffer.wrap(Bytes.concat(header, request)),
                MessageUtility.ORAM_READBLOCKS_PREFETCH);

        int expectedLength = selectedCount * Configs.BLOCK_DATA_LEN;
        if (response == null || response.length != expectedLength) {
            throw new IllegalStateException("Invalid prefetch response length: "
                    + (response == null ? -1 : response.length));
        }
        int responseOffset = 0;
        for (int i = 0; i < Configs.HEIGHT; i++) {
            for (int slot = 0; slot < Configs.REAL_BLOCK_COUNT; slot++) {
                if (!selected[i][slot]) {
                    continue;
                }
                int blockId = blockIds[i][slot];
                byte[] blockData = Arrays.copyOfRange(
                        response, responseOffset, responseOffset + Configs.BLOCK_DATA_LEN);
                stash.add(new Block(blockId, position_map[blockId], blockData));
                responseOffset += Configs.BLOCK_DATA_LEN;
            }
        }
        return metaList;
    }





    public void finalizeLshInitialPlacement() {
        for (int bucketId = Configs.BUCKET_COUNT - 1; bucketId >= 0 && stash.size() > 0; bucketId--) {
            write_bucket(bucketId);
        }
        if (stash.size() != 0) {
            throw new IllegalStateException(
                    "LSH initial placement failed: remaining stash blocks=" + stash.size());
        }
    }

    public BucketMetadata[] get_metadata(int pathID) {

        byte[] pos = Ints.toByteArray(pathID);
        byte[] header = MessageUtility.createMessageHeaderBytes(MessageUtility.ORAM_GETMETA, pos.length);
        ByteBuffer requestBuffer = ByteBuffer.wrap(Bytes.concat(header, pos));
        byte[] responseBytes = sendAndGetMessage(requestBuffer, MessageUtility.ORAM_GETMETA);

        BucketMetadata[] meta_list = new BucketMetadata[Configs.HEIGHT];
        int startIndex = 0;
        int index = 0;

        for (int pos_run = pathID; pos_run >= 0; pos_run = (pos_run - 1) >> 1) {
            byte[] meta_bytes = Arrays.copyOfRange(responseBytes, startIndex, startIndex + Configs.METADATA_BYTES_LEN);
            meta_list[index] = seria.metadataFromSerialize(meta_bytes);
            startIndex += Configs.METADATA_BYTES_LEN;
            index++;
            if (pos_run == 0) break;
        }
        return meta_list;
    }

    /** Reads one slot per tree level, using dummy slots outside the target block's level. */
    public void read_block(int pathID, int blockIndex, BucketMetadata[] meta_list) {

        boolean found = false;
        int[] read_offset = new int[Configs.HEIGHT];

        for (int i = 0, pos_run = pathID; pos_run >= 0; pos_run = (pos_run - 1) >> 1, i++) {
            if (found) {
                read_offset[i] = math.get_random_dummy(meta_list[i].getValid_bits(), meta_list[i].get_offset());
            } else {
                for (int j = 0; j < Configs.REAL_BLOCK_COUNT; j++) {
                    int offset = meta_list[i].get_offset()[j];
                    if ((meta_list[i].get_block_index()[j] == blockIndex) &&
                        (meta_list[i].getValid_bits()[offset] == 1)) {
                        read_offset[i] = offset;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    read_offset[i] = math.get_random_dummy(meta_list[i].getValid_bits(), meta_list[i].get_offset());
                }
            }
            if (pos_run == 0) break;
        }

        byte[][] read_offset_2d_bytes = new byte[Configs.HEIGHT][];
        for (int i = 0; i < Configs.HEIGHT; i++) {
            read_offset_2d_bytes[i] = Ints.toByteArray(read_offset[i]);
        }
        byte[] read_offset_bytes = read_offset_2d_bytes[0];
        for (int i = 1; i < Configs.HEIGHT; i++) {
            read_offset_bytes = Bytes.concat(read_offset_bytes, read_offset_2d_bytes[i]);
        }

        byte[] pos_bytes = Ints.toByteArray(pathID);
        byte[] requestBytes = Bytes.concat(pos_bytes, read_offset_bytes);
        byte[] header = MessageUtility.createMessageHeaderBytes(MessageUtility.ORAM_READBLOCK, requestBytes.length);
        ByteBuffer requestBuffer = ByteBuffer.wrap(Bytes.concat(header, requestBytes));


        byte[] responseBytes = sendAndGetMessage(requestBuffer, MessageUtility.ORAM_READBLOCK);
        if (found) {
            Block blk = new Block(blockIndex, pathID, responseBytes);
            stash.add(blk);
        }
    }

    @Override
    /** Reads a complete path into the stash and writes eligible blocks back to that path. */
    public void evict_path(int pathID) {

        for (int pos_run = pathID; pos_run >= 0; pos_run = (pos_run - 1) >> 1) {
            read_bucket(pos_run);
            if (pos_run == 0) break;
        }
        for (int pos_run = pathID; pos_run >= 0; pos_run = (pos_run - 1) >> 1) {
            write_bucket(pos_run);
            if (pos_run == 0) break;
        }
    }

    public void read_bucket(int bucket_id) {

        byte[] bucket_id_bytes = Ints.toByteArray(bucket_id);
        byte[] header = MessageUtility.createMessageHeaderBytes(MessageUtility.ORAM_READBUCKET, bucket_id_bytes.length);
        ByteBuffer requestBuffer = ByteBuffer.wrap(Bytes.concat(header, bucket_id_bytes));

        byte[] responseBytes = sendAndGetMessage(requestBuffer, MessageUtility.ORAM_READBUCKET);
        Bucket bucket = seria.bucketFromSerialize(responseBytes);

        BucketMetadata meta = bucket.getBucket_meta();
        int[] block_index = meta.get_block_index();
        int[] offset = meta.get_offset();
        byte[] valid_bits = meta.getValid_bits();

        for (int i = 0; i < Configs.REAL_BLOCK_COUNT; i++) {
            if ((block_index[i] >= 0) && (valid_bits[offset[i]] == (byte) 1)) {
                byte[] block_data = bucket.getBlock(offset[i]);
                stash.add(new Block(block_index[i], position_map[block_index[i]], block_data));
            }
        }
    }

    /**
     * Ages non-accessed stash blocks, batches expired candidates after a cache miss,
     * and falls back to a full path eviction only when the hard stash limit is exceeded.
     */
    private void delayed_batch_evict_after_access(int accessedBlockIndex, boolean cacheMiss) {

        for (Block block : stash.blocksSnapshot()) {
            if (block.getBlockIndex() != accessedBlockIndex) {
                block.decrementEvictCount();
            }
        }



        if (cacheMiss) {
            batch_single_evict(accessedBlockIndex);
        }

        if (stash.size() > Configs.HARD_STASH_LIMIT) {

            int fallbackPath = math.gen_reverse_lexicographic(evict_g, Configs.BUCKET_COUNT, Configs.HEIGHT);
            System.out.println("[DELAYED-EVICT-FALLBACK] stashSize=" + stash.size()
                    + " hardLimit=" + Configs.HARD_STASH_LIMIT
                    + " path=" + fallbackPath);
            evict_path(fallbackPath);
            fallbackEvictCount++;
            evict_g = (evict_g + 1) % Configs.LEAF_COUNT;
        }
    }

    /** Sends expired stash blocks in one request and removes only server-confirmed successes. */
    private void batch_single_evict(int accessedBlockIndex) {
        List<Block> candidates = new ArrayList<Block>();
        for (Block block : stash.blocksSnapshot()) {
            if (block.getBlockIndex() != accessedBlockIndex
                    && block.getEvictCount() <= 0
                    && block.getLeaf_id() >= Configs.LEAF_START
                    && block.getLeaf_id() < Configs.BUCKET_COUNT) {
                candidates.add(block);
            }
        }

        candidates.sort((left, right) -> {
            int countCompare = Integer.compare(left.getEvictCount(), right.getEvictCount());
            return countCompare != 0
                    ? countCompare
                    : Integer.compare(left.getBlockIndex(), right.getBlockIndex());
        });
        if (candidates.size() > Configs.MAX_BATCH_EVICT_BLOCKS_PER_ACCESS) {
            candidates = new ArrayList<Block>(candidates.subList(
                    0, Configs.MAX_BATCH_EVICT_BLOCKS_PER_ACCESS));
        }
        if (candidates.isEmpty()) {
            return;
        }

        byte[] request = Ints.toByteArray(candidates.size());
        for (Block candidate : candidates) {
            int clusterRoot = clusterRootForLeaf(candidate.getLeaf_id());
            request = Bytes.concat(
                    request,
                    Ints.toByteArray(candidate.getBlockIndex()),
                    Ints.toByteArray(candidate.getLeaf_id()),
                    Ints.toByteArray(clusterRoot),
                    candidate.getData());
        }
        byte[] header = MessageUtility.createMessageHeaderBytes(
                MessageUtility.ORAM_BATCH_SINGLE_EVICT, request.length);
        batchEvictAttempts++;
        byte[] response = sendAndGetMessage(
                ByteBuffer.wrap(Bytes.concat(header, request)),
                MessageUtility.ORAM_BATCH_SINGLE_EVICT);
        if (response == null || response.length != 12) {
            System.out.println("[BATCH-EVICT-FAIL] candidates=" + candidates.size()
                    + " stashSize=" + stash.size());
            return;
        }

        int successMask = Ints.fromByteArray(Arrays.copyOfRange(response, 0, 4));
        int bucketReads = Ints.fromByteArray(Arrays.copyOfRange(response, 4, 8));
        int bucketWrites = Ints.fromByteArray(Arrays.copyOfRange(response, 8, 12));
        int removed = 0;
        for (int i = 0; i < candidates.size(); i++) {
            if ((successMask & (1 << i)) != 0
                    && stash.remove_by_blockIndex(candidates.get(i).getBlockIndex())) {
                removed++;
            }
        }
        batchEvictSuccesses++;
        batchEvictedBlocks += removed;
        batchEvictBucketReads += bucketReads;
        batchEvictBucketWrites += bucketWrites;
        System.out.println("[BATCH-SINGLE-EVICT] candidates=" + candidates.size()
                + " blocks=" + removed
                + " bucketReads=" + bucketReads
                + " bucketWrites=" + bucketWrites
                + " stashSize=" + stash.size());
    }

    /** Returns the root bucket of the LSH prefix containing the given leaf. */
    private int clusterRootForLeaf(int leafId) {
        int clusterDepth = Math.max(0, Math.min(lshClusterPrefixBits, Configs.HEIGHT - 1));
        int bucketId = leafId;
        for (int depth = Configs.HEIGHT - 1; depth > clusterDepth; depth--) {
            bucketId = (bucketId - 1) >> 1;
        }
        return bucketId;
    }

    private Bucket read_bucket_object(int bucket_id) {
        byte[] bucket_id_bytes = Ints.toByteArray(bucket_id);
        byte[] header = MessageUtility.createMessageHeaderBytes(MessageUtility.ORAM_READBUCKET, bucket_id_bytes.length);
        ByteBuffer requestBuffer = ByteBuffer.wrap(Bytes.concat(header, bucket_id_bytes));
        byte[] responseBytes = sendAndGetMessage(requestBuffer, MessageUtility.ORAM_READBUCKET);
        if (responseBytes == null || responseBytes.length == 0) {
            return null;
        }
        return seria.bucketFromSerialize(responseBytes);
    }

    private boolean write_bucket_object(Bucket bucket) {
        byte[] bucket_bytes = seria.bucketSerialize(bucket);
        byte[] header = MessageUtility.createMessageHeaderBytes(MessageUtility.ORAM_WRITEBUCKET, bucket_bytes.length);
        ByteBuffer requestBuffer = ByteBuffer.wrap(Bytes.concat(header, bucket_bytes));
        byte[] response = sendAndGetMessage(requestBuffer, MessageUtility.ORAM_WRITEBUCKET);
        return response != null && response.length > 0 && response[0] == (byte) 1;
    }

    private void write_block_to_bucket_data(byte[] bucket_data, int offset, byte[] block_data) {
        int offset_i = offset * Configs.BLOCK_DATA_LEN;
        Arrays.fill(bucket_data, offset_i, offset_i + Configs.BLOCK_DATA_LEN, (byte) 0);
        if (block_data != null) {
            System.arraycopy(block_data, 0, bucket_data, offset_i,
                    Math.min(block_data.length, Configs.BLOCK_DATA_LEN));
        }
    }

    private void refresh_exhausted_buckets(
            int pathID, BucketMetadata[] meta_list, boolean metadataCapturedBeforeRead) {


        for (int pos_run = pathID, i = 0; pos_run >= 0; pos_run = (pos_run - 1) >> 1, i++) {
            int effectiveReadCounter = meta_list[i].getRead_counter()
                    + (metadataCapturedBeforeRead ? 1 : 0);
            if (effectiveReadCounter >= (Configs.DUMMY_BLOCK_COUNT - 2)) {
                System.out.println("[BUCKET-REFRESH] pos=" + pos_run);
                bucketRefreshAttempts++;
                if (refresh_bucket_preserving_valid_blocks(pos_run)) {
                    bucketRefreshSuccesses++;
                }
            }
            if (pos_run == 0) {
                break;
            }
        }
    }

    private boolean refresh_bucket_preserving_valid_blocks(int bucket_id) {
        Bucket oldBucket = read_bucket_object(bucket_id);
        if (oldBucket == null) {
            return false;
        }

        BucketMetadata oldMeta = oldBucket.getBucket_meta();
        int[] oldBlockIndex = oldMeta.get_block_index();
        int[] oldOffset = oldMeta.get_offset();
        byte[] oldValidBits = oldMeta.getValid_bits();

        BucketMetadata newMeta = new BucketMetadata();
        newMeta.init_block_index();
        newMeta.set_offset(math.get_random_permutation(Configs.Z));
        int[] newOffset = newMeta.get_offset();
        byte[] newBucketData = new byte[Configs.Z * Configs.BLOCK_DATA_LEN];

        int count = 0;
        for (int i = 0; i < Configs.REAL_BLOCK_COUNT; i++) {
            int oldOffset_i = oldOffset[i];
            if (oldBlockIndex[i] >= 0 && oldValidBits[oldOffset_i] == (byte) 1) {
                int newOffset_i = newOffset[count];
                write_block_to_bucket_data(newBucketData, newOffset_i, oldBucket.getBlock(oldOffset_i));
                newMeta.set_blockIndex_bit(count, oldBlockIndex[i]);
                count++;
            }
        }

        Bucket refreshed = new Bucket(bucket_id, newBucketData, newMeta);
        return write_bucket_object(refreshed);
    }

    public void write_bucket(int bucket_id) {

        BucketMetadata meta = new BucketMetadata();
        Block[] block_list = new Block[Configs.REAL_BLOCK_COUNT];

        int count = stash.remove_by_bucket(bucket_id, Configs.REAL_BLOCK_COUNT, block_list);


        meta.set_offset(math.get_random_permutation(Configs.Z));
        int[] offset = meta.get_offset();

        byte[] bucket_data = new byte[Configs.Z * Configs.BLOCK_DATA_LEN];
        for (int i = 0; i < count; i++) {
            int offset_i = offset[i] * Configs.BLOCK_DATA_LEN;
            byte[] block_data = block_list[i].getData();
            for (int j = 0; j < Configs.BLOCK_DATA_LEN; j++) {
                bucket_data[offset_i + j] = block_data[j];
            }
            meta.set_blockIndex_bit(i, block_list[i].getBlockIndex());
        }

        for (int i = count; i < Configs.Z; i++) {
            int offset_i = offset[i] * Configs.BLOCK_DATA_LEN;
            Arrays.fill(bucket_data, offset_i, offset_i + Configs.BLOCK_DATA_LEN, (byte) 0);
            if (i < Configs.REAL_BLOCK_COUNT) {
                meta.set_blockIndex_bit(i, -1);
            }
        }

        Bucket bucket = new Bucket(bucket_id, bucket_data, meta);
        byte[] bucket_bytes = seria.bucketSerialize(bucket);
        byte[] header = MessageUtility.createMessageHeaderBytes(MessageUtility.ORAM_WRITEBUCKET, bucket_bytes.length);
        ByteBuffer requestBuffer = ByteBuffer.wrap(Bytes.concat(header, bucket_bytes));

        sendAndGetMessage(requestBuffer, MessageUtility.ORAM_WRITEBUCKET);
    }

    @Override
    public void early_reshuffle(int pathID, BucketMetadata[] meta_list) {

        for (int pos_run = pathID, i = 0; pos_run >= 0; pos_run = (pos_run - 1) >> 1, i++) {
            if (meta_list[i].getRead_counter() >= (Configs.DUMMY_BLOCK_COUNT - 2)) {
                System.out.println("early reshuffle in pos " + pos_run);
                baselineEarlyReshuffleBucketCount++;
                read_bucket(pos_run);
                write_bucket(pos_run);
            }
            if (pos_run == 0) break;
        }
    }

    @SuppressWarnings("rawtypes")
    public byte[] sendAndGetMessage(ByteBuffer requestBuffer, int messageType) {

        byte[] responseBytes = null;
        if (messageType >= 0 && messageType < requestCounts.length) {
            requestCounts[messageType]++;
        }
        try {
            writeFully(mChannel, requestBuffer);

            ByteBuffer typeAndSize = ByteBuffer.allocate(8);
            readFully(mChannel, typeAndSize);
            typeAndSize.flip();

            int[] typeAndSizeInt = MessageUtility.parseTypeAndLength(typeAndSize);
            int type = typeAndSizeInt[0];
            int size = typeAndSizeInt[1];

            ByteBuffer responseBuffer = ByteBuffer.allocate(size);
            readFully(mChannel, responseBuffer);
            responseBuffer.flip();

            if (type == messageType) {
                responseBytes = new byte[size];
                responseBuffer.get(responseBytes);
            } else {
                System.out.println("client get wrong when receive response from server!");
            }
        } catch (Exception e) {
            try {
                mChannel.close();
            } catch (IOException e1) {}
        }
        return responseBytes;
    }


    private static void readFully(AsynchronousSocketChannel channel, ByteBuffer buffer)
            throws InterruptedException, ExecutionException, EOFException {
        while (buffer.hasRemaining()) {
            Integer bytesRead = channel.read(buffer).get();
            if (bytesRead == null || bytesRead < 0) {
                throw new EOFException("Server closed the connection while receiving a response");
            }
        }
    }

    private static void writeFully(AsynchronousSocketChannel channel, ByteBuffer buffer)
            throws InterruptedException, ExecutionException, EOFException {
        while (buffer.hasRemaining()) {
            Integer bytesWritten = channel.write(buffer).get();
            if (bytesWritten == null || bytesWritten < 0) {
                throw new EOFException("Server closed the connection while sending a request");
            }
        }
    }








    public static byte[] loadBinAsBlockData(String path) throws IOException {
        byte[] out = new byte[Configs.BLOCK_DATA_LEN];
        Arrays.fill(out, (byte) 0);

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)))) {


            byte[] tmp = in.readAllBytes();
            int n = Math.min(tmp.length, out.length);
            System.arraycopy(tmp, 0, out, 0, n);
        }
        return out;
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static double medianMillis(long[] sortedNanos) {
        int n = sortedNanos.length;
        if ((n & 1) == 1) {
            return nanosToMillis(sortedNanos[n / 2]);
        }
        return nanosToMillis(sortedNanos[n / 2 - 1] + sortedNanos[n / 2]) / 2.0;
    }

    private static double percentileMillis(long[] sortedNanos, double percentile) {
        int index = (int) Math.ceil(percentile * sortedNanos.length) - 1;
        index = Math.max(0, Math.min(index, sortedNanos.length - 1));
        return nanosToMillis(sortedNanos[index]);
    }

    private static int[][] createContiguousAccessGroups(int blockCount, int groupCount) {
        if (blockCount <= 0 || groupCount <= 0 || blockCount % groupCount != 0) {
            throw new IllegalArgumentException(
                    "blockCount must be positive and divisible by groupCount");
        }
        int blocksPerGroup = blockCount / groupCount;
        int[][] groups = new int[groupCount][blocksPerGroup];
        for (int group = 0; group < groupCount; group++) {
            for (int offset = 0; offset < blocksPerGroup; offset++) {
                groups[group][offset] = group * blocksPerGroup + offset;
            }
        }
        return groups;
    }








    public static void main(String[] args) {
        boolean useLsh = true;
        boolean useCache = true;
        String experimentMode = "LSH_CACHE";
        if (args.length > 0) {
            String mode = args[0].trim().toLowerCase();
            switch (mode) {
                case "baseline-cache":
                case "baseline":
                case "normal":
                case "random":
                    useLsh = false;
                    useCache = true;
                    experimentMode = "BASELINE_CACHE";
                    break;
                case "baseline-no-cache":
                    useLsh = false;
                    useCache = false;
                    experimentMode = "BASELINE_NO_CACHE";
                    break;
                case "lsh-cache":
                case "lsh":
                    useLsh = true;
                    useCache = true;
                    experimentMode = "LSH_CACHE";
                    break;
                case "lsh-no-cache":
                    useLsh = true;
                    useCache = false;
                    experimentMode = "LSH_NO_CACHE";
                    break;
                default:
                    System.out.println("[ERROR] unknown mode: " + args[0]);
                    System.out.println("Usage: ./run_client.sh "
                            + "{baseline-cache|baseline-no-cache|lsh-cache|lsh-no-cache} "
                            + "[accessSeed] [transitionProbability]");
                    return;
            }
        }



        long accessSeed;
        if (args.length > 1) {
            try {
                accessSeed = Long.parseLong(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("[ERROR] accessSeed must be a signed 64-bit integer: " + args[1]);
                System.out.println("Usage: ./run_client.sh "
                        + "{baseline-cache|baseline-no-cache|lsh-cache|lsh-no-cache} "
                        + "[accessSeed] [transitionProbability]");
                return;
            }
        } else {
            accessSeed = new SecureRandom().nextLong();
        }

        int nBlocks = Configs.BLOCK_COUNT;
        String dataDir = "data";
        int readIterations = 300;
        int currentGroup = 0;
        double transitionProbability = 0.20;
        if (args.length > 2) {
            try {
                transitionProbability = Double.parseDouble(args[2]);
            } catch (NumberFormatException e) {
                System.out.println("[ERROR] transitionProbability must be a number from 0.0 to 1.0: "
                        + args[2]);
                return;
            }
            if (!Double.isFinite(transitionProbability)
                    || transitionProbability < 0.0 || transitionProbability > 1.0) {
                System.out.println("[ERROR] transitionProbability must be from 0.0 to 1.0: " + args[2]);
                return;
            }
        }
        int[][] accessGroups = createContiguousAccessGroups(nBlocks, 3);
        int[] groupReadCount = new int[accessGroups.length];
        int[] groupStashHitCount = new int[accessGroups.length];
        int transitionCount = 0;
        long[] readLatenciesNs = new long[readIterations];
        long[] hitLatenciesNs = new long[readIterations];
        long[] missLatenciesNs = new long[readIterations];
        int measuredHitCount = 0;
        int measuredMissCount = 0;


        PrintStream resultOut = System.out;
        PrintStream silentOut = new PrintStream(OutputStream.nullOutputStream());
        System.setOut(silentOut);

        Random rand = new Random(accessSeed);
        long oramSeed = deriveOramSeed(accessSeed);
        Client client = new Client(useLsh, useCache, oramSeed);
        client.initServer();
        if (useLsh) {
            client.buildLshLeafClustersAuto(dataDir, nBlocks);
        }
        System.out.println("[EXPERIMENT] mode=" + experimentMode
                + " lsh=" + useLsh
                + " cache=" + useCache
                + " readIterations=" + readIterations
                + " transitionProbability=" + transitionProbability
                + " accessSeed=" + accessSeed
                + " oramSeed=" + oramSeed);


        for (int i = 0; i < nBlocks; i++) {
            String path = dataDir + "/" + i + ".bin";
            try {
                byte[] blk = loadBinAsBlockData(path);
                client.oblivious_access(i, OPERATION.ORAM_ACCESS_WRITE, blk);

            } catch (Exception e) {
                System.err.println("[WRITE] failed to load: " + path);
                e.printStackTrace();
            }
        }




        if (useLsh) {
            client.finalizeLshInitialPlacement();
        }
        client.resetExperimentState();


        System.out.println("[GROUP-START] group=" + currentGroup
                + " blocks=" + Arrays.toString(accessGroups[currentGroup]));

        for (int t = 0; t < readIterations; t++) {
            if (rand.nextDouble() < transitionProbability) {
                int oldGroup = currentGroup;
                do {
                    currentGroup = rand.nextInt(accessGroups.length);
                } while (currentGroup == oldGroup);

                System.out.println("[GROUP-TRANSITION] from=" + oldGroup
                        + " to=" + currentGroup
                        + " blocks=" + Arrays.toString(accessGroups[currentGroup]));
                transitionCount++;
            }

            int[] group = accessGroups[currentGroup];
            int blockId = group[rand.nextInt(group.length)];
            byte[] dummy = new byte[Configs.BLOCK_DATA_LEN];


            long readStartNs = System.nanoTime();
            byte[] data = client.oblivious_access(blockId, OPERATION.ORAM_ACCESS_READ, dummy);
            long readLatencyNs = System.nanoTime() - readStartNs;
            readLatenciesNs[t] = readLatencyNs;

            groupReadCount[currentGroup]++;
            if (client.wasLastAccessStashHit()) {
                groupStashHitCount[currentGroup]++;
                hitLatenciesNs[measuredHitCount++] = readLatencyNs;
            } else {
                missLatenciesNs[measuredMissCount++] = readLatencyNs;
            }

            if (data == null) {
                System.err.println("[READ-ERROR] iter=" + t + " block=" + blockId + " data=null");
            }
        }

        client.close();
        silentOut.close();
        System.setOut(resultOut);

        long totalReadNs = 0L;
        for (long latency : readLatenciesNs) totalReadNs += latency;
        long[] sortedReadLatenciesNs = Arrays.copyOf(readLatenciesNs, readLatenciesNs.length);
        Arrays.sort(sortedReadLatenciesNs);


        double totalReadMs = nanosToMillis(totalReadNs);
        double meanReadMs = totalReadMs / readIterations;
        double medianReadMs = medianMillis(sortedReadLatenciesNs);
        double p95ReadMs = percentileMillis(sortedReadLatenciesNs, 0.95);
        double p99ReadMs = percentileMillis(sortedReadLatenciesNs, 0.99);
        double stashHitRate = client.getReadCount() == 0 ? 0.0
                : (double) client.getStashHitCount() / client.getReadCount();

        long hitTotalNs = 0L;
        for (int i = 0; i < measuredHitCount; i++) hitTotalNs += hitLatenciesNs[i];
        double hitMeanMs = measuredHitCount == 0 ? 0.0
                : nanosToMillis(hitTotalNs) / measuredHitCount;
        long missTotalNs = 0L;
        for (int i = 0; i < measuredMissCount; i++) missTotalNs += missLatenciesNs[i];
        double missMeanMs = measuredMissCount == 0 ? 0.0
                : nanosToMillis(missTotalNs) / measuredMissCount;

        long measuredPhaseNs;
        if (useLsh) {
            measuredPhaseNs = client.getPrefetchReadNanos()
                    + client.getLeafAssignmentNanos()
                    + client.getDelayedEvictNanos()
                    + client.getRefreshCheckNanos();
        } else {
            measuredPhaseNs = client.getBaselineReadPathNanos()
                    + client.getBaselinePathEvictNanos()
                    + client.getBaselineEarlyReshuffleNanos();
        }
        long otherNs = Math.max(0L, totalReadNs - measuredPhaseNs);

        int getMetaRequests = client.getRequestCount(MessageUtility.ORAM_GETMETA);
        int readBlockRequests = client.getRequestCount(MessageUtility.ORAM_READBLOCK);
        int prefetchReadRequests = client.getRequestCount(MessageUtility.ORAM_READBLOCKS_PREFETCH);
        int readBucketRequests = client.getRequestCount(MessageUtility.ORAM_READBUCKET);
        int writeBucketRequests = client.getRequestCount(MessageUtility.ORAM_WRITEBUCKET);
        int batchSingleEvictRequests = client.getRequestCount(MessageUtility.ORAM_BATCH_SINGLE_EVICT);
        int bucketFileReads = 0;
        int bucketFileWrites = 0;

        bucketFileReads = getMetaRequests * Configs.HEIGHT
                + readBlockRequests * Configs.HEIGHT
                + prefetchReadRequests * Configs.HEIGHT
                + readBucketRequests
                + client.getBatchEvictBucketReads();
        bucketFileWrites = readBlockRequests * Configs.HEIGHT
                + prefetchReadRequests * Configs.HEIGHT
                + writeBucketRequests
                + client.getBatchEvictBucketWrites();

        double[] groupHitRates = new double[accessGroups.length];
        for (int groupId = 0; groupId < accessGroups.length; groupId++) {
            groupHitRates[groupId] = groupReadCount[groupId] == 0 ? 0.0
                    : (double) groupStashHitCount[groupId] / groupReadCount[groupId];
        }

        int[] retainedRequestCounts = {
                getMetaRequests,
                readBlockRequests,
                prefetchReadRequests,
                readBucketRequests,
                writeBucketRequests,
                batchSingleEvictRequests
        };
        lastExperimentMetrics = new ExperimentMetrics(
                experimentMode, useLsh, useCache, accessSeed, oramSeed, transitionCount,
                client.getReadCount(), client.getStashHitCount(), client.getServerReadCount(),
                totalReadMs, meanReadMs, medianReadMs, p95ReadMs, p99ReadMs, stashHitRate,
                hitMeanMs, missMeanMs, measuredPhaseNs, otherNs, retainedRequestCounts,
                bucketFileReads, bucketFileWrites, groupReadCount, groupStashHitCount, groupHitRates);


        resultOut.println("[LATENCY] totalMs="
                + String.format(java.util.Locale.ROOT, "%.3f", totalReadMs));
        resultOut.println("[SUMMARY] stashHitRate="
                + String.format(java.util.Locale.ROOT, "%.4f", stashHitRate));


        if (Boolean.getBoolean("roost.diagnostics")) {
            resultOut.println("[DIAGNOSTIC] mode=" + experimentMode
                    + " accessSeed=" + accessSeed
                    + " oramSeed=" + oramSeed
                    + " transitions=" + transitionCount
                    + " reads=" + client.getReadCount()
                    + " hits=" + client.getStashHitCount()
                    + " serverReads=" + client.getServerReadCount());
            resultOut.println("[DIAGNOSTIC-PHASE] readMs="
                    + String.format(java.util.Locale.ROOT, "%.3f", totalReadMs)
                    + " primaryMs="
                    + String.format(java.util.Locale.ROOT, "%.3f", nanosToMillis(
                            useLsh ? client.getPrefetchReadNanos() : client.getBaselineReadPathNanos()))
                    + " evictMs="
                    + String.format(java.util.Locale.ROOT, "%.3f", nanosToMillis(
                            useLsh ? client.getDelayedEvictNanos() : client.getBaselinePathEvictNanos()))
                    + " maintenanceMs="
                    + String.format(java.util.Locale.ROOT, "%.3f", nanosToMillis(
                            useLsh ? client.getRefreshCheckNanos() : client.getBaselineEarlyReshuffleNanos()))
                    + " assignmentMs="
                    + String.format(java.util.Locale.ROOT, "%.3f", nanosToMillis(
                            useLsh ? client.getLeafAssignmentNanos() : 0L))
                    + " otherMs="
                    + String.format(java.util.Locale.ROOT, "%.3f", nanosToMillis(otherNs)));
            resultOut.println("[DIAGNOSTIC-MAINTENANCE] baselinePathEvicts="
                    + client.getBaselinePathEvictCount()
                    + " baselineReshuffleBuckets=" + client.getBaselineEarlyReshuffleBucketCount()
                    + " batchEvictAttempts=" + client.getBatchEvictAttempts()
                    + " batchEvictSuccesses=" + client.getBatchEvictSuccesses()
                    + " batchEvictedBlocks=" + client.getBatchEvictedBlocks()
                    + " batchEvictBucketReads=" + client.getBatchEvictBucketReads()
                    + " batchEvictBucketWrites=" + client.getBatchEvictBucketWrites()
                    + " refreshAttempts=" + client.getBucketRefreshAttempts()
                    + " refreshSuccesses=" + client.getBucketRefreshSuccesses()
                    + " fallbackEvicts=" + client.getFallbackEvictCount());
            resultOut.println("[DIAGNOSTIC-REQUESTS] getMeta=" + getMetaRequests
                    + " readBlock=" + readBlockRequests
                    + " prefetchRead=" + prefetchReadRequests
                    + " readBucket=" + readBucketRequests
                    + " writeBucket=" + writeBucketRequests
                    + " batchSingleEvict=" + batchSingleEvictRequests
                    + " estimatedFileReads=" + bucketFileReads
                    + " estimatedFileWrites=" + bucketFileWrites);
        }
    }
}
