package com.example;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;

import com.ringoram.Configs;

/**
 * ORAMの動作を変更せず、現在のLSH hashクラスタリングだけを評価するツール。
 * 正解カテゴリはクラスタリング完了後の指標計算にのみ使用する。
 */
public final class LshClassificationEvaluator {
    private static final int CATEGORY_COUNT = 3;
    private static final int BLOCK_COUNT = Configs.BLOCK_COUNT;
    private static final int BLOCKS_PER_CATEGORY = BLOCK_COUNT / CATEGORY_COUNT;
    private static final int VECTOR_DIMENSION = 348;
    private static final int HASH_BITS = 256;
    private static final int MAX_CLUSTERS = Configs.LEAF_COUNT;
    private static final long LSH_SEED = 42L;

    private static final class ClusterResult {
        final String[] medoids;
        final int[] assignment;

        ClusterResult(String[] medoids, int[] assignment) {
            this.medoids = medoids;
            this.assignment = assignment;
        }
    }

    private LshClassificationEvaluator() {}

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args.length > 0 ? args[0] : "data");
        float[][] vectors = new float[BLOCK_COUNT][];
        String[] hashes = new String[BLOCK_COUNT];
        LSH lsh = new LSH(HASH_BITS, VECTOR_DIMENSION, LSH_SEED);

        for (int block = 0; block < BLOCK_COUNT; block++) {
            vectors[block] = readVector(dataDir.resolve(block + ".bin"));
            hashes[block] = lsh.computeHash(vectors[block]);
        }

        ClusterResult selected = null;
        double selectedScore = Double.NEGATIVE_INFINITY;
        System.out.println("[EVAL-CONDITION] blocks=" + BLOCK_COUNT
                + " categories=" + CATEGORY_COUNT
                + " hashBits=" + HASH_BITS
                + " lshSeed=" + LSH_SEED
                + " candidateClusters=2-" + MAX_CLUSTERS);
        for (int k = 2; k <= MAX_CLUSTERS; k++) {
            ClusterResult candidate = clusterHashes(hashes, k);
            double score = silhouetteScore(hashes, candidate);
            System.out.printf(Locale.ROOT, "[EVAL-SILHOUETTE] k=%d score=%.6f%n", k, score);
            if (score > selectedScore) {
                selected = candidate;
                selectedScore = score;
            }
        }

        if (selected == null) throw new IllegalStateException("No clustering result");
        int[][] contingency = contingency(selected.assignment, selected.medoids.length);
        System.out.printf(Locale.ROOT, "[EVAL-SELECTED] clusters=%d silhouette=%.6f%n",
                selected.medoids.length, selectedScore);
        for (int cluster = 0; cluster < selected.medoids.length; cluster++) {
            StringBuilder blocks = new StringBuilder();
            for (int block = 0; block < BLOCK_COUNT; block++) {
                if (selected.assignment[block] == cluster) {
                    if (blocks.length() > 0) blocks.append(',');
                    blocks.append(block);
                }
            }
            System.out.println("[EVAL-CLUSTER] cluster=" + cluster
                    + " leaf=" + (Configs.LEAF_START + cluster)
                    + " categoryCounts=" + Arrays.toString(contingency[cluster])
                    + " blocks=[" + blocks + "]");
        }

        double purity = purity(contingency);
        double ari = adjustedRandIndex(contingency);
        double nmi = normalizedMutualInformation(contingency);
        DistanceSummary hashDistance = summarizeHashDistances(hashes);
        DistanceSummary cosineDistance = summarizeCosineSimilarities(vectors);

        System.out.printf(Locale.ROOT,
                "[EVAL-METRIC] purity=%.6f ARI=%.6f NMI=%.6f%n", purity, ari, nmi);
        System.out.printf(Locale.ROOT,
                "[EVAL-HAMMING] withinMean=%.6f betweenMean=%.6f separation=%.6f%n",
                hashDistance.within, hashDistance.between,
                hashDistance.between - hashDistance.within);
        System.out.printf(Locale.ROOT,
                "[EVAL-COSINE] withinMean=%.6f betweenMean=%.6f separation=%.6f%n",
                cosineDistance.within, cosineDistance.between,
                cosineDistance.within - cosineDistance.between);
    }

    private static float[] readVector(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        int expected = VECTOR_DIMENSION * Float.BYTES;
        if (bytes.length != expected) {
            throw new IOException(path + " has " + bytes.length + " bytes; expected " + expected);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        float[] vector = new float[VECTOR_DIMENSION];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = buffer.getFloat();
            if (!Float.isFinite(vector[i])) {
                throw new IOException(path + " contains a non-finite value at index " + i);
            }
        }
        return vector;
    }

    // Client.clusterHashesと同じ初期化・更新規則を使用する。
    private static ClusterResult clusterHashes(String[] hashes, int clusterCount) {
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
        for (int iteration = 0; iteration < 20; iteration++) {
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
        return new ClusterResult(medoids, assignment);
    }

    // Client.silhouetteScoreと同じ規則を使用する。
    private static double silhouetteScore(String[] hashes, ClusterResult result) {
        double totalScore = 0.0;
        for (int i = 0; i < hashes.length; i++) {
            int ownCluster = result.assignment[i];
            int ownCount = 0;
            double ownDistance = 0.0;
            for (int j = 0; j < hashes.length; j++) {
                if (i != j && result.assignment[j] == ownCluster) {
                    ownDistance += hammingDistance(hashes[i], hashes[j]);
                    ownCount++;
                }
            }
            if (ownCount == 0) continue;
            double a = ownDistance / ownCount;
            double b = Double.POSITIVE_INFINITY;
            for (int c = 0; c < result.medoids.length; c++) {
                if (c == ownCluster) continue;
                int otherCount = 0;
                double otherDistance = 0.0;
                for (int j = 0; j < hashes.length; j++) {
                    if (result.assignment[j] == c) {
                        otherDistance += hammingDistance(hashes[i], hashes[j]);
                        otherCount++;
                    }
                }
                if (otherCount > 0) b = Math.min(b, otherDistance / otherCount);
            }
            if (Double.isFinite(b) && Math.max(a, b) > 0.0) {
                totalScore += (b - a) / Math.max(a, b);
            }
        }
        return totalScore / hashes.length;
    }

    private static int[][] contingency(int[] assignment, int clusterCount) {
        int[][] table = new int[clusterCount][CATEGORY_COUNT];
        for (int block = 0; block < assignment.length; block++) {
            table[assignment[block]][block / BLOCKS_PER_CATEGORY]++;
        }
        return table;
    }

    private static double purity(int[][] table) {
        int correct = 0;
        for (int[] row : table) correct += Arrays.stream(row).max().orElse(0);
        return (double) correct / BLOCK_COUNT;
    }

    private static double adjustedRandIndex(int[][] table) {
        double sumCells = 0.0;
        double sumRows = 0.0;
        double[] columns = new double[CATEGORY_COUNT];
        for (int[] row : table) {
            int rowTotal = 0;
            for (int c = 0; c < CATEGORY_COUNT; c++) {
                sumCells += combinations2(row[c]);
                rowTotal += row[c];
                columns[c] += row[c];
            }
            sumRows += combinations2(rowTotal);
        }
        double sumColumns = 0.0;
        for (double column : columns) sumColumns += combinations2(column);
        double totalPairs = combinations2(BLOCK_COUNT);
        double expected = sumRows * sumColumns / totalPairs;
        double maximum = 0.5 * (sumRows + sumColumns);
        return maximum == expected ? 1.0 : (sumCells - expected) / (maximum - expected);
    }

    private static double normalizedMutualInformation(int[][] table) {
        double[] rowTotals = new double[table.length];
        double[] columnTotals = new double[CATEGORY_COUNT];
        for (int r = 0; r < table.length; r++) {
            for (int c = 0; c < CATEGORY_COUNT; c++) {
                rowTotals[r] += table[r][c];
                columnTotals[c] += table[r][c];
            }
        }
        double mutualInformation = 0.0;
        for (int r = 0; r < table.length; r++) {
            for (int c = 0; c < CATEGORY_COUNT; c++) {
                if (table[r][c] == 0) continue;
                double p = table[r][c] / (double) BLOCK_COUNT;
                mutualInformation += p * Math.log((table[r][c] * (double) BLOCK_COUNT)
                        / (rowTotals[r] * columnTotals[c]));
            }
        }
        double rowEntropy = entropy(rowTotals);
        double columnEntropy = entropy(columnTotals);
        return rowEntropy == 0.0 || columnEntropy == 0.0
                ? 0.0 : mutualInformation / Math.sqrt(rowEntropy * columnEntropy);
    }

    private static double entropy(double[] counts) {
        double result = 0.0;
        for (double count : counts) {
            if (count == 0.0) continue;
            double probability = count / BLOCK_COUNT;
            result -= probability * Math.log(probability);
        }
        return result;
    }

    private static final class DistanceSummary {
        final double within;
        final double between;

        DistanceSummary(double within, double between) {
            this.within = within;
            this.between = between;
        }
    }

    private static DistanceSummary summarizeHashDistances(String[] hashes) {
        double within = 0.0;
        double between = 0.0;
        int withinCount = 0;
        int betweenCount = 0;
        for (int i = 0; i < hashes.length; i++) {
            for (int j = i + 1; j < hashes.length; j++) {
                double normalized = hammingDistance(hashes[i], hashes[j]) / (double) HASH_BITS;
                if (category(i) == category(j)) {
                    within += normalized;
                    withinCount++;
                } else {
                    between += normalized;
                    betweenCount++;
                }
            }
        }
        return new DistanceSummary(within / withinCount, between / betweenCount);
    }

    private static DistanceSummary summarizeCosineSimilarities(float[][] vectors) {
        double within = 0.0;
        double between = 0.0;
        int withinCount = 0;
        int betweenCount = 0;
        for (int i = 0; i < vectors.length; i++) {
            for (int j = i + 1; j < vectors.length; j++) {
                double similarity = cosineSimilarity(vectors[i], vectors[j]);
                if (category(i) == category(j)) {
                    within += similarity;
                    withinCount++;
                } else {
                    between += similarity;
                    betweenCount++;
                }
            }
        }
        return new DistanceSummary(within / withinCount, between / betweenCount);
    }

    private static int category(int block) {
        return block / BLOCKS_PER_CATEGORY;
    }

    private static int hammingDistance(String first, String second) {
        int distance = 0;
        for (int i = 0; i < first.length(); i++) {
            if (first.charAt(i) != second.charAt(i)) distance++;
        }
        return distance;
    }

    private static double cosineSimilarity(float[] first, float[] second) {
        double dot = 0.0;
        double firstNorm = 0.0;
        double secondNorm = 0.0;
        for (int i = 0; i < first.length; i++) {
            dot += (double) first[i] * second[i];
            firstNorm += (double) first[i] * first[i];
            secondNorm += (double) second[i] * second[i];
        }
        return dot / Math.sqrt(firstNorm * secondNorm);
    }

    private static double combinations2(double value) {
        return value * (value - 1.0) / 2.0;
    }
}
