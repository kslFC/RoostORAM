package com.example;

import java.util.Random;

/**
 * Random Hyperplane LSH (SimHash系)
 * - 入力: float[] (dimension)
 * - 出力: hash bit列 (numHashBits)
 * - 依存ライブラリなし
 *
 * 前提:
 * - ベクトル次元は固定 (例: 348)
 * - numHashBits は 64 以上推奨
 * - seed を固定すれば再現性あり
 */
public final class LSH {

    private final int numHashBits;
    private final int dimension;
    // 各ビットに対応する固定ランダム超平面。seed を固定すると実験ごとに同じハッシュになる。
    private final float[][] randomHyperplanes; // [numHashBits][dimension]

    /**
     * @param numHashBits 生成するハッシュのビット長（例: 32）
     * @param dimension   入力ベクトル次元（例: 348）
     * @param seed        乱数シード（再現性のため固定推奨: 42L など）
     */
    public LSH(int numHashBits, int dimension, long seed) {
        if (numHashBits <= 0) throw new IllegalArgumentException("numHashBits must be > 0");
        if (dimension <= 0) throw new IllegalArgumentException("dimension must be > 0");

        this.numHashBits = numHashBits;
        this.dimension = dimension;
        this.randomHyperplanes = new float[numHashBits][dimension];

        Random rng = new Random(seed);

        // ランダム超平面（法線ベクトル）を N(0,1) で生成する。
        // あるベクトルが超平面のどちら側にあるかを、ハッシュの1ビットとして使う。
        for (int i = 0; i < numHashBits; i++) {
            for (int d = 0; d < dimension; d++) {
                randomHyperplanes[i][d] = (float) rng.nextGaussian();
            }
        }
    }

    /**
     * float[] ベクトルから LSH ハッシュ（ビット列）を生成する。
     *
     * @param vector 入力ベクトル（dimension と同じ長さ）
     * @return ハッシュビット列（例: "101010..."）長さ = numHashBits
     */
    public String computeHash(float[] vector) {
        final long[] bits = computeHashBits(vector);
        return bitsToString(bits, numHashBits);
    }

    /**
     * 高速版: ハッシュを long 配列（ビット列）で返す。
     * bits[0] の bit0 が hashの0番目、bit1 が1番目…という扱い（LSB側から詰める）
     *
     * @param vector 入力ベクトル
     * @return long[] ビット列
     */
    public long[] computeHashBits(float[] vector) {
        if (vector == null) throw new IllegalArgumentException("vector is null");
        if (vector.length != dimension) {
            throw new IllegalArgumentException("vector length " + vector.length + " != dimension " + dimension);
        }

        int words = (numHashBits + 63) >>> 6; // ceil(numHashBits/64)
        long[] out = new long[words];
        boolean hasSignal = hasFiniteNonZeroValue(vector);

        for (int i = 0; i < numHashBits; i++) {
            // dot > 0 なら超平面の正側、dot < 0 なら負側にあるとみなす。
            double dot = 0.0;
            float[] hyperplane = randomHyperplanes[i];

            for (int d = 0; d < dimension; d++) {
                float v = vector[d];
                // NaN/Inf 対策（壊れた値が来てもLSHが死なないように）
                if (!Float.isFinite(v)) v = 0f;
                dot += (double) v * (double) hyperplane[d];
            }

            // dot == 0 は境界上なので、固定的な tie-break でビットを決めて再現性を保つ。
            if (dot > 0.0 || (dot == 0.0 && tieBit(i, hasSignal))) {
                int word = i >>> 6;          // i / 64
                int bit = i & 63;            // i % 64
                out[word] |= (1L << bit);
            }
        }

        return out;
    }

    /**
     * ハッシュ同士のハミング距離（bit差分数）を計算する（long[]版）
     */
    public static int hammingDistance(long[] a, long[] b) {
        if (a == null || b == null) throw new IllegalArgumentException("hash is null");
        if (a.length != b.length) throw new IllegalArgumentException("hash length mismatch");

        int dist = 0;
        for (int i = 0; i < a.length; i++) {
            dist += Long.bitCount(a[i] ^ b[i]);
        }
        return dist;
    }

    /**
     * ハミング距離（String版）
     */
    public static int hammingDistance(String hash1, String hash2) {
        if (hash1 == null || hash2 == null) throw new IllegalArgumentException("hash is null");
        if (hash1.length() != hash2.length()) throw new IllegalArgumentException("Hash lengths must match.");

        int distance = 0;
        for (int i = 0; i < hash1.length(); i++) {
            if (hash1.charAt(i) != hash2.charAt(i)) distance++;
        }
        return distance;
    }

    // =========================
    // helpers
    // =========================

    private static boolean hasFiniteNonZeroValue(float[] vector) {
        // 全要素が0または不正値のベクトルでは、LSHの符号判定がすべて同点になりやすい。
        for (float v : vector) {
            if (Float.isFinite(v) && v != 0f) {
                return true;
            }
        }
        return false;
    }

    private static boolean tieBit(int bitIndex, boolean hasSignal) {
        // dot == 0 の同点時に、bitIndex だけから決まる擬似ランダムなビットを返す。
        long x = 0x9E3779B97F4A7C15L * (bitIndex + 1L);
        x ^= (x >>> 30);
        x *= 0xBF58476D1CE4E5B9L;
        x ^= (x >>> 27);
        x *= 0x94D049BB133111EBL;
        x ^= (x >>> 31);
        return hasSignal ? ((x & 1L) == 1L) : (((x >>> 1) & 1L) == 1L);
    }

    private static String bitsToString(long[] bits, int numBits) {
        // 内部表現の long[] を、ログ表示やデバッグに使いやすい 0/1 文字列へ変換する。
        StringBuilder sb = new StringBuilder(numBits);
        for (int i = 0; i < numBits; i++) {
            int word = i >>> 6;
            int bit = i & 63;
            long v = (bits[word] >>> bit) & 1L;
            sb.append(v == 1L ? '1' : '0');
        }
        return sb.toString();
    }

    public int getNumHashBits() {
        return numHashBits;
    }

    public int getDimension() {
        return dimension;
    }
}
