package com.example;

import java.util.Random;












/** Random-hyperplane LSH used to preserve semantic locality in ORAM leaf assignment. */
public final class LSH {

    private final int numHashBits;
    private final int dimension;

    private final float[][] randomHyperplanes;






    public LSH(int numHashBits, int dimension, long seed) {
        if (numHashBits <= 0) throw new IllegalArgumentException("numHashBits must be > 0");
        if (dimension <= 0) throw new IllegalArgumentException("dimension must be > 0");

        this.numHashBits = numHashBits;
        this.dimension = dimension;
        this.randomHyperplanes = new float[numHashBits][dimension];

        Random rng = new Random(seed);



        for (int i = 0; i < numHashBits; i++) {
            for (int d = 0; d < dimension; d++) {
                randomHyperplanes[i][d] = (float) rng.nextGaussian();
            }
        }
    }







    /** Encodes a vector as a binary signature determined by hyperplane signs. */
    public String computeHash(float[] vector) {
        final long[] bits = computeHashBits(vector);
        return bitsToString(bits, numHashBits);
    }








    public long[] computeHashBits(float[] vector) {
        if (vector == null) throw new IllegalArgumentException("vector is null");
        if (vector.length != dimension) {
            throw new IllegalArgumentException("vector length " + vector.length + " != dimension " + dimension);
        }

        int words = (numHashBits + 63) >>> 6;
        long[] out = new long[words];
        boolean hasSignal = hasFiniteNonZeroValue(vector);

        for (int i = 0; i < numHashBits; i++) {

            double dot = 0.0;
            float[] hyperplane = randomHyperplanes[i];

            for (int d = 0; d < dimension; d++) {
                float v = vector[d];

                if (!Float.isFinite(v)) v = 0f;
                dot += (double) v * (double) hyperplane[d];
            }


            if (dot > 0.0 || (dot == 0.0 && tieBit(i, hasSignal))) {
                int word = i >>> 6;
                int bit = i & 63;
                out[word] |= (1L << bit);
            }
        }

        return out;
    }




    public static int hammingDistance(long[] a, long[] b) {
        if (a == null || b == null) throw new IllegalArgumentException("hash is null");
        if (a.length != b.length) throw new IllegalArgumentException("hash length mismatch");

        int dist = 0;
        for (int i = 0; i < a.length; i++) {
            dist += Long.bitCount(a[i] ^ b[i]);
        }
        return dist;
    }




    public static int hammingDistance(String hash1, String hash2) {
        if (hash1 == null || hash2 == null) throw new IllegalArgumentException("hash is null");
        if (hash1.length() != hash2.length()) throw new IllegalArgumentException("Hash lengths must match.");

        int distance = 0;
        for (int i = 0; i < hash1.length(); i++) {
            if (hash1.charAt(i) != hash2.charAt(i)) distance++;
        }
        return distance;
    }





    private static boolean hasFiniteNonZeroValue(float[] vector) {

        for (float v : vector) {
            if (Float.isFinite(v) && v != 0f) {
                return true;
            }
        }
        return false;
    }

    private static boolean tieBit(int bitIndex, boolean hasSignal) {

        long x = 0x9E3779B97F4A7C15L * (bitIndex + 1L);
        x ^= (x >>> 30);
        x *= 0xBF58476D1CE4E5B9L;
        x ^= (x >>> 27);
        x *= 0x94D049BB133111EBL;
        x ^= (x >>> 31);
        return hasSignal ? ((x & 1L) == 1L) : (((x >>> 1) & 1L) == 1L);
    }

    private static String bitsToString(long[] bits, int numBits) {

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
