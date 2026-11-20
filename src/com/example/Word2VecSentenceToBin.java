package com.example;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
















/** Converts sentence tokens into the fixed-size vector blocks consumed by the ORAM experiment. */
public class Word2VecSentenceToBin {


    public static void main(String[] args) throws Exception {

        Map<String, String> a = parseArgs(args);

        String modelPath = req(a, "--model");
        String sentence  = req(a, "--sentence");
        String outPath   = req(a, "--out");
        int dim          = Integer.parseInt(a.getOrDefault("--dim", "348"));
        boolean normalize = Boolean.parseBoolean(a.getOrDefault("--normalize", "true"));

        System.out.println("[INFO] model=" + modelPath);
        System.out.println("[INFO] dim=" + dim);
        System.out.println("[INFO] out=" + outPath);
        System.out.println("[INFO] normalize=" + normalize);
        System.out.println("[INFO] sentence=\"" + sentence + "\"");


        Path out = Paths.get(outPath);
        if (out.getParent() != null) Files.createDirectories(out.getParent());


        List<String> tokens = tokenize(sentence);
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("Sentence has no tokens.");
        }


        float[] avg300 = averageVectorFromBinaryModel(modelPath, tokens);


        float[] vec = toDim(avg300, dim);
        if (normalize) {

            normalizeL2(vec);
        }


        writeFloatBinLittleEndian(vec, out);

        System.out.println("[OK] wrote: " + out.toAbsolutePath() + " bytes=" + (dim * 4));
        System.out.println("[OK] firstFloats=" + preview(vec, 6));
    }










    static float[] averageVectorFromBinaryModel(String modelPath, List<String> tokens) throws IOException {

        List<Set<String>> tokenVariants = new ArrayList<>();
        Set<String> wantedVariants = new HashSet<>();
        for (String token : tokens) {
            Set<String> variants = tokenVariants(token);
            tokenVariants.add(variants);
            wantedVariants.addAll(variants);
        }

        float[] sum = null;
        boolean[] matched = new boolean[tokens.size()];
        int found = 0;

        try (InputStream is = new BufferedInputStream(new FileInputStream(modelPath), 1 << 20)) {

            int vocabSize = readIntAscii(is);
            int vectorSize = readIntAscii(is);

            System.out.println("[INFO] vocabSize=" + vocabSize + " vectorSize=" + vectorSize);
            if (vectorSize <= 0) throw new IOException("Invalid vectorSize: " + vectorSize);

            sum = new float[vectorSize];


            for (int i = 0; i < vocabSize; i++) {
                String word = readWord(is);
                if (wantedVariants.contains(word)) {

                    byte[] vectorBytes = readVectorBytes(is, vectorSize);
                    int matchedNow = addMatchedVector(
                            word, vectorBytes, vectorSize, tokenVariants, matched, sum);
                    found += matchedNow;
                } else {
                    skipFully(is, (long) vectorSize * 4L);
                }


                consumeLineBreaks(is);

                if (found >= tokens.size()) {
                    System.out.println("[INFO] early stop: found=" + found);
                    break;
                }
            }
        }

        if (found == 0) {

            throw new IllegalArgumentException(
                    "No token found in model. tokens=" + tokens
                            + " ; .bin was not written to avoid identical OOV vectors.");
        }

        if (found < tokens.size()) {
            System.out.println("[WARN] missingTokens=" + missingTokens(tokens, matched));
        }


        for (int d = 0; d < sum.length; d++) sum[d] /= found;
        System.out.println("[INFO] matchedTokens=" + found + "/" + tokens.size());
        return sum;
    }


    static int readIntAscii(InputStream is) throws IOException {

        int c;
        do {
            c = is.read();
            if (c == -1) throw new EOFException("EOF while reading header int");
        } while (c == ' ' || c == '\n' || c == '\r' || c == '\t');


        int sign = 1;
        if (c == '-') { sign = -1; c = is.read(); }
        int val = 0;
        while (c >= '0' && c <= '9') {
            val = val * 10 + (c - '0');
            c = is.read();
        }

        return val * sign;
    }

    static String readWord(InputStream is) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(64);
        int c;

        do {
            c = is.read();
            if (c == -1) throw new EOFException("EOF while reading word");
        } while (c == '\n' || c == '\r' || c == ' ');

        while (c != ' ' && c != '\n' && c != '\r') {
            bos.write(c);
            c = is.read();
            if (c == -1) throw new EOFException("EOF mid-word");
        }
        return bos.toString(StandardCharsets.UTF_8);
    }

    static byte[] readVectorBytes(InputStream is, int n) throws IOException {

        byte[] buf = is.readNBytes(n * 4);
        if (buf.length != n * 4) throw new EOFException("EOF while reading vector");
        return buf;
    }

    static void addVectorBytesLE(byte[] buf, int n, float[] sum) {

        for (int i = 0; i < n; i++) {
            int off = i * 4;
            int bits = (buf[off] & 0xFF)
                    | ((buf[off + 1] & 0xFF) << 8)
                    | ((buf[off + 2] & 0xFF) << 16)
                    | ((buf[off + 3] & 0xFF) << 24);
            sum[i] += Float.intBitsToFloat(bits);
        }
    }

    static void skipFully(InputStream is, long n) throws IOException {

        long remaining = n;
        while (remaining > 0) {
            long skipped = is.skip(remaining);
            if (skipped > 0) {
                remaining -= skipped;
                continue;
            }
            if (is.read() == -1) {
                throw new EOFException("EOF while skipping vector");
            }
            remaining--;
        }
    }

    static void consumeLineBreaks(InputStream is) throws IOException {

        is.mark(8);
        int c = is.read();
        while (c == '\n' || c == '\r') {
            is.mark(8);
            c = is.read();
        }
        if (c != -1) is.reset();
    }


    static float[] toDim(float[] v, int dim) {
        float[] out = new float[dim];
        int copy = Math.min(v.length, dim);
        System.arraycopy(v, 0, out, 0, copy);

        return out;
    }

    static void normalizeL2(float[] vector) {
        double squaredNorm = 0.0;
        for (float value : vector) {
            if (!Float.isFinite(value)) {
                throw new IllegalArgumentException("Vector contains a non-finite value.");
            }
            squaredNorm += (double) value * (double) value;
        }
        if (squaredNorm == 0.0) {
            throw new IllegalArgumentException("Cannot normalize a zero vector.");
        }

        double inverseNorm = 1.0 / Math.sqrt(squaredNorm);
        for (int i = 0; i < vector.length; i++) {
            vector[i] = (float) (vector[i] * inverseNorm);
        }
    }

    static void writeFloatBinLittleEndian(float[] v, Path out) throws IOException {

        try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(out))) {
            byte[] buf = new byte[4];
            for (float f : v) {
                int bits = Float.floatToIntBits(f);
                buf[0] = (byte) (bits & 0xFF);
                buf[1] = (byte) ((bits >>> 8) & 0xFF);
                buf[2] = (byte) ((bits >>> 16) & 0xFF);
                buf[3] = (byte) ((bits >>> 24) & 0xFF);
                os.write(buf);
            }
        }
    }


    static List<String> tokenize(String s) {

        String norm = s.replaceAll("[^A-Za-z0-9_]+", " ").trim();
        if (norm.isEmpty()) return Collections.emptyList();
        return Arrays.asList(norm.split("\\s+"));
    }

    static Set<String> tokenVariants(String token) {

        Set<String> variants = new LinkedHashSet<>();
        variants.add(token);
        variants.add(token.toLowerCase(Locale.ROOT));
        variants.add(capitalize(token));
        variants.add(token.toUpperCase(Locale.ROOT));
        return variants;
    }

    static int addMatchedVector(
            String word,
            byte[] vectorBytes,
            int vectorSize,
            List<Set<String>> tokenVariants,
            boolean[] matched,
            float[] sum) {

        int matchedNow = 0;
        for (int i = 0; i < tokenVariants.size(); i++) {
            if (!matched[i] && tokenVariants.get(i).contains(word)) {
                addVectorBytesLE(vectorBytes, vectorSize, sum);
                matched[i] = true;
                matchedNow++;
            }
        }
        return matchedNow;
    }

    static List<String> missingTokens(List<String> tokens, boolean[] matched) {

        List<String> missing = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            if (!matched[i]) missing.add(tokens.get(i));
        }
        return missing;
    }

    static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        String lower = s.toLowerCase(Locale.ROOT);
        return Character.toUpperCase(lower.charAt(0)) + lower.substring(1);
    }

    static String preview(float[] v, int n) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < Math.min(n, v.length); i++) {
            if (i > 0) sb.append(", ");
            sb.append(String.format(Locale.ROOT, "%.6g", v[i]));
        }
        sb.append("]");
        return sb.toString();
    }


    static Map<String, String> parseArgs(String[] args) {

        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String k = args[i];
            if (!k.startsWith("--")) continue;
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                m.put(k, args[i + 1]);
                i++;
            } else {
                m.put(k, "true");
            }
        }
        return m;
    }

    static String req(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null) throw new IllegalArgumentException("Missing arg: " + key);
        return v;
    }
}
