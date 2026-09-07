package com.example;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Word2Vec (GoogleNews-vectors-negative300.bin などの "C word2vec binary" 形式) を読み、
 * 入力文を単語分割して平均ベクトルを作り、指定次元(dim)に変換して .bin に書き出す。
 *
 * 出力 .bin 形式:
 *   float32 little-endian を dim 個 (dim*4 bytes)
 *
 * 例:
 *   javac src/com/example/Word2VecSentenceToBin.java
 *   java -cp src com.example.Word2VecSentenceToBin \
 *     --model models/GoogleNews-vectors-negative300.bin \
 *     --sentence "application security privacy" \
 *     --out data/0.bin \
 *     --dim 348
 */
public class Word2VecSentenceToBin {

    // ===== CLI =====
    public static void main(String[] args) throws Exception {
        // コマンドライン引数からモデル、入力文、出力先、出力次元を取得する。
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

        // 出力先ディレクトリがなければ作成し、後続の .bin 書き込み失敗を防ぐ。
        Path out = Paths.get(outPath);
        if (out.getParent() != null) Files.createDirectories(out.getParent());

        // 文章を英数字ベースでトークン化し、Word2Vec の語彙検索対象にする。
        List<String> tokens = tokenize(sentence);
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("Sentence has no tokens.");
        }

        // Word2Vecを開いて必要単語だけ読む（巨大モデルなので全件をメモリに載せない）。
        float[] avg300 = averageVectorFromBinaryModel(modelPath, tokens);

        // 300次元モデルを実験で使う dim に合わせる。足りない分は0埋めする。
        float[] vec = toDim(avg300, dim);
        if (normalize) {
            // Random Hyperplane LSHが比較する方向（コサイン類似度）に合わせる。
            normalizeL2(vec);
        }

        // Client 側の LSH がそのまま読める little-endian float32 配列として保存する。
        writeFloatBinLittleEndian(vec, out);

        System.out.println("[OK] wrote: " + out.toAbsolutePath() + " bytes=" + (dim * 4));
        System.out.println("[OK] firstFloats=" + preview(vec, 6));
    }

    // ===== Word2Vec binary loader (stream scan) =====
    /**
     * word2vec C binary:
     *   header: "<vocabSize> <vectorSize>\n"
     *   then repeated vocabSize:
     *     word bytes until space (0x20)
     *     vector floats (float32 little-endian) length=vectorSize
     *     (and usually newline)
     */
    static float[] averageVectorFromBinaryModel(String modelPath, List<String> tokens) throws IOException {
        // 各トークンについて大文字小文字の揺れを候補化し、モデル内の表記差を吸収する。
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
            // ヘッダ読み
            int vocabSize = readIntAscii(is);
            int vectorSize = readIntAscii(is);

            System.out.println("[INFO] vocabSize=" + vocabSize + " vectorSize=" + vectorSize);
            if (vectorSize <= 0) throw new IOException("Invalid vectorSize: " + vectorSize);

            sum = new float[vectorSize];

            // 語彙を先頭から逐次スキャンし、必要語以外のベクトルは読み飛ばす。
            for (int i = 0; i < vocabSize; i++) {
                String word = readWord(is); // spaceまで
                if (wantedVariants.contains(word)) {
                    // 見つかった語だけベクトル本体を読み、入力文の平均ベクトルへ加算する。
                    byte[] vectorBytes = readVectorBytes(is, vectorSize);
                    int matchedNow = addMatchedVector(
                            word, vectorBytes, vectorSize, tokenVariants, matched, sum);
                    found += matchedNow;
                } else {
                    skipFully(is, (long) vectorSize * 4L);
                }

                // 行末の \n が来ることが多い（来ないファイルもあるので空白類だけ軽く捨てる）
                consumeLineBreaks(is);

                if (found >= tokens.size()) {
                    System.out.println("[INFO] early stop: found=" + found);
                    break;
                }
            }
        }

        if (found == 0) {
            // 全単語が OOV の場合、全ブロックが同一の0ベクトルになりLSH比較が壊れるため失敗にする。
            throw new IllegalArgumentException(
                    "No token found in model. tokens=" + tokens
                            + " ; .bin was not written to avoid identical OOV vectors.");
        }

        if (found < tokens.size()) {
            System.out.println("[WARN] missingTokens=" + missingTokens(tokens, matched));
        }

        // 見つかった単語数で割り、文章全体を1つの平均ベクトルとして表現する。
        for (int d = 0; d < sum.length; d++) sum[d] /= found;
        System.out.println("[INFO] matchedTokens=" + found + "/" + tokens.size());
        return sum;
    }

    // ===== Helpers: binary reading =====
    static int readIntAscii(InputStream is) throws IOException {
        // ヘッダ中の空白を読み飛ばしてから、ASCII 数字を整数として読む。
        int c;
        do {
            c = is.read();
            if (c == -1) throw new EOFException("EOF while reading header int");
        } while (c == ' ' || c == '\n' || c == '\r' || c == '\t');

        // 符号と数字列を読み、区切り文字は消費したまま次の読み込みへ進む。
        int sign = 1;
        if (c == '-') { sign = -1; c = is.read(); }
        int val = 0;
        while (c >= '0' && c <= '9') {
            val = val * 10 + (c - '0');
            c = is.read();
        }
        // c is separator (space/newline). put back not possible; we just keep it consumed.
        return val * sign;
    }

    static String readWord(InputStream is) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(64);
        int c;
        // 前レコードの改行や空白を読み飛ばし、次の語彙文字列の先頭まで進める。
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
        // word2vec binary のベクトルは float32 が n 個連続するため、バイト列のまま読む。
        byte[] buf = is.readNBytes(n * 4);
        if (buf.length != n * 4) throw new EOFException("EOF while reading vector");
        return buf;
    }

    static void addVectorBytesLE(byte[] buf, int n, float[] sum) {
        // little-endian の4バイトを float に戻し、平均計算用の sum に加算する。
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
        // 巨大モデルでは不要ベクトルを捨てる処理が大半なので、EOF を検知しながら確実に読み飛ばす。
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
        // 実装やモデルによりベクトル後の改行有無が異なるため、改行だけ消費して次レコードへ合わせる。
        is.mark(8);
        int c = is.read();
        while (c == '\n' || c == '\r') {
            is.mark(8);
            c = is.read();
        }
        if (c != -1) is.reset();
    }

    // ===== Vector + file writing =====
    static float[] toDim(float[] v, int dim) {
        float[] out = new float[dim];
        int copy = Math.min(v.length, dim);
        System.arraycopy(v, 0, out, 0, copy);
        // dim > v.length は 0 埋め
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
        // Java の DataOutputStream は big-endian なので、ここでは手動で little-endian に並べる。
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

    // ===== Tokenize =====
    static List<String> tokenize(String s) {
        // 英語前提：記号は空白扱い
        String norm = s.replaceAll("[^A-Za-z0-9_]+", " ").trim();
        if (norm.isEmpty()) return Collections.emptyList();
        return Arrays.asList(norm.split("\\s+"));
    }

    static Set<String> tokenVariants(String token) {
        // GoogleNews モデルは単語の大小文字が混在するため、代表的な表記揺れを同じ単語として扱う。
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
        // 同じ入力単語に複数の表記候補が当たっても、最初の1回だけ平均へ加算する。
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
        // 見つからなかった単語をログに出し、生成したベクトルの解釈に使えるようにする。
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

    // ===== CLI parse =====
    static Map<String, String> parseArgs(String[] args) {
        // "--key value" 形式を Map に変換する。値なしオプションは true として扱う。
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
