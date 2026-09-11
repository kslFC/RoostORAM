package com.ringoram;

/**
 * 実験全体で共有するORAM設定値。
 *
 * 木サイズ、bucket容量、ブロック長、サーバ保存先などをここで一元管理する。
 */
public class Configs {
	// クライアント・サーバの非同期I/O用固定スレッド数。
	public static int THREAD_FIXED = 4;

	// 接続先サーバ。別ホストで実験する場合はSERVER_HOSTNAMEを変更する。
	public static String SERVER_HOSTNAME = "localhost";
	public static int SERVER_PORT = 12339;
	
	// .binの348次元floatを格納するため、348 * 4 = 1392 bytesにしている。
	public static int BLOCK_DATA_LEN = 1392;
	// 1bucketが保持できる実ブロック枠とdummy枠。総スロット数Zは10。
	public static int REAL_BLOCK_COUNT = 4;
	public static int DUMMY_BLOCK_COUNT = 6;
	// 完全二分木を前提とする。31 bucketなら高さ5、leaf数16、leaf IDは15..30。
	public static int BUCKET_COUNT = 31;
	
	// 1bucketの総スロット数。
	public static int Z = REAL_BLOCK_COUNT + DUMMY_BLOCK_COUNT;
	// 実験で使用する論理データブロック数（3カテゴリ各16件）。
	public static int BLOCK_COUNT = 48;
	// 木構造から導出される高さ、leaf数、leaf開始ID。
	public static int HEIGHT = (int) (Math.log(BUCKET_COUNT)/Math.log(2) + 1);
	public static int LEAF_COUNT = (BUCKET_COUNT+1)/2;
	public static int LEAF_START = BUCKET_COUNT - LEAF_COUNT;
	
	//read_counter, meta_buf, valid_bits
	public static int METADATA_BYTES_LEN = 4+4*(Configs.REAL_BLOCK_COUNT + Configs.Z)+Configs.Z;
	
	// 何回アクセスしたら定期evictを行うか。4なら4アクセスごとにevictする。
	public static int SHUFFLE_RATE = 4;

	// LSHモードの遅延evict設定。count初期値4、soft超過で単体evict、hard超過でpath fallback。
	// baselineはこれらを使わず、従来のSHUFFLE_RATEごとのpath evictを行う。
	public static int DEFAULT_DELAYED_EVICT_COUNT = 4;
	public static int SOFT_STASH_LIMIT = 24;
	public static int HARD_STASH_LIMIT = 36;
	public static int MAX_SINGLE_EVICT_PER_ACCESS = 10;
	
	//request operation: read or write
	public enum OPERATION{ORAM_ACCESS_READ,ORAM_ACCESS_WRITE};
	
	//bucket store source
	public static String STORAGE_PATH = "/tmp/serverStorage/bucket_";
}
