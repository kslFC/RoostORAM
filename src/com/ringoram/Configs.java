package com.ringoram;






/** Central experiment and tree parameters shared by the client and server. */
public class Configs {

	public static int THREAD_FIXED = 4;


	public static String SERVER_HOSTNAME = "localhost";
	public static int SERVER_PORT = 12339;


	public static int BLOCK_DATA_LEN = 1392;

	public static int REAL_BLOCK_COUNT = 4;
	public static int DUMMY_BLOCK_COUNT = 6;

	public static int BUCKET_COUNT = 31;


	public static int Z = REAL_BLOCK_COUNT + DUMMY_BLOCK_COUNT;

	public static int BLOCK_COUNT = 48;

	public static int HEIGHT = (int) (Math.log(BUCKET_COUNT)/Math.log(2) + 1);
	public static int LEAF_COUNT = (BUCKET_COUNT+1)/2;
	public static int LEAF_START = BUCKET_COUNT - LEAF_COUNT;


	public static int METADATA_BYTES_LEN = 4+4*(Configs.REAL_BLOCK_COUNT + Configs.Z)+Configs.Z;


	public static int SHUFFLE_RATE = 4;



	public static int DEFAULT_DELAYED_EVICT_COUNT = 4;
	public static int HARD_STASH_LIMIT = 36;

	public static int MAX_BATCH_EVICT_BLOCKS_PER_ACCESS = 10;


	public enum OPERATION{ORAM_ACCESS_READ,ORAM_ACCESS_WRITE};


	public static String STORAGE_PATH = "/tmp/serverStorage/bucket_";
}
