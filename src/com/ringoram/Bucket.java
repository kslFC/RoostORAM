package com.ringoram;

import java.io.Serializable;

/**
 * ORAM木の1bucketを表す。
 *
 * bucket_dataには実ブロック領域とdummy領域を連結して保存し、bucket_metaが
 * どのoffsetにどの論理ブロックがあるか、どのoffsetが有効かを管理する。
 */
public class Bucket implements Serializable {

	private static final long serialVersionUID = 1L;

	private int id;                 // bucket id in the tree, root is 0
	private int address;           // ← 追加：ツリー内でのアドレス
	private byte[] bucket_data;    // all block data in the bucket
	private BucketMetadata bucket_meta; // bucket meta data

	public Bucket() {
		this.id = 0;
		this.address = 0; // ← 初期値
		this.bucket_data = new byte[Configs.BLOCK_DATA_LEN * Configs.Z];
		this.bucket_meta = new BucketMetadata();
	}

	public Bucket(int id, byte[] bucket_data, BucketMetadata meta) {
		this.id = id;
		this.address = id; // ← デフォルトでidと一致させることも可能
		this.bucket_data = bucket_data;
		this.bucket_meta = meta;
	}

	public Bucket(int id, int address, byte[] bucket_data, BucketMetadata meta) {
		this.id = id;
		this.address = address;
		this.bucket_data = bucket_data;
		this.bucket_meta = meta;
	}

	// bucket_data内のoffset番目にある固定長ブロックを取り出す。
	public byte[] getBlock(int offset) {
		int startIndex = offset * Configs.BLOCK_DATA_LEN;
		byte[] returndata = new byte[Configs.BLOCK_DATA_LEN];
		for (int i = 0; i < Configs.BLOCK_DATA_LEN; i++) {
			returndata[i] = bucket_data[startIndex + i];
		}
		return returndata;
	}

	// READBLOCKで読まれたoffsetを無効化し、次のshuffleまで再読込されないようにする。
	public void reset_valid_bits(int index) {
		bucket_meta.set_meta_validbit(index);
	}

	// dummy枯渇を判断するため、bucketが読まれた回数を増やす。
	public void add_read_counter() {
		bucket_meta.add_meta_readcounter();
	}

	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}

	public int getAddress() {
		return address;
	}
	public void setAddress(int address) {
		this.address = address;
	}

	public byte[] getBucket_data() {
		return bucket_data;
	}
	public void setBucket_data(byte[] bucket_data) {
		this.bucket_data = bucket_data;
	}

	public BucketMetadata getBucket_meta() {
		return bucket_meta;
	}
	public void setBucket_meta(BucketMetadata bucket_meta) {
		this.bucket_meta = bucket_meta;
	}
}
