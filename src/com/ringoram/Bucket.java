package com.ringoram;

import java.io.Serializable;







/** Serialized tree bucket containing block data and Ring ORAM metadata. */
public class Bucket implements Serializable {

	private static final long serialVersionUID = 1L;

	private int id;
	private int address;
	private byte[] bucket_data;
	private BucketMetadata bucket_meta;

	public Bucket() {
		this.id = 0;
		this.address = 0;
		this.bucket_data = new byte[Configs.BLOCK_DATA_LEN * Configs.Z];
		this.bucket_meta = new BucketMetadata();
	}

	public Bucket(int id, byte[] bucket_data, BucketMetadata meta) {
		this.id = id;
		this.address = id;
		this.bucket_data = bucket_data;
		this.bucket_meta = meta;
	}

	public Bucket(int id, int address, byte[] bucket_data, BucketMetadata meta) {
		this.id = id;
		this.address = address;
		this.bucket_data = bucket_data;
		this.bucket_meta = meta;
	}


	public byte[] getBlock(int offset) {
		int startIndex = offset * Configs.BLOCK_DATA_LEN;
		byte[] returndata = new byte[Configs.BLOCK_DATA_LEN];
		for (int i = 0; i < Configs.BLOCK_DATA_LEN; i++) {
			returndata[i] = bucket_data[startIndex + i];
		}
		return returndata;
	}


	public void reset_valid_bits(int index) {
		bucket_meta.set_meta_validbit(index);
	}


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
