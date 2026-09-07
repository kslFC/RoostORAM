package com.ringoram;

import java.io.Serializable;

/**
 * bucketの配置情報と読み取り状態を保持するメタデータ。
 *
 * meta_bufの前半は実ブロック枠に入っているblockIndex、後半はbucket_data内の
 * offset順列を表す。valid_bitsは各offsetがまだ読めるかどうかを示す。
 */
public class BucketMetadata implements Serializable{
	/**
	 * bucket meta data
	 * @param read_counter: the bucket be read times
	 * @param meta_buf: the length is (Configs.REAL_BLOCK_COUNT + Configs.Z),
	 *                  from 0 to Configs.REAL_BLOCK_COUNT is real block index,
	 *                  and from Configs.REAL_BLOCK_COUNT to (Configs.REAL_BLOCK_COUNT + Configs.Z) 
	 *                  is block data offset in bucket_data,
	 *                  offset:from 0 to Configs.REAL_BLOCK_COUNT is real block offset in bucket_data,
	 *                   and from Configs.REAL_BLOCK_COUNT to Configs.Z is dummy block offset in bucket_data.
	 * @param valid_bits: record every block in the bucket can be accessed or not,
	 *                    0 means this block can't been accessed, the block may in client stash,
	 *                    1 means this block can be read, after being read, set to 0
	 */
	private static final long serialVersionUID = 1L;
	private int read_counter;
	private int[] meta_buf;
	private byte[] valid_bits;
	
	public BucketMetadata(){
		// 初期状態では全offsetを有効にし、実ブロック枠は後でinit_block_index()で空にする。
		this.read_counter = 0;
		this.meta_buf = new int[Configs.REAL_BLOCK_COUNT + Configs.Z];
		this.valid_bits = new byte[Configs.Z];		
		for(int i=0;i<Configs.Z;i++){
			this.valid_bits[i] = (byte)1;
		}
	}
	
	public BucketMetadata(int read_counter,int[] meta_buf,byte[] valid_bits){
		this.read_counter = read_counter;
		this.meta_buf = meta_buf;
		this.valid_bits = valid_bits;
	}
	
	public void init_block_index(){
		// -1は実ブロックが入っていないdummy枠であることを示す。
		for(int i=0;i<Configs.REAL_BLOCK_COUNT;i++){
			meta_buf[i] = -1;
		}
	}
	
	public int[] get_block_index(){
		// 実ブロック枠に対応するblockIndexだけを切り出して返す。
		int[] add = new int[Configs.REAL_BLOCK_COUNT];
		for(int i=0;i<Configs.REAL_BLOCK_COUNT;i++){
			add[i] = meta_buf[i];
		}
		return add;
	}
	
	public void set_offset(int[] offset){
		// bucket_data上の物理offset順列をメタデータ後半へ保存する。
		for(int i = 0;i<Configs.Z;i++){
			meta_buf[Configs.REAL_BLOCK_COUNT + i] = offset[i];
		}
	}
	
	public int[] get_offset(){
		int[] off_set = new int[Configs.Z];
		for(int i=0;i<Configs.Z;i++){
			off_set[i] = meta_buf[Configs.REAL_BLOCK_COUNT+i];
		}
		return off_set;
	}
	
	public void add_meta_readcounter(){
		// bucket内のdummy消費量を推定するためのread counter。
		this.read_counter += 1;
	}
	
	public void set_meta_validbit(int id){
		// 読まれたoffsetを無効化し、同じdummy/real blockの再利用を避ける。
		this.valid_bits[id] = (byte)0;
	}
	
	public void set_blockIndex_bit(int index, int blockIndex){
		// write_bucket時に、実ブロック枠へ新しいblockIndexを書き込む。
		if(index<Configs.REAL_BLOCK_COUNT){
			meta_buf[index] = blockIndex;
		}else{
			System.out.println("set meta_buf block index out of range!");
		}
	}
	
	public int[] getMeta_buf() {
		return meta_buf;
	}
	public void setMeta_buf(int[] meta_buf) {
		this.meta_buf = meta_buf;
	}
	public byte[] getValid_bits() {
		return valid_bits;
	}
	public void setValid_bits(byte[] valid_bits) {
		this.valid_bits = valid_bits;
	}
	public int getRead_counter() {
		return read_counter;
	}
	public void setRead_counter(int read_counter) {
		this.read_counter = read_counter;
	}
}
