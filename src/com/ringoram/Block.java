package com.ringoram;

import java.util.ArrayList;
import java.util.List;







/** A real ORAM block together with its assigned leaf and delayed-eviction counter. */
public class Block {
	private int blockIndex;
	private int leaf_id;
	private byte[] data;
	private int evictCount;
	private List<Byte> topicBitsList;

	public Block(){
		this.blockIndex = -1;
		this.leaf_id = -1;
		this.data = new byte[Configs.BLOCK_DATA_LEN];
		this.evictCount = Configs.DEFAULT_DELAYED_EVICT_COUNT;
		this.topicBitsList = new ArrayList<>();
	}

	public Block( int blockIndex, int leaf_id, byte[] data){
		this.blockIndex = blockIndex;
		this.leaf_id = leaf_id;
		this.data = data;
		this.evictCount = Configs.DEFAULT_DELAYED_EVICT_COUNT;
		this.topicBitsList = new ArrayList<>();
	}
	public Block(int blockIndex, int leaf_id, byte[] data, List<Byte> topicBitsList) {
		this.blockIndex = blockIndex;
		this.leaf_id = leaf_id;
		this.data = data;
		this.evictCount = Configs.DEFAULT_DELAYED_EVICT_COUNT;
		this.topicBitsList = new ArrayList<>(topicBitsList);
	}

	public int getBlockIndex() {
		return blockIndex;
	}
	public void setBlockIndex(int blockIndex) {
		this.blockIndex = blockIndex;
	}
	public int getLeaf_id() {
		return leaf_id;
	}
	public void setLeaf_id(int leaf_id) {
		this.leaf_id = leaf_id;
	}
	public byte[] getData() {
		return data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}
	public int getEvictCount() {
		return evictCount;
	}
	public void setEvictCount(int evictCount) {
		this.evictCount = evictCount;
	}
	public void decrementEvictCount() {
		this.evictCount--;
	}
	public void addTopicBits(byte value) {

        if ((value & 0xF0) != 0) {
            throw new IllegalArgumentException("Only 4-bit values (0-15) are allowed.");
        }
        topicBitsList.add(value);
    }

    public List<Byte> getTopicBitsList() {
        return new ArrayList<>(topicBitsList);
    }

    public void clearTopicBits() {
        topicBitsList.clear();
    }

}
