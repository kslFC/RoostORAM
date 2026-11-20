package com.ringoram;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;








/**
 * Client-side block buffer indexed both by block ID and assigned leaf.
 * Subtree counters support efficient selection of blocks eligible for write-back.
 */
public class Stash {
	private Map<Integer,Block> stash_hash;
	private List<List<Block>> stash_list;
	private int[] counter;
	private final Random random;

	public Stash(){
		this(new Random().nextLong());
	}

	public Stash(long seed){
		this.random = new Random(seed);
		this.counter = new int[Configs.BUCKET_COUNT];
		this.stash_hash = new HashMap<Integer,Block>();
		this.stash_list = new ArrayList<List<Block>>();
		for(int i=0;i<Configs.BUCKET_COUNT;i++){
			List<Block> bucket_i = new ArrayList<Block>();
			this.stash_list.add(bucket_i);
		}
	}

	public void add(Block blk){

		if(!stash_hash.containsKey(blk.getBlockIndex())){
			blk.setEvictCount(Configs.DEFAULT_DELAYED_EVICT_COUNT);
			stash_hash.put(blk.getBlockIndex(), blk);
			stash_list.get(blk.getLeaf_id()).add(blk);
			for (int pos = blk.getLeaf_id();pos>=0;pos = (pos - 1) >> 1) {

	            counter[pos]++;
	            if (pos == 0)
	                break;
	        }
		}
	}

	public Block find_by_blockIndex(int blockIndex){

		if(!stash_hash.containsKey(blockIndex)){
	        return null;
	    }
	    return stash_hash.get(blockIndex);
	}

	public int size() {
		return stash_hash.size();
	}

	public List<Block> blocksSnapshot() {
		return new ArrayList<Block>(stash_hash.values());
	}

	public boolean update_leaf_id(int blockIndex, int newLeafId) {

		if (!stash_hash.containsKey(blockIndex)) {
			return false;
		}
		if (newLeafId < Configs.LEAF_START || newLeafId >= Configs.BUCKET_COUNT) {
			throw new IllegalArgumentException("newLeafId out of range: " + newLeafId);
		}

		Block block = stash_hash.get(blockIndex);
		int oldLeafId = block.getLeaf_id();
		if (oldLeafId == newLeafId) {
			return true;
		}

		if (oldLeafId >= 0 && oldLeafId < Configs.BUCKET_COUNT) {
			boolean removed = stash_list.get(oldLeafId).remove(block);
			if (removed) {
				for (int pos = oldLeafId; pos >= 0; pos = (pos - 1) >> 1) {
					counter[pos]--;
					if (pos == 0)
						break;
				}
			}
		}

		block.setLeaf_id(newLeafId);
		stash_list.get(newLeafId).add(block);
		for (int pos = newLeafId; pos >= 0; pos = (pos - 1) >> 1) {
			counter[pos]++;
			if (pos == 0)
				break;
		}
		return true;
	}

	public boolean remove_by_blockIndex(int blockIndex) {
		Block block = stash_hash.get(blockIndex);
		if (block == null) {
			return false;
		}
		int leafId = block.getLeaf_id();
		if (leafId >= 0 && leafId < Configs.BUCKET_COUNT) {
			boolean removed = stash_list.get(leafId).remove(block);
			if (removed) {
				for (int pos = leafId; pos >= 0; pos = (pos - 1) >> 1) {
					counter[pos]--;
					if (pos == 0) {
						break;
					}
				}
			}
		}
		stash_hash.remove(blockIndex);
		return true;
	}


	/** Removes up to {@code max} blocks whose assigned paths include the target bucket. */
	public int remove_by_bucket(int bucket_id, int max, Block[] block_list){
		int remove = remove_by_bucket_helper(bucket_id, max, 0, block_list);
		return remove;
	}
	public int remove_by_bucket_helper(int bucket_id, int len, int start, Block[] block_list){

		int delete_now=0;
	    int delete_max=0;
	    Block block;
	    if (bucket_id >=Configs.BUCKET_COUNT || counter[bucket_id]<=0) {
	        return 0;
	    }
	    if (bucket_id >= Configs.LEAF_START) {

	        delete_now = counter[bucket_id];
	        delete_max = min(delete_now, len);
	        for (int j = 0;j < delete_max;j++) {
	            block = stash_list.get(bucket_id).get(0);
	            stash_list.get(bucket_id).remove(0);
	            block_list[start++] = block;
	            stash_hash.remove(block.getBlockIndex());
	            for (int pos_run = bucket_id;pos_run>=0; pos_run = (pos_run - 1) >> 1) {
	                counter[pos_run]--;
	                if (pos_run == 0)
	                    break;
	            }
	        }
	        return delete_max;
	    }
	    int random = get_random(2);

	    int left, right;
	    if (random > 0) {
	        left = 2 * bucket_id + 1;
	        right = 2 * bucket_id + 2;
	    } else {
	        left = 2 * bucket_id + 2;
	        right = 2 * bucket_id + 1;
	    }


	    int add = remove_by_bucket_helper(left, len, start, block_list);
	    len -= add;
	    start += add;
	    if (len == 0)
	        return add;
	    return remove_by_bucket_helper(right, len, start, block_list) + add;
	}
	public int min(int a,int b){
		int min = 0;
		min = (((a) < (b)) ? (a):(b));
		return min;
	}
	public int get_random(int range) {
		return random.nextInt(range);
	 }

	public Map<Integer, Block> getStash_hash() {
		return stash_hash;
	}
	public void setStash_hash(Map<Integer, Block> stash_hash) {
		this.stash_hash = stash_hash;
	}
	public List<List<Block>> getStash_list() {
		return stash_list;
	}
	public void setStash_list(List<List<Block>> stash_list) {
		this.stash_list = stash_list;
	}
	public int[] getCount() {
		return counter;
	}
	public void setCount(int[] counter) {
		this.counter = counter;
	}
    public void showStash() {

        System.out.println();
        for (Integer key : stash_hash.keySet()) {
            System.out.println(key + " block in the stash");
        }
        System.out.println();
    }

}
