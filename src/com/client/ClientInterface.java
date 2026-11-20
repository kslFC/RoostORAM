package com.client;

import com.ringoram.BucketMetadata;






public interface ClientInterface {





	public void read_path(int pathID, int blockIndex);


	public void evict_path(int pathID);


	public void early_reshuffle(int pathID, BucketMetadata[] meta_list);
}
