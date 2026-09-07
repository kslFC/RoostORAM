package com.ringoram;
/*
 * store each bucket to a file
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * サーバ側bucketのファイル保存を担当する薄い永続化層。
 *
 * 各bucketはConfigs.STORAGE_PATH + bucketIdのファイルにJavaオブジェクトとして保存される。
 */
public class ServerStorage {

	public ServerStorage(){
		
	}
	
	// bucketを1ファイルとして保存する。WRITEBUCKETや初期化時に使う。
	public void set_bucket(int id,Bucket bkt) throws IOException{
		String dst = Configs.STORAGE_PATH + id;
		File f = new File(dst);
		File storageDirectory = f.getParentFile();
		if (storageDirectory != null
				&& !storageDirectory.isDirectory()
				&& !storageDirectory.mkdirs()
				&& !storageDirectory.isDirectory()) {
			throw new IOException("Failed to create server storage directory: "
					+ storageDirectory.getAbsolutePath());
		}
		if(!f.exists())
			f.createNewFile();
		
        FileOutputStream out;
        try {
            out = new FileOutputStream(f);
            ObjectOutputStream objOut=new ObjectOutputStream(out);
            objOut.writeObject(bkt);
            objOut.flush();
            objOut.close();
           // System.out.println("write bucket "+id+" success!");
        } catch (IOException e) {
            System.out.println("write bucket "+id+" failed");
            e.printStackTrace();
        }
	}
	
	// bucket IDに対応するファイルを読み、Bucketオブジェクトとして復元する。
	public Bucket get_bucket(int id) throws IOException{
		String dst = Configs.STORAGE_PATH + id;
		Bucket bucket = new Bucket();
		
		File f = new File(dst);
		if(!f.exists()){
			System.out.println("error:when reading bucket "+id+", find no file in storage!");
		}
        FileInputStream in;
        try {
            in = new FileInputStream(f);
            ObjectInputStream objIn=new ObjectInputStream(in);
            bucket = (Bucket) objIn.readObject();
            objIn.close();
         //   System.out.println("read object success!");
        } catch (IOException e) {
            System.out.println("read bucket "+id+" failed");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
		return bucket;
	}
}
