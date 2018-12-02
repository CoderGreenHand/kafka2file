package com.bfd.kafka.util;

import java.io.File;
import java.io.IOException;

public class FileUtil {
	public static boolean createDir(String path){
    	File outDir = new File(path);
    	if(outDir.exists()){
    		if(outDir.isDirectory()){
    			System.out.println("dir exists");
    		}else{
    			System.out.println("The same name file exists,can't create dir");
    		}
    		return false;
    	}else{
    		System.out.println("dir not exists,create it ...");
    		return outDir.mkdirs();
    		
    	}
	}
	
	public static boolean createFile(String path){
		String filePath = path;
    	File outFile = new File(path);
    	if(!outFile.exists()){
    		try {
				return outFile.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
		return false;
	}

}
