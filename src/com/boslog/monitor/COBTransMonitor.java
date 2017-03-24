package com.boslog.monitor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.boslog.fromexcel.ReadDataFromExcel;
import com.boslog.toimpala.InsertImpalaCobFunction;

public class COBTransMonitor {
	private static Logger logger = Logger.getLogger(COBTransMonitor.class);
	public static void main(String[] args) {
		logger.info("COB Start Succeess1");		
		File file=null;
		InputStream in=null;
		try {
			//get excel path
			Properties prop = new Properties();
			String property = System.getProperty("user.home");
			file = new File(property + "/cob/conf/mix_COB.properties");
			in = new FileInputStream(file);
			prop.load(in);
			String path = prop.getProperty("cob_excel_path");
			path=System.getProperty("user.home")+path;
			System.out.println(path);
			//read excel to list
			ReadDataFromExcel readExcel = new ReadDataFromExcel();
			List list = readExcel.readXls(path);
			//insert data into impala
			InsertImpalaCobFunction insertData=new InsertImpalaCobFunction();
			List<List<String>> overviewList=(List<List<String>>) list.get(0);
			List<List<String>> jobList=(List<List<String>>) list.get(1);
			List<List<String>> reportList=(List<List<String>>) list.get(2);
			insertData.insertTransDetailsHour(overviewList, jobList, reportList);
		} catch (Exception e) {
			logger.error("Send Data From EXECL TO Impala Exception"+e);
			e.printStackTrace();
		}finally {
			if(in!=null){
				try {
					in.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
