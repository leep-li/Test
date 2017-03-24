package com.boslog.toimpala;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.log4j.Logger;

@SuppressWarnings("all")
public class InsertImpalaCobFunction {
	private static Logger logger = Logger.getLogger(InsertImpalaCobFunction.class);
	public void insertTransDetailsHour(List overviewList,List jobList,List reportList ) {
		//get the date before today
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		Date sysDate=new Date();
		String date="";
		Calendar calendar=new GregorianCalendar();
		calendar.setTime(sysDate);
		calendar.add(calendar.DATE, -1);
		sysDate=calendar.getTime();
		date=sdf.format(sysDate);
		// Impala JDBC
		ImpalaUtil iu = new ImpalaUtil();
		Properties prop = new Properties();
		String property = null;
		String cobJobTimes = null;
		String cobReportTimes = null;
		String cobOverView = null;
		File file=null;
		InputStream in =null;
		try {
			//获取当前用户home目录
			property = System.getProperty("user.home");
			file = new File(property + "/cob/conf/mix_COB.properties");
			in = new FileInputStream(file);
			//加载参数
			prop.load(in);
			cobOverView = prop.getProperty("impala_cob_overview");
			cobJobTimes = prop.getProperty("impala_cob_jobtimes");
			cobReportTimes = prop.getProperty("impala_cob_reporttime");
			
			//插入Impala CobJobTimes的具体方法,可以为它传入你需要的参数
			insertImpalaCobSql iics = new insertImpalaCobSql();
			
			//同理插入Impala CobReportTimes
			iics.insertCobOverView(cobOverView,overviewList,date);
			
			//从cob_impala中取统计数据,Map可作为参数传入
			iics.insertCobTimes(cobJobTimes,jobList,date);
			
			//同理插入Impala CobReportTimes
			iics.insertCobReportTimes(cobReportTimes,reportList,date);
		} catch (Exception e) {
			try {
				logger.error("DAY_COB_BTACH IS ERROR"+e);
				e.printStackTrace();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}finally{
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
