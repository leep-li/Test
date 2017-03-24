package com.boslog.toimpala;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.log4j.Logger;

import com.boslog.util.DateTools;

public class insertImpalaCobSql {

	private static Logger logger = Logger.getLogger(InsertImpalaCobFunction.class);

	/**
	 * 插入cob_jobtimes表
	 * 
	 * @param cobJobTimes
	 */
	public void insertCobTimes(String cobJobTimes, List<List<String>> jobList, String date) {
		// Impala JDBC
		ImpalaUtil iu = new ImpalaUtil();
		Connection conn = null;
		Statement stmt = null;
		String SQL_QUERY = null;
		
		SQL_QUERY = "insert into cob_jobtimes (stage,cob_job_id,select_start,start_time,end_time,elapsed_time,total,throughtput,completed,selection_time,date_time)"
				+ "values ";
		
		if (cobJobTimes != null && !cobJobTimes.equals("")) {
			try {
				// for impala
				conn = iu.getConnection();
				stmt = conn.createStatement();
				// pstmt = conn.prepareStatement(SQL_QUERY.toString());
				// 遍历list
				for (int i = 1; i < jobList.size(); i++) {
					String stage = jobList.get(i).get(0).trim();
					String cob_job_id = jobList.get(i).get(1).trim();
					if(stage != null && !"".equals(stage) && cob_job_id != null && !"".equals(cob_job_id)){
						if(jobList.get(i).size()>=10){
							String select_start = jobList.get(i).get(2).trim();
							String start_time = jobList.get(i).get(3).trim();
							String end_time = jobList.get(i).get(4).trim();
							String total = jobList.get(i).get(6).trim();
							String throughtput = jobList.get(i).get(7).trim();
							String completed = jobList.get(i).get(8).trim();
							String selection_time = jobList.get(i).get(9).trim();
							int elapsed_time=0;
							if(null!=jobList.get(i).get(5)&&!"".equals(jobList.get(i).get(5).trim())){
								String[] elapsedTime = jobList.get(i).get(5).split(":");
								int hour = Integer.parseInt(elapsedTime[0].trim());
								int min = Integer.parseInt(elapsedTime[1].trim());
								int sec = Integer.parseInt(elapsedTime[2].trim());
								elapsed_time = hour * 3600 + min * 60 + sec;
							}
							/*SQL_QUERY = "insert into cob_jobtimes (stage,cob_job_id,select_start,start_time,end_time,elapsed_time,total,throughtput,completed,selection_time,date_time)"
									+ "values('" + stage + "','" + cob_job_id + "','" + select_start + "','" + start_time
									+ "','" + end_time + "'," + elapsed_time + ",'" + total + "','" + throughtput + "','"
									+ completed + "','" + selection_time + "','" + date + "')";*/

//							stmt.executeUpdate(SQL_QUERY);
							
							SQL_QUERY += "('" + stage + "','" + cob_job_id + "','" + select_start + "','" + start_time
									+ "','" + end_time + "'," + elapsed_time + ",'" + total + "','" + throughtput + "','"
									+ completed + "','" + selection_time + "','" + date + "'),";
						}

					} else
						break;
				}
				stmt.executeUpdate(SQL_QUERY.substring(0,SQL_QUERY.length()-1));
			} catch (Exception e) {
				logger.error("INSERT INTO DAY_INSERT_COBJOBTIMES_BTACHEXCEPTION"+e);
				e.printStackTrace();
			} finally {
				iu.release(conn, stmt);
			}
		}
	}

	/**
	 * 插入cob_reporttimes表
	 * 
	 * @param cobJobTimes
	 */
	public void insertCobReportTimes(String cobJobTimes, List<List<String>> reportList, String date) {
		// Impala JDBC
		ImpalaUtil iu = new ImpalaUtil();
		Connection conn = null;
		Statement stmt = null;
		String SQL_QUERY = null;
		
		SQL_QUERY = "insert into cob_reporttimes (stage,cob_job_id,start_time,end_time,elapsed_time,date_time)"
				+ "values ";

		if (cobJobTimes != null && !cobJobTimes.equals("")) {
			try {
				conn = iu.getConnection();
				stmt = conn.createStatement();
				for (int i = 1; i < reportList.size(); i++) {
					String stage = reportList.get(i).get(0).trim();
					String cob_job_id = reportList.get(i).get(1).trim();
					if (stage != null && !"".equals(stage) && cob_job_id != null && !"".equals(cob_job_id)) {
						String start_time = reportList.get(i).get(2).trim();
						String end_time = reportList.get(i).get(3).trim();
						int elapsed_time=0;
						if(null!=reportList.get(i).get(4)&&!"".equals(reportList.get(i).get(4).trim())){
							String[] elapsedTime = reportList.get(i).get(4).split(":");
							int hour = Integer.parseInt(elapsedTime[0].trim());
							int min = Integer.parseInt(elapsedTime[1].trim());
							int sec = Integer.parseInt(elapsedTime[2].trim());
							elapsed_time = hour * 3600 + min * 60 + sec;
						}
						/*SQL_QUERY = "insert into cob_reporttimes (stage,cob_job_id,start_time,end_time,elapsed_time,date_time)"
								+ "values('" + stage + "','" + cob_job_id + "','" + start_time + "','" + end_time + "',"
								+ elapsed_time + ",'" + date + "')";
						stmt.executeUpdate(SQL_QUERY);*/
						
						SQL_QUERY += "('" + stage + "','" + cob_job_id + "','" + start_time + "','" + end_time + "',"
								+ elapsed_time + ",'" + date + "'),";
						
					} else
						break;
				}
				
				stmt.executeUpdate(SQL_QUERY.substring(0,SQL_QUERY.length()-1));
				
			} catch (Exception e) {
				logger.error("INSERT INTO DAY_INSERT_COBREPORTTIMES_BTACH EXCEPTION"+e);
				e.printStackTrace();
			} finally {
				iu.release(conn, stmt);
				System.out.println("OKReport");
			}
		}
	}

	/**
	 * 插入cob_overview表
	 * 
	 * @param cobOverView
	 */
	public void insertCobOverView(String cobOverView, List<List<String>> overviewList, String date) {
		// Impala JDBC
		ImpalaUtil iu = new ImpalaUtil();
		Connection conn = null;
		Statement stmt = null;
		String SQL_QUERY = null;
		if (cobOverView != null && !cobOverView.equals("")) {
			try {
				// date formate
				SimpleDateFormat adf = new SimpleDateFormat("yyyy/MM/dd");
				Date startDate = adf.parse(overviewList.get(19).get(1).trim());
				Date endDate = adf.parse(overviewList.get(20).get(1).trim());
				int proceed_time = 0;
				adf = new SimpleDateFormat("yyyy-MM-dd");
				String start_time = adf.format(startDate) + " " + overviewList.get(19).get(2).trim();
				String end_time = adf.format(endDate) + " " + overviewList.get(20).get(2).trim();
				adf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				proceed_time = DateTools.getDiffTimeByMin(adf.parse(end_time), adf.parse(start_time));

				conn = iu.getConnection();
				stmt = conn.createStatement();

				SQL_QUERY = "insert into cob_overview (start_time,end_time,elapsed_time,date_time)" + "values('"
						+ start_time + "','" + end_time + "'," + proceed_time + ",'" + date + "')";
				stmt.executeUpdate(SQL_QUERY);

			} catch (Exception e) {
				logger.error("INSERT INTO DAY_INSERT_COBOVERVIEW_BTACH EXCEPTION"+e);
				e.printStackTrace();
			} finally {
				iu.release(conn, stmt);
			}
		}
	}

}
