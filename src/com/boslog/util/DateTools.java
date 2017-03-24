package com.boslog.util;

import java.io.Serializable;
import java.util.Date;

public class DateTools implements Serializable {
	
	public static int getDiffTimeByMilSec(Date date1, Date date2) {
		
		return (int) (date1.getTime() - date2.getTime());
			
	}
	
	public static int getDiffTimeBySec(Date date1, Date date2) {
		
		return (int) ((date1.getTime() - date2.getTime())/1000);
			
	}
	
	public static int getDiffTimeByMin(Date date1, Date date2) {
		
		return (int) ((date1.getTime() - date2.getTime())/1000/60);
			
	}
	
	public static int getDiffTimeByHour(Date date1, Date date2) {
		
		return (int) ((date1.getTime() - date2.getTime())/1000/60/60);
			
	}
	
	public static long getDiffTimeByDay(Date date1, Date date2) {
		
		return (date1.getTime() - date2.getTime())/1000/60/60/24;
			
	}

}
