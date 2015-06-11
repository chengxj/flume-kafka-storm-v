package test;

import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import util.DateUtil;

public class App1 {

	public static void main(String[] args) throws Exception {
		String beginDate = "2015-06-10 12:26:19.955";
		String endDate = "2015-06-10 12:29:12.345";
		
		double bd = DateUtil.dateChangeSecond(beginDate);
		double ed = DateUtil.dateChangeSecond(endDate);
		double time = ed-bd;
		double ceil = DateUtil.secondCeil(time);
		
		System.out.println(ceil);
		

	}

}
