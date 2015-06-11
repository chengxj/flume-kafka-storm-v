package entity;

import java.util.Date;

import util.DateUtil;

public class BillCharging {
	/**
	 * 主叫号码
	 */
	private String CALLING_NBR;
	/**
	 * 被叫号码
	 */
	private String CALLED_NBR;
	/**
	 * 计费时间
	 */
	private Date START_DATETIME;
	/**
	 * 挂机时间
	 */
	private Date END_DATETIME;
	/**
	 * 实际呼叫时长，单位秒
	 */
	private Number TICKET_DURATION;
	/**
	 * 计费呼叫时间，单位秒（取整）
	 */
	private Number BILLING_DURATION;
	/**
	 * sp_id
	 */
	private String SP_ID;
	/**
	 * 业务id
	 */
	private String SERVICE_ID;
	/**
	 * 类型id
	 */
	private String CLASS_ID;
	/**
	 * 拨打电话资费
	 */
	private Number BILLING_CHARGE;
	
	public String getCALLING_NBR() {
		return CALLING_NBR;
	}
	public void setCALLING_NBR(String cALLING_NBR) {
		CALLING_NBR = cALLING_NBR;
	}
	public String getCALLED_NBR() {
		return CALLED_NBR;
	}
	public void setCALLED_NBR(String cALLED_NBR) {
		CALLED_NBR = cALLED_NBR;
	}
	public Date getSTART_DATETIME() {
		return START_DATETIME;
	}
	public void setSTART_DATETIME(Date sTART_DATETIME) {
		START_DATETIME = sTART_DATETIME;
	}
	public Date getEND_DATETIME() {
		return END_DATETIME;
	}
	public void setEND_DATETIME(Date eND_DATETIME) {
		END_DATETIME = eND_DATETIME;
	}
	public Number getTICKET_DURATION() {
		return TICKET_DURATION;
	}
	public void setTICKET_DURATION(Date sTART_DATETIME,Date eND_DATETIME) throws Exception {
		double ticketTime = DateUtil.ticketTime(sTART_DATETIME, eND_DATETIME);
		TICKET_DURATION = ticketTime;
	}
	public Number getBILLING_DURATION() {
		return BILLING_DURATION;
	}
	public void setBILLING_DURATION(Date sTART_DATETIME,Date eND_DATETIME) throws Exception {
		double ticketTime = DateUtil.ticketTime(sTART_DATETIME, eND_DATETIME);
		double secondCeil = DateUtil.secondCeil(ticketTime);
		BILLING_DURATION = secondCeil;
	}
	public String getSP_ID() {
		return SP_ID;
	}
	public void setSP_ID(String sP_ID) {
		SP_ID = sP_ID;
	}
	public String getSERVICE_ID() {
		return SERVICE_ID;
	}
	public void setSERVICE_ID(String sERVICE_ID) {
		SERVICE_ID = sERVICE_ID;
	}
	public String getCLASS_ID() {
		return CLASS_ID;
	}
	public void setCLASS_ID(String cLASS_ID) {
		CLASS_ID = cLASS_ID;
	}
	public Number getBILLING_CHARGE() {
		return BILLING_CHARGE;
	}
	public void setBILLING_CHARGE(Number bILLING_CHARGE) {
		BILLING_CHARGE = bILLING_CHARGE;
	}
	
}
