package com.llw.mobileoperators.mr;

import java.text.ParseException;

import org.apache.hadoop.io.Text;

/*
 * 用于处理输入日志的分析工作，输入格式如下：
 * 0000000000|00000133|00-05|1.9333334
 * 0000000000|00000158|00-05|7.45
 * 0000000000|00000041|00-05|44.100002
 * 0000000000|00000194|00-05|57.1
 */
public class FirstThreeTable {

	private String imsi;//用户名
	private String position;//基站名
	private String timePeriod;//时间段
	private String timeDuration;//停留时长
	//解析传递过来的字符串
	public void parseLine(String line) throws LineException{
		try {
			String[] splitLine = line.split("\\|");
			this.imsi = splitLine[0];
			this.position = splitLine[1];
			this.timePeriod = splitLine[2];
			this.timeDuration = splitLine[3];
		} catch (Exception e) {
			throw new LineException("", 0);
		}
	}
	/*
	 * 输出KEY
	 */
	public Text outKey() {
		return new Text( this.imsi + "|" +this.timePeriod);
	}
	
	/*
	 * 输出VALUE
	 */
	public Text outValue() {
		return new Text( this.position + "|" +this.timeDuration);
	}
	public String getImsi() {
		return imsi;
	}
	public void setImsi(String imsi) {
		this.imsi = imsi;
	}
	public String getPosition() {
		return position;
	}
	public void setPosition(String position) {
		this.position = position;
	}
	public String getTimePeriod() {
		return timePeriod;
	}
	public void setTimePeriod(String timePeriod) {
		this.timePeriod = timePeriod;
	}
	public String getTimeDuration() {
		return timeDuration;
	}
	public void setTimeDuration(String timeDuration) {
		this.timeDuration = timeDuration;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
