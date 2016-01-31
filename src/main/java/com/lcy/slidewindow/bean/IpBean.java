package com.lcy.slidewindow.bean;

import java.io.Serializable;

public class IpBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3496296403180240399L;
	private String ip;
	private Integer num;
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public Integer getNum() {
		return num;
	}
	public void setNum(Integer num) {
		this.num = num;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return ip + "  " + num;
	}
	
}
