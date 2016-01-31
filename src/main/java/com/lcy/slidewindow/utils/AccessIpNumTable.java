package com.lcy.slidewindow.utils;

import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.lcy.slidewindow.bean.IpBean;

public class AccessIpNumTable {
	
		public static Logger LOG = Logger.getLogger(HBaseUtils.class);
		private String tableName;
		private HBaseUtils hBaseUtils;
		public AccessIpNumTable(HBaseUtils hBaseUtils,String tableName){
			this.hBaseUtils=hBaseUtils;
			this.tableName = tableName;
		}
		
		
		/**
		 * 将订单信息插入到hbase表中
		 * @param hBaseUtils
		 * @param ipbean
		 */
		public void insert(HBaseUtils hBaseUtils,IpBean ipbean) {
			if(null==ipbean){
				LOG.warn("insert hbase table: access_ip_num,content is null");
				return ;
			}
			HTableInterface table = hBaseUtils.getTable(tableName);
			try {
				Put p1 = new Put(Bytes.toBytes(ipbean.getIp()));
				p1.add(Bytes.toBytes("detail"), Bytes.toBytes("num"), Bytes.toBytes(ipbean.getNum()));
				table.put(p1);
				table.flushCommits();
			} catch (Throwable e) {
				LOG.error("error accurs while inserting order into hbase");
				LOG.error(ExceptionUtils.getFullStackTrace(e));
			} finally {
				try {
					table.close();
				} catch (IOException e) {
					LOG.error(String.valueOf(e));
				}
			}
		}	

}
