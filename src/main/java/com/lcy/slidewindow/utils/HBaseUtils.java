package com.lcy.slidewindow.utils;

import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

/**
 * lichangyue
 */
public class HBaseUtils {
	public static Logger LOG = Logger.getLogger(HBaseUtils.class);

	private static Configuration config;
	private static HConnection connection;

	
	public HBaseUtils(final String zookeeper, final String zkRoot) {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", zookeeper);
		config.set("zookeeper.znode.parent", zkRoot);
	}

	public HTableInterface getTable(String tablename) {
		try {
			if (connection == null) {
				connection = HConnectionManager.createConnection(config);
			}
			return connection.getTable(tablename);
		} catch (IOException e) {
			LOG.error(ExceptionUtils.getFullStackTrace(e));
			LOG.error("exception accurs when getting table from hconnection, will be retried");
			try {
				connection = HConnectionManager.createConnection(config);
				return connection.getTable(tablename);
			} catch (Throwable throwable) {
				throw new RuntimeException(throwable);
			}
		}
	}

}
