package com.gemstone.gemfire.internal.tools.gfsh.app.cache;

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.text.MessageFormat;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheExistsException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.internal.OSProcess;

@SuppressWarnings("deprecation")
public class CacheBase 
{
	/**
	 * The system region that holds system-level data for configuration
	 * and real-time state updates. 
	 */
	public final static String PROPERTY_SYSTEM_REGION_PATH = "systemRegionPath";
	
	protected DistributedSystem distributedSystem;
	protected Cache cache;
	protected LogWriter logger;

	public static void startLocator(String address, int port, String logFile)
			throws IOException 
	{
		InetAddress inetAddress = InetAddress.getByName(address);
		Locator.startLocatorAndDS(port, new File(logFile), inetAddress, new Properties());
	}

	public CacheBase()
	{
	}
	
	protected void initializeCache() throws CacheException, IOException {
		
		try {
			InstantiatorClassLoader.loadDataSerializables();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		open();
	}
  
  protected void initializeLogStatsResources() {
    String gemfirePropertyFile = System.getProperty("gemfirePropertyFile");
    Properties props = new Properties();
    try {
      if (gemfirePropertyFile != null) {
        File propFile = new File(gemfirePropertyFile);
        if (propFile.exists()) {
          FileInputStream fis = new FileInputStream(gemfirePropertyFile);
          props.load(fis);
          fis.close();
        }
      }
    } catch (IOException e) {
      //Ignore here.
    }
    
    String pid = String.valueOf(OSProcess.getId());

    String logFile = System.getProperty("gemfire.log-file");
    if (logFile == null) {
      logFile = props.getProperty("log-file");
      if (logFile == null) {
        String gfshLogFileFormat = System.getProperty("gfsh.log-file-format");
        logFile = MessageFormat.format(gfshLogFileFormat, new Object[] { pid });
        System.setProperty("gemfire.log-file", logFile);
      }
    }

    String statArchive = System.getProperty("gemfire.statistic-archive-file");
    if (statArchive == null) {
        statArchive = props.getProperty("statistic-archive-file");
      if (statArchive == null) {
        String gfshLogFileFormat = System.getProperty("gfsh.stat-file-format");
        statArchive = MessageFormat.format(gfshLogFileFormat, new Object[] { pid });
        System.setProperty("gemfire.statistic-archive-file", statArchive);
      }
    }
  }
	
	protected void open() throws IOException {
		// Create distributed system properties
		Properties properties = new Properties();

		// Connect to the distributed system
		distributedSystem = DistributedSystem.connect(properties);

		try {
			// Create cache
			cache = CacheFactory.create(distributedSystem);
			cache.setLockLease(10); // 10 second time out
			
			int bridgeServerPort = Integer.getInteger("bridgeServerPort", 0).intValue();
			String groups = System.getProperty("serverGroups");
			String[] serverGroups = null;
			if (groups != null) {
				serverGroups = groups.split(",");
			}
			if (bridgeServerPort != 0) {
				cache.setIsServer(true);
				com.gemstone.gemfire.cache.server.CacheServer server = cache.addCacheServer();
				server.setPort(bridgeServerPort);
				server.setNotifyBySubscription(true);
				server.setGroups(serverGroups);
				server.start();
			}
		} catch (CacheExistsException ex) {
			cache = CacheFactory.getAnyInstance();
		}
		if (cache != null) {
			logger = cache.getLogger();
		}
	}
	
	protected void close()
	{
		if (cache != null) {
			cache.close();
		}
	}
	
	public DistributedSystem getDistributedSystem()
	{
		return distributedSystem;
	}
	
	public Cache getGemFireCache()
	{
		return cache;
	}

//	Findbugs - wait not in loop - also seems unused code
	public void waitForever() throws InterruptedException {
		Object obj = new Object();
		synchronized (obj) {
			obj.wait();
		}
	}
	
	public LogWriter getLogger()
	{
		return cache.getLogger();
	}

	public static void main(String[] args) throws Exception {
		CacheBase base = new CacheBase();
		base.initializeCache();
	}

	public Cache getCache()
	{
		return cache;
	}
	
}
