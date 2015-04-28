/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

/**
 * This generic interface is a place holder for all
 * the generic methods that we want to invoke for Proxies
 * @author rishim
 *
 */
public interface ProxyInterface {
	
	  /**
	   * Last refreshed time for proxy
	   * @return last refreshed time
	   */
		public long getLastRefreshedTime();
		
		/**
		 * Sets the last refreshed time for the proxy
		 * @param lastRefreshedTime
		 */
		public void setLastRefreshedTime(long lastRefreshedTime);
		

}
