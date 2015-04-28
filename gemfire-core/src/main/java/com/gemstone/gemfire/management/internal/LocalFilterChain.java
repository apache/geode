/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import javax.management.ObjectName;

import com.gemstone.gemfire.management.ManagementException;



/**
 * This chain currently has only one filter.
 * Have made it a chain to support future filters
 * which should be evaluated locally before registering 
 * an MBean
 * @author rishim
 *
 */

public class LocalFilterChain extends FilterChain{

	private StringBasedFilter localMBeanFilter;

	public LocalFilterChain(){
/*		
		String excludeFilter = managementConfig.getLocalMBeanExcludeFilter();
		String includeFilter = managementConfig.getLocalMBeanIncludeFilter();
		FilterParam param = createFilterParam(includeFilter, excludeFilter);
		localMBeanFilter = new StringBasedFilter(param);*/
		
	}



	public boolean isFiltered(ObjectName objectName) {
	  
	  return false;
	  
	  // For future use of filters
	  
		/*boolean isExcluded = localMBeanFilter.isExcluded(objectName.getCanonicalName());
		boolean isIncluded = localMBeanFilter.isIncluded(objectName.getCanonicalName());

		return isFiltered(isIncluded, isExcluded);*/
	}



}
