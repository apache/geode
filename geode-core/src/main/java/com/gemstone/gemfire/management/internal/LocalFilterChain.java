/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal;

import javax.management.ObjectName;

import com.gemstone.gemfire.management.ManagementException;



/**
 * This chain currently has only one filter.
 * Have made it a chain to support future filters
 * which should be evaluated locally before registering 
 * an MBean
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
