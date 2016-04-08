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
package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.management.internal.cli.domain.RegionInformation;

/**
 * Function that retrieves regions hosted on every member
 *
 */
public class GetRegionsFunction extends FunctionAdapter implements InternalEntity {

	/**
	 * 
	 */
	private static final long	serialVersionUID	= 1L;

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return GetRegionsFunction.class.toString();
	}
	
	@Override
	public void execute(FunctionContext functionContext) {
		try {
			
			Cache cache = CacheFactory.getAnyInstance();
			Set <Region<?,?>> regions = cache.rootRegions();
			
			if (regions.isEmpty() || regions == null) {
				functionContext.getResultSender().lastResult(null);
			} else {
				//Set<RegionInformation> regionInformationSet = RegionInformation.getRegionInformation(regions, true);
				Set<RegionInformation> regionInformationSet = new HashSet<RegionInformation>();
				
				for (Region<?,?> region : regions) {
				  RegionInformation regInfo = new RegionInformation(region, true);
				  regionInformationSet.add(regInfo);
				}
				functionContext.getResultSender().lastResult(regionInformationSet.toArray());
			}
		} catch (CacheClosedException e) {
			functionContext.getResultSender().sendException(e);
		} catch (Exception e) {
			functionContext.getResultSender().sendException(e);
		}
	}

}
