/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
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
 * @author bansods
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
