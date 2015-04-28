/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;


import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.management.internal.cli.domain.RegionDescriptionPerMember;

public class GetRegionDescriptionFunction extends FunctionAdapter implements
		InternalEntity {


	private static final long	serialVersionUID	= 1L;

	@Override
	public void execute(FunctionContext context) {
		String regionPath  = (String) context.getArguments();
		try {
			Cache cache = CacheFactory.getAnyInstance();
			Region<?, ?> region = cache.getRegion(regionPath);
			
			if (region != null) {
				String memberName = cache.getDistributedSystem().getDistributedMember().getName();
				RegionDescriptionPerMember regionDescription =  new RegionDescriptionPerMember(region, memberName);
				context.getResultSender().lastResult(regionDescription);
			} else {
				context.getResultSender().lastResult(null);
			}
		} catch (CacheClosedException e) {
			context.getResultSender().sendException(e);
		} catch (Exception e) {
			context.getResultSender().sendException(e);
		}
	}

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return GetRegionDescriptionFunction.class.toString();
	}

}
