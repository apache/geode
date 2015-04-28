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

import com.gemstone.gemfire.distributed.DistributedMember;

public class RemoteFilterChain extends FilterChain {

	private StringBasedFilter serverGroupFilter;
	private StringBasedFilter remoteMBeanFilter;
	private StringBasedFilter managedMemberFilter;

	public RemoteFilterChain(){
		
		
		/*String remoteMBeanExcludeFilter = managementConfig.getRemoteMBeanExcludeFilter();
		String remoteMBeanIncludeFilter = managementConfig.getRemoteMBeanIncludeFilter();

		FilterParam remoteMBeanParam = createFilterParam(remoteMBeanIncludeFilter, remoteMBeanExcludeFilter);
		remoteMBeanFilter = new StringBasedFilter(remoteMBeanParam);
		
		String memberNodeExcludeFilter = managementConfig.getMemberNodeExcludeFilter();
		String memberNodeIncludeFilter = managementConfig.getMemberNodeIncludeFilter();

		FilterParam memberNodeParam = createFilterParam(memberNodeIncludeFilter, memberNodeExcludeFilter);
		managedMemberFilter = new StringBasedFilter(memberNodeParam);
		
		String serverGroupExcludeFilter = managementConfig.getServerGroupExcludeFilter();
		String serverGroupIncludeFilter = managementConfig.getServerGroupIncludeFilter();

		FilterParam serverGroupParam = createFilterParam(serverGroupIncludeFilter, serverGroupExcludeFilter);
		serverGroupFilter = new StringBasedFilter(serverGroupParam);*/
		
	}

	public boolean isFiltered(ObjectName name, DistributedMember member,
			String serverGroup) {
	  return false;
	  // <For future use>
		/*isRemoteMBeanFiltered(name);
		isManagedNodeFiltered(member);
		isServerGroupFiltered(serverGroup);*/
		

	}
	public boolean isRemoteMBeanFiltered(ObjectName objectName) {

	  return false;
    // <For future use>
		/*boolean isExcluded = remoteMBeanFilter.isExcluded(objectName.getCanonicalName());
		boolean isIncluded = remoteMBeanFilter.isIncluded(objectName.getCanonicalName());

		return isFiltered(isIncluded, isExcluded);*/

	}

	public boolean isManagedNodeFiltered(DistributedMember member) {
	  return false;
    // <For future use>
		/*boolean isExcluded = managedMemberFilter.isExcluded(member.getId());
		boolean isIncluded = managedMemberFilter.isIncluded(member.getId());

		return isFiltered(isIncluded, isExcluded);*/

	}


	public boolean isServerGroupFiltered(String serverGroup) {
	  return false;
    // <For future use>
		/*boolean isExcluded = serverGroupFilter.isExcluded(serverGroup);
		boolean isIncluded = serverGroupFilter.isIncluded(serverGroup);

		return isFiltered(isIncluded, isExcluded);
*/
	}

	
}
