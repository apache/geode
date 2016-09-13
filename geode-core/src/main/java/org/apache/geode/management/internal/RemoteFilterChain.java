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
package org.apache.geode.management.internal;

import javax.management.ObjectName;

import org.apache.geode.distributed.DistributedMember;

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
