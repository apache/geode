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
package org.apache.geode.management.internal.cli.domain;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;

import java.io.Serializable;
import java.util.*;

/***
 * Gives the most basic common information of a region
 * Used by the GetRegionsFunction for 'list region' command
 *	since 7.0
 */
public class RegionInformation implements Serializable {
	
	private static final long	serialVersionUID	= 1L;
	protected String name;
	protected String path;
	protected Scope scope;
	protected DataPolicy dataPolicy;
	protected boolean isRoot;
	protected String parentRegion;
	
	private Set<RegionInformation> subRegionInformationSet = null;
	
	public RegionInformation(Region<?, ?> region, boolean recursive) {
		this.name = region.getFullPath().substring(1);
		this.path = region.getFullPath().substring(1);
		this.scope = region.getAttributes().getScope();
		this.dataPolicy = region.getAttributes().getDataPolicy();
		
		if (region.getParentRegion() == null) {
			this.isRoot = true;
			
			if (recursive) {
				Set<Region<?,?>> subRegions = region.subregions(recursive);
				subRegionInformationSet = getSubRegions(subRegions);
			}
		} else {
			this.isRoot = false;
			this.parentRegion = region.getParentRegion().getFullPath();
		}
	}
	
	private Set<RegionInformation> getSubRegions(Set<Region<?,?>> subRegions) {
	  Set<RegionInformation> subRegionInformation = new HashSet<RegionInformation>();
	  
	  for (Region<?,?> region : subRegions) {
	    RegionInformation regionInformation = new RegionInformation(region, false);
	    subRegionInformation.add(regionInformation);
	  }
	  
	  return subRegionInformation;
	}
	
	public Set<String> getSubRegionNames() {
	  Set<String> subRegionNames = new HashSet<String>();
	  
	  if (subRegionInformationSet != null) {
	    for (RegionInformation regInfo : subRegionInformationSet) {
	      subRegionNames.add(regInfo.getName());
	    }
	  }
	  
	  return subRegionNames;
	}

	public String getName() {
		return name;
	}
	
	public String getPath() {
		return path;
	}
	
	public Scope getScope() {
		return scope;
	}
	
	public DataPolicy getDataPolicy() {
		return dataPolicy;
	}
	
	public Set<RegionInformation> getSubRegionInformation() {
		return subRegionInformationSet;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RegionInformation) {
			RegionInformation regionInfoObj = (RegionInformation) obj;
			return this.name.equals(regionInfoObj.getName())
							&& this.path.equals(regionInfoObj.getPath())
							&& this.isRoot == regionInfoObj.isRoot
							&& this.dataPolicy.equals(regionInfoObj.getDataPolicy())
							&& this.scope.equals(regionInfoObj.getScope());
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return	this.name.hashCode()
						^ this.path.hashCode()
						^ this.dataPolicy.hashCode()
						^ this.scope.hashCode();
	}
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nName          :\t");
		sb.append(this.getName());
		sb.append("\nPath          :\t");
		sb.append(this.getPath());
		sb.append("\nScope         :\t");
		sb.append(this.getScope().toString());
		sb.append("\nData Policy   :\t");
		sb.append(this.getDataPolicy().toString());
		
		if (this.parentRegion != null) {
			sb.append("\nParent Region :\t");
			sb.append(this.parentRegion);
		}
		
		return sb.toString();
	}
	
	public String getSubRegionInfoAsString() {
		StringBuilder sb = new StringBuilder();
		if (this.isRoot) {
			
			for (RegionInformation regionInfo : this.subRegionInformationSet) {
				sb.append("\n");
				sb.append(regionInfo.getName());
			}
		}
		return sb.toString();
	}
}
