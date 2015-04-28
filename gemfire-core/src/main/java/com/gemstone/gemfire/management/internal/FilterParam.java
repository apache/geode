/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

public class FilterParam {

	private String[] inclusionList;
	private String[] exclusionList;

	private boolean isDefaultExcludeFilter = false;

	private boolean isDefaultIncludeFilter = false;

	private String DEFAULT_EXCLUDE_FILTER = "";

	private String DEFAULT_INCLUDE_FILTER = "";

	public FilterParam(String[] inclusionList, String[] exclusionList) {

		this.exclusionList = exclusionList;
		this.inclusionList = inclusionList;
		if (exclusionList.length == 1
				&& exclusionList[0].equals(DEFAULT_EXCLUDE_FILTER)) {
			isDefaultExcludeFilter = true;
		}
		if (inclusionList.length == 1
				&& inclusionList[0].equals(DEFAULT_INCLUDE_FILTER)) {
			isDefaultIncludeFilter = true;
		}

	}

	public boolean isDefaultExcludeFilter() {
		return isDefaultExcludeFilter;
	}

	public boolean isDefaultIncludeFilter() {
		return isDefaultIncludeFilter;
	}

	public void setDefaultExcludeFilter(boolean isDefaultExcludeFilter) {
		this.isDefaultExcludeFilter = isDefaultExcludeFilter;
	}

	public void setDefaultIncludeFilter(boolean isDefaultIncludeFilter) {
		this.isDefaultIncludeFilter = isDefaultIncludeFilter;
	}

	public String[] getInclusionList() {
		return inclusionList;
	}

	public String[] getExclusionList() {
		return exclusionList;
	}

}