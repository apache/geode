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
 * Abstract class containing methods which will be accesed 
 * by both Local and Remote filter chains
 * 
 * @author rishim
 *
 */
public abstract class FilterChain {

	protected boolean isFiltered(boolean included, boolean excluded) {

		if (excluded && included) {
			return true;
		}
		if (!excluded && included) {
			return false;
		}
		if (excluded && !included) {
			return true;
		}
		if (!excluded && !included) {
			return false;
		}
		return false;
	}
	
	/**
	 * This method splits the specified filters to array of string objects
	 */
	protected FilterParam createFilterParam(String inclusionList, String exclusionList){
		
		String[] inclusionListArray = inclusionList.split(";");
		String[] exclusionListArray = exclusionList.split(";");
		return new FilterParam(inclusionListArray, exclusionListArray);
	}
	
	


}
