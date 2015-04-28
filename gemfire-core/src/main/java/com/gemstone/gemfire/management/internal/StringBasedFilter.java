/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.gemfire.management.ManagementException;

/**
 * This is a string pattern based Filter
 * with some limitations.
 * 
 * @author rishim
 *
 */

public class StringBasedFilter {
	
	private FilterParam params;

	private List<Pattern> exclusionPatternList;
	private List<Pattern> inclusionPatternList;

	/**
	 * 
	 */
	public StringBasedFilter(FilterParam params){
		this.params = params;
		exclusionPatternList = new ArrayList<Pattern>();
		inclusionPatternList = new ArrayList<Pattern>();

		compileFilterList(params.getExclusionList(), exclusionPatternList);
		compileFilterList(params.getInclusionList(), inclusionPatternList);
		
	}
	/**
	 * 
	 */

	public boolean isExcluded(String tokenToMatch) {
		if(params.isDefaultExcludeFilter()){
			return false;
		}
		tokenToMatch = formatStringTokens(tokenToMatch);
		for (Pattern pattern : exclusionPatternList) {
			Matcher matcher = pattern.matcher(tokenToMatch);
			if (matcher.find()) {
				return true;
			}
		}
		return false;
	}


	public boolean isIncluded(String tokenToMatch) {
		if(params.isDefaultIncludeFilter()){
			return true;
		}
		tokenToMatch = formatStringTokens(tokenToMatch);
		for (Pattern pattern : inclusionPatternList) {
			Matcher matcher = pattern.matcher(tokenToMatch);
			if (matcher.find()) {
				return true;
			}
		}
		return false;
	}

	private void compileFilterList(String[]  list, List<Pattern> patternList){

	
		for (String s : list) {
			try {
				s = formatStringTokens(s);
				Pattern pattern;
				if(s.contains("*")){
					pattern = Pattern.compile(s);
				}else{
					pattern = Pattern.compile(s,Pattern.LITERAL);
				}
				
				patternList.add(pattern);
			} catch (NullPointerException e) {
				throw new ManagementException(e);
			}
		}
	}
	
	
	private String formatStringTokens(String value) {

		value = value.replace('<', '-');
		value = value.replace('>', '-');
		value = value.replace('(', '-');
		value = value.replace(')', '-');

		if (value.length() < 1) {
			value = "nothing";
		}
		return value;
	}


}
