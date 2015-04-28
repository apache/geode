/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.util;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.EvictionAttributes;

public class EvictionAttributesInfo implements Serializable{
	
	/**
	 * 
	 */
	private static final long	serialVersionUID	= 1L;
	private String evictionAction = "";
	private String evictionAlgorithm = "";
	private int  evictionMaxValue = 0;
	
	public EvictionAttributesInfo(EvictionAttributes ea) {
		EvictionAction evictAction = ea.getAction();
		
		if (evictAction != null) {
				evictionAction = evictAction.toString();
		}
		EvictionAlgorithm evictionAlgo = ea.getAlgorithm();
		if (evictionAlgo != null){
			evictionAlgorithm = evictionAlgo.toString();
		}
			
		evictionMaxValue = ea.getMaximum();
	}

	public String getEvictionAction() {
		return evictionAction;
	}

	public String getEvictionAlgorithm() {
		return evictionAlgorithm;
	}

	public int getEvictionMaxValue() {
		return evictionMaxValue;
	}

}
