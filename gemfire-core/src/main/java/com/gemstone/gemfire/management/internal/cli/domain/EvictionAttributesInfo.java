/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.management.internal.cli.util.RegionAttributesDefault;
import com.gemstone.gemfire.management.internal.cli.util.RegionAttributesNames;

public class EvictionAttributesInfo implements Serializable{
	
	/**
	 * 
	 */
	private static final long	serialVersionUID	= 1L;
	private String evictionAction = "";
	private String evictionAlgorithm = "";
	private int  evictionMaxValue = 0;
	private Map<String, String> nonDefaultAttributes;
	
	public EvictionAttributesInfo(EvictionAttributes ea) {
		EvictionAction evictAction = ea.getAction();
		
		if (evictAction != null) {
				evictionAction = evictAction.toString();
		}
		EvictionAlgorithm evictionAlgo = ea.getAlgorithm();
		if (evictionAlgo != null){
			evictionAlgorithm = evictionAlgo.toString();
		}
		if (!EvictionAlgorithm.LRU_HEAP.equals(evictionAlgo)) {
	    evictionMaxValue = ea.getMaximum();
    }
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
	
	public boolean equals(Object obj) {
	  if (obj instanceof EvictionAttributesInfo) {
	    EvictionAttributesInfo their = (EvictionAttributesInfo) obj;
	    return this.evictionAction.equals(their.getEvictionAction()) 
	          && this.evictionAlgorithm.equals(their.getEvictionAlgorithm())
	          && this.evictionMaxValue == their.getEvictionMaxValue();
	  } else {
	    return false;
	  }
	}
	
	public int hashCode() {
	  return 42; // any arbitrary constant will do
	  
	}
	
	public Map<String, String> getNonDefaultAttributes() {
	  if (nonDefaultAttributes == null) {
	    nonDefaultAttributes = new HashMap<String, String>();
	  }
	  
	  if (this.evictionMaxValue != RegionAttributesDefault.EVICTION_MAX_VALUE) {
	    nonDefaultAttributes.put(RegionAttributesNames.EVICTION_MAX_VALUE, Long.toString(evictionMaxValue));
	  }
	  if (this.evictionAction != null && !this.evictionAction.equals(RegionAttributesDefault.EVICTION_ACTION)) {
	    nonDefaultAttributes.put(RegionAttributesNames.EVICTION_ACTION, this.evictionAction);
	  }
	  if (this.evictionAlgorithm != null && !this.evictionAlgorithm.equals(RegionAttributesDefault.EVICTION_ALGORITHM)) {
	    nonDefaultAttributes.put(RegionAttributesNames.EVICTION_ALGORITHM, this.evictionAlgorithm);
	  }
	  return nonDefaultAttributes;
	}
}
