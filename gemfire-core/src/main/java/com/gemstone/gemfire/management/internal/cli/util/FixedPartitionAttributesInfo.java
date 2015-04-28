/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.util;

import java.io.Serializable;

import com.gemstone.gemfire.cache.FixedPartitionAttributes;

public class FixedPartitionAttributesInfo implements Serializable{
	private boolean isPrimary;
	private String partitionName;
	private int numBuckets;
	
	public FixedPartitionAttributesInfo (FixedPartitionAttributes fpa) {
		this.numBuckets = fpa.getNumBuckets();
		this.partitionName =fpa.getPartitionName();
		this.isPrimary = fpa.isPrimary();
	}
	
	public boolean equals (Object obj) {
		if (obj instanceof FixedPartitionAttributesInfo) {
			FixedPartitionAttributesInfo fpaInfo = (FixedPartitionAttributesInfo) obj;
			return this.numBuckets == fpaInfo.getNumBuckets() &&
					this.partitionName.equals(fpaInfo.getPartitionName()) &&
					this.isPrimary  == fpaInfo.isPrimary();
						
		} else {
			return false;
		}
	}
	
	public int getNumBuckets() {
		return this.numBuckets;
	}
	
	public String getPartitionName() {
		return this.partitionName;
	}
	
	public boolean isPrimary() {
		return this.isPrimary;
	}
	
  public int hashCode() {
    return 42; // any arbitrary constant will do

  }
	
}


