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


