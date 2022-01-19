/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.FixedPartitionAttributes;

public class FixedPartitionAttributesInfo implements Serializable {

  private static final long serialVersionUID = 1L;
  private final boolean isPrimary;
  private final String partitionName;
  private final int numBuckets;

  public FixedPartitionAttributesInfo(FixedPartitionAttributes fpa) {
    numBuckets = fpa.getNumBuckets();
    partitionName = fpa.getPartitionName();
    isPrimary = fpa.isPrimary();
  }

  public boolean equals(Object obj) {
    if (obj instanceof FixedPartitionAttributesInfo) {
      FixedPartitionAttributesInfo fpaInfo = (FixedPartitionAttributesInfo) obj;
      return numBuckets == fpaInfo.getNumBuckets()
          && StringUtils.equals(partitionName, fpaInfo.getPartitionName())
          && isPrimary == fpaInfo.isPrimary();

    } else {
      return false;
    }
  }

  public int hashCode() {
    return 42; // any arbitrary constant will do

  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public boolean isPrimary() {
    return isPrimary;
  }

}
