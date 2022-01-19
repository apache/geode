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
package org.apache.geode.management;

import java.beans.ConstructorProperties;

import org.apache.geode.cache.Region;

/**
 * Composite date type used to distribute the fixed partition attributes for a {@link Region}.
 *
 * @since GemFire 7.0
 */
public class FixedPartitionAttributesData {

  /**
   * Name of the Fixed partition
   */
  private final String name;

  /**
   * whether this is the primary partition
   */
  private final boolean primary;

  /**
   * Number of buckets in the partition
   */
  private final int numBucket;

  /**
   *
   * This constructor is to be used by internal JMX framework only. User should not try to create an
   * instance of this class.
   */
  @ConstructorProperties({"name", "primary", "numBucket"

  })
  public FixedPartitionAttributesData(String name, boolean primary, int numBucket) {
    this.name = name;
    this.primary = primary;
    this.numBucket = numBucket;
  }

  /**
   * Returns the name of the partition.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns whether this member is the primary for the partition.
   *
   * @return True if this member is the primary, false otherwise.
   */
  public boolean isPrimary() {
    return primary;
  }

  /**
   * Returns the number of buckets allowed for the partition.
   */
  public int getNumBucket() {
    return numBucket;
  }

  /**
   * String representation of FixedPartitionAttributesData
   */
  @Override
  public String toString() {
    return "FixedPartitionAttributesData [name=" + name + ", numBucket=" + numBucket + ", primary="
        + primary + "]";
  }


}
