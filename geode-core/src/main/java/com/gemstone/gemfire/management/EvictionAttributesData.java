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
package com.gemstone.gemfire.management;

import java.beans.ConstructorProperties;

import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
/**
 * Composite data type used to distribute the eviction attributes for
 * a {@link Region}.
 * 
 * @since GemFire 7.0
 */

public class EvictionAttributesData {
  
  /**
   * Algorithm used for eviction
   */
  private String algorithm;

  /**
   * Maximum entries in Region before eviction starts
   */
  private Integer maximum;

  /**
   * Action to be taken if entries reaches maximum value
   */
  private String action;
  
  /**
   * 
   * This constructor is to be used by internal JMX framework only. User should
   * not try to create an instance of this class.
   */
  @ConstructorProperties({ "algorithm", "maximum", "action"

  })
  public EvictionAttributesData(String algorithm, Integer maximum, String action){
    this.algorithm = algorithm;
    this.maximum = maximum;
    this.action = action;
  }

  /**
   * Returns the algorithm (policy) used to determine which entries will be
   * evicted.
   */
  public String getAlgorithm() {
    return algorithm;
  }

  /**
   * The unit of this value is determined by the definition of the {@link EvictionAlgorithm} set by one of the creation
   * methods e.g. {@link EvictionAttributes#createLRUEntryAttributes()}
   *
   * For algorithm LRU HEAP null will be returned
   * @return maximum value used by the {@link EvictionAlgorithm} which determines when the {@link EvictionAction} is
   *         performed.
   */
  public Integer getMaximum() {
    return maximum;
  }

  /**
   * Returns the action that will be taken on entries that are evicted.
   */
  public String getAction() {
    return action;
  }

  /**
   * String representation of EvictionAttributesData
   */
  @Override
  public String toString() {

    final StringBuilder buffer = new StringBuilder(128);
    buffer.append("EvictionAttributesData [");

    buffer.append("algorithm=").append(this.getAlgorithm());
    if (!this.getAlgorithm().equals(EvictionAlgorithm.NONE.toString())) {
      buffer.append(", action=").append(this.getAction());
      if (!this.getAlgorithm().equals(EvictionAlgorithm.LRU_HEAP.toString())) {
        buffer.append(", maximum=").append(this.getMaximum());
      }
    }
    buffer.append("]");
    return buffer.toString();
  }


}
