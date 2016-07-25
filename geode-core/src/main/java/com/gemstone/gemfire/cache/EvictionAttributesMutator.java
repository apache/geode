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
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.internal.cache.EvictionAttributesImpl;

/**
 * The EvictionAttributesMutator allows changes to be made to a 
 * {@link com.gemstone.gemfire.cache.EvictionAttributes}. It is returned
 * by {@link com.gemstone.gemfire.cache.AttributesMutator#getEvictionAttributesMutator()}
 * @since GemFire 5.0
 */
public interface EvictionAttributesMutator
{
  /**
   * Sets the maximum value on the {@link EvictionAttributesImpl} that the given
   * {@link EvictionAlgorithm} uses to determine when to perform its
   * {@link EvictionAction}. The unit of the maximum value is determined by the
   * {@link EvictionAlgorithm}
   * 
   * @param maximum
   *          value used by the {@link EvictionAlgorithm}
   */
  public void setMaximum(int maximum);
}
