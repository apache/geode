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

package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * Thrown if statistics are requested when statistics are disabled on the
 * region.
 *
 *
 *
 * @see com.gemstone.gemfire.cache.AttributesFactory#setStatisticsEnabled(boolean)
 * @see com.gemstone.gemfire.cache.RegionAttributes#getStatisticsEnabled
 * @see com.gemstone.gemfire.cache.Region#getStatistics
 * @see com.gemstone.gemfire.cache.Region.Entry#getStatistics
 * @since GemFire 3.0
 */
public class InternalStatisticsDisabledException extends GemFireCheckedException {
private static final long serialVersionUID = 4146181546364258311L;
  
  /**
   * Creates a new instance of <code>StatisticsDisabledException</code> without detail message.
   */
  public InternalStatisticsDisabledException() {
  }
  
  
  /**
   * Constructs an instance of <code>StatisticsDisabledException</code> with the specified detail message.
   * @param msg the detail message
   */
  public InternalStatisticsDisabledException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>StatisticsDisabledException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public InternalStatisticsDisabledException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>StatisticsDisabledException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public InternalStatisticsDisabledException(Throwable cause) {
    super(cause);
  }
}
