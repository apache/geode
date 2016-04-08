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

/**
 * Indicates a failure to perform a distributed operation on a Partitioned Region 
 * after multiple attempts.
 *
 * @since 5.1
 */
public class PartitionedRegionDistributionException extends
   CacheRuntimeException
{
  private static final long serialVersionUID = -3004093739855972548L;
   
  public PartitionedRegionDistributionException() {
    super();
  }

  public PartitionedRegionDistributionException(String msg) {
    super(msg);
  }

  public PartitionedRegionDistributionException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public PartitionedRegionDistributionException(Throwable cause) {
    super(cause);
  }
}
