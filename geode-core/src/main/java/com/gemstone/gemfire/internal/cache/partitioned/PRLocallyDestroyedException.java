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

package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.internal.cache.DataLocationException;

/**
 * An exception indicating that a PartitionedRegion was found to be Locally
 * Destroyed
 * @since 5.1
 *
 */
public class PRLocallyDestroyedException extends DataLocationException {
  private static final long serialVersionUID = -1291911181409686840L;
  public PRLocallyDestroyedException(Throwable cause) {
    super();
    this.initCause(cause);
  }
  
  public PRLocallyDestroyedException(String message) {
    super(message);
  }
  
}
