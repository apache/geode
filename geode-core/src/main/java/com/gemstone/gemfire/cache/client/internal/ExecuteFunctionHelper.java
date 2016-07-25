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
package com.gemstone.gemfire.cache.client.internal;

public class ExecuteFunctionHelper {

  public final static byte BUCKETS_AS_FILTER_MASK = 0x02;
  public final static byte IS_REXECUTE_MASK = 0x01;
  
  static byte createFlags(boolean executeOnBucketSet, byte isReExecute) {
    byte flags = executeOnBucketSet ? 
        (byte)(0x00 | BUCKETS_AS_FILTER_MASK) : 0x00;
    flags = isReExecute == 1? (byte)(flags | IS_REXECUTE_MASK) : flags;      
    return flags;
  }
}
