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
package org.apache.geode.redis.internal.executor.set;

import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.ExecutionHandlerContext;


/**
 * Utility class for interpreting and processing Redis Set data structure
 * 
 * @author Gregory Green
 *
 */
public class SetInterpreter {

  /**
   * <pre>
   * Default region name/key 
   * 
   *  SET_REGION_KEY =
    new ByteArrayWrapper(Coder.stringToBytes("RedisSET"))
   * </pre>
   */
  public static final ByteArrayWrapper SET_REGION_KEY =
      new ByteArrayWrapper(Coder.stringToBytes(GeodeRedisServer.SET_REGION));

  /**
   * Get the region used for the Region SET regions
   * 
   * @param context the execution context handler
   * @return the Redis SET region
   */
  public static Region<ByteArrayWrapper, Set<ByteArrayWrapper>> getRegion(
      ExecutionHandlerContext context) {
    return context.getRegionProvider().getSetRegion();
  }
}
