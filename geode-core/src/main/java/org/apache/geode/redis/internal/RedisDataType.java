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
package org.apache.geode.redis.internal;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.GeodeRedisServer;

/**
 * The RedisDataType enum contains the choices to which every {@link Region}
 * on the server must be. There is only one instance of {@link #REDIS_STRING}
 * and {@link #REDIS_PROTECTED} defined by {@link GeodeRedisServer#STRING_REGION} and
 * {@link GeodeRedisServer#REDIS_META_DATA_REGION} respectively.
 * <p>
 * The data types are:
 * <li>{@link RedisDataType#REDIS_STRING}</li>
 * <li>{@link RedisDataType#REDIS_HASH}</li>
 * <li>{@link RedisDataType#REDIS_LIST}</li>
 * <li>{@link RedisDataType#REDIS_SET}</li>
 * <li>{@link RedisDataType#REDIS_SORTEDSET}</li>
 * <li>{@link RedisDataType#REDIS_PROTECTED}</li>
 *
 */
public enum RedisDataType {
  /**
   * Strings Regions
   */
  REDIS_STRING {
    @Override
    public String toString() {
      return "string";
    }
  }, 

  /**
   * Hashes Regions
   */
  REDIS_HASH {
    @Override
    public String toString() {
      return "hash";
    }
  },

  /**
   * Lists Regions
   */
  REDIS_LIST {
    @Override
    public String toString() {
      return "list";
    }
  }, 

  /**
   * Sets Regions
   */
  REDIS_SET {
    @Override
    public String toString() {
      return "set";
    }
  }, 

  /**
   * SortedSets Regions
   */
  REDIS_SORTEDSET {
    @Override
    public String toString() {
      return "zset";
    }
  }, 

  /**
   * HyperLogLog Regions
   */
  REDIS_HLL {
    @Override
    public String toString() {
      return "hyperloglog";
    }
  },

  /**
   * Regions protected from overwrite or deletion
   */
  REDIS_PROTECTED {
    @Override
    public String toString() {
      return "protected";
    }
  },

  /**
   * None
   */
  NONE {
    @Override
    public String toString() {
      return "none";
    }
  };

};
