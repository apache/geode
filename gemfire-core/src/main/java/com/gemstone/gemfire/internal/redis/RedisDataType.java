package com.gemstone.gemfire.internal.redis;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.redis.GemFireRedisServer;

/**
 * The RedisDataType enum contains the choices to which every {@link Region}
 * on the server must be. There is only one instance of {@link #REDIS_STRING}
 * and {@link #REDIS_PROTECTED} defined by {@link GemFireRedisServer#STRING_REGION} and
 * {@link GemFireRedisServer#REDIS_META_DATA_REGION} respectively. 
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