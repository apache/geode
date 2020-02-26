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

package org.apache.geode.redis.internal;

import org.apache.geode.redis.internal.executor.AuthExecutor;
import org.apache.geode.redis.internal.executor.DBSizeExecutor;
import org.apache.geode.redis.internal.executor.DelExecutor;
import org.apache.geode.redis.internal.executor.EchoExecutor;
import org.apache.geode.redis.internal.executor.ExistsExecutor;
import org.apache.geode.redis.internal.executor.ExpireAtExecutor;
import org.apache.geode.redis.internal.executor.ExpireExecutor;
import org.apache.geode.redis.internal.executor.FlushAllExecutor;
import org.apache.geode.redis.internal.executor.KeysExecutor;
import org.apache.geode.redis.internal.executor.PExpireAtExecutor;
import org.apache.geode.redis.internal.executor.PExpireExecutor;
import org.apache.geode.redis.internal.executor.PTTLExecutor;
import org.apache.geode.redis.internal.executor.PersistExecutor;
import org.apache.geode.redis.internal.executor.PingExecutor;
import org.apache.geode.redis.internal.executor.QuitExecutor;
import org.apache.geode.redis.internal.executor.RenameExecutor;
import org.apache.geode.redis.internal.executor.ScanExecutor;
import org.apache.geode.redis.internal.executor.ShutDownExecutor;
import org.apache.geode.redis.internal.executor.TTLExecutor;
import org.apache.geode.redis.internal.executor.TimeExecutor;
import org.apache.geode.redis.internal.executor.TypeExecutor;
import org.apache.geode.redis.internal.executor.UnkownExecutor;
import org.apache.geode.redis.internal.executor.hash.HDelExecutor;
import org.apache.geode.redis.internal.executor.hash.HExistsExecutor;
import org.apache.geode.redis.internal.executor.hash.HGetAllExecutor;
import org.apache.geode.redis.internal.executor.hash.HGetExecutor;
import org.apache.geode.redis.internal.executor.hash.HIncrByExecutor;
import org.apache.geode.redis.internal.executor.hash.HIncrByFloatExecutor;
import org.apache.geode.redis.internal.executor.hash.HKeysExecutor;
import org.apache.geode.redis.internal.executor.hash.HLenExecutor;
import org.apache.geode.redis.internal.executor.hash.HMGetExecutor;
import org.apache.geode.redis.internal.executor.hash.HMSetExecutor;
import org.apache.geode.redis.internal.executor.hash.HScanExecutor;
import org.apache.geode.redis.internal.executor.hash.HSetExecutor;
import org.apache.geode.redis.internal.executor.hash.HSetNXExecutor;
import org.apache.geode.redis.internal.executor.hash.HValsExecutor;
import org.apache.geode.redis.internal.executor.hll.PFAddExecutor;
import org.apache.geode.redis.internal.executor.hll.PFCountExecutor;
import org.apache.geode.redis.internal.executor.hll.PFMergeExecutor;
import org.apache.geode.redis.internal.executor.list.LIndexExecutor;
import org.apache.geode.redis.internal.executor.list.LInsertExecutor;
import org.apache.geode.redis.internal.executor.list.LLenExecutor;
import org.apache.geode.redis.internal.executor.list.LPopExecutor;
import org.apache.geode.redis.internal.executor.list.LPushExecutor;
import org.apache.geode.redis.internal.executor.list.LPushXExecutor;
import org.apache.geode.redis.internal.executor.list.LRangeExecutor;
import org.apache.geode.redis.internal.executor.list.LRemExecutor;
import org.apache.geode.redis.internal.executor.list.LSetExecutor;
import org.apache.geode.redis.internal.executor.list.LTrimExecutor;
import org.apache.geode.redis.internal.executor.list.RPopExecutor;
import org.apache.geode.redis.internal.executor.list.RPushExecutor;
import org.apache.geode.redis.internal.executor.list.RPushXExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PsubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PublishExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PunsubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.SubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.UnsubscribeExecutor;
import org.apache.geode.redis.internal.executor.set.SAddExecutor;
import org.apache.geode.redis.internal.executor.set.SCardExecutor;
import org.apache.geode.redis.internal.executor.set.SDiffExecutor;
import org.apache.geode.redis.internal.executor.set.SDiffStoreExecutor;
import org.apache.geode.redis.internal.executor.set.SInterExecutor;
import org.apache.geode.redis.internal.executor.set.SInterStoreExecutor;
import org.apache.geode.redis.internal.executor.set.SIsMemberExecutor;
import org.apache.geode.redis.internal.executor.set.SMembersExecutor;
import org.apache.geode.redis.internal.executor.set.SMoveExecutor;
import org.apache.geode.redis.internal.executor.set.SPopExecutor;
import org.apache.geode.redis.internal.executor.set.SRandMemberExecutor;
import org.apache.geode.redis.internal.executor.set.SRemExecutor;
import org.apache.geode.redis.internal.executor.set.SScanExecutor;
import org.apache.geode.redis.internal.executor.set.SUnionExecutor;
import org.apache.geode.redis.internal.executor.set.SUnionStoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.GeoAddExecutor;
import org.apache.geode.redis.internal.executor.sortedset.GeoDistExecutor;
import org.apache.geode.redis.internal.executor.sortedset.GeoHashExecutor;
import org.apache.geode.redis.internal.executor.sortedset.GeoPosExecutor;
import org.apache.geode.redis.internal.executor.sortedset.GeoRadiusByMemberExecutor;
import org.apache.geode.redis.internal.executor.sortedset.GeoRadiusExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZAddExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZCardExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZCountExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZIncrByExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZLexCountExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRangeByLexExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRangeByScoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRangeExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRankExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemRangeByLexExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemRangeByRankExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemRangeByScoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRevRangeByScoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRevRangeExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRevRankExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZScanExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZScoreExecutor;
import org.apache.geode.redis.internal.executor.string.AppendExecutor;
import org.apache.geode.redis.internal.executor.string.BitCountExecutor;
import org.apache.geode.redis.internal.executor.string.BitOpExecutor;
import org.apache.geode.redis.internal.executor.string.BitPosExecutor;
import org.apache.geode.redis.internal.executor.string.DecrByExecutor;
import org.apache.geode.redis.internal.executor.string.DecrExecutor;
import org.apache.geode.redis.internal.executor.string.GetBitExecutor;
import org.apache.geode.redis.internal.executor.string.GetExecutor;
import org.apache.geode.redis.internal.executor.string.GetRangeExecutor;
import org.apache.geode.redis.internal.executor.string.GetSetExecutor;
import org.apache.geode.redis.internal.executor.string.IncrByExecutor;
import org.apache.geode.redis.internal.executor.string.IncrByFloatExecutor;
import org.apache.geode.redis.internal.executor.string.IncrExecutor;
import org.apache.geode.redis.internal.executor.string.MGetExecutor;
import org.apache.geode.redis.internal.executor.string.MSetExecutor;
import org.apache.geode.redis.internal.executor.string.MSetNXExecutor;
import org.apache.geode.redis.internal.executor.string.PSetEXExecutor;
import org.apache.geode.redis.internal.executor.string.SetBitExecutor;
import org.apache.geode.redis.internal.executor.string.SetEXExecutor;
import org.apache.geode.redis.internal.executor.string.SetExecutor;
import org.apache.geode.redis.internal.executor.string.SetNXExecutor;
import org.apache.geode.redis.internal.executor.string.SetRangeExecutor;
import org.apache.geode.redis.internal.executor.string.StrlenExecutor;
import org.apache.geode.redis.internal.executor.transactions.DiscardExecutor;
import org.apache.geode.redis.internal.executor.transactions.ExecExecutor;
import org.apache.geode.redis.internal.executor.transactions.MultiExecutor;
import org.apache.geode.redis.internal.executor.transactions.UnwatchExecutor;
import org.apache.geode.redis.internal.executor.transactions.WatchExecutor;

/**
 * The redis command type used by the server. Each command is directly from the redis protocol and
 * calling {@link #getExecutor()} on a type returns the executor class for that command.
 */
public enum RedisCommandType {

  /***************************************
   *************** Keys ******************
   ***************************************/

  /**
   * AUTH password
   * <p>
   * Authenticate to the server
   */
  AUTH {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new AuthExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * DEL key [key ...]
   * <p>
   * Delete a key
   */
  DEL {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new DelExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * EXISTS key
   * <p>
   * Determine if a key exists
   */
  EXISTS {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ExistsExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * EXPIRE key seconds
   * <p>
   * Set a key's time to live in seconds
   */
  EXPIRE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ExpireExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * EXPIREAT key timestamp
   * <p>
   * Set the expiration for a key as a UNIX timestamp
   */
  EXPIREAT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ExpireAtExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * FLUSHALL
   * <p>
   * Remove all keys from all databases
   */
  FLUSHALL {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new FlushAllExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * FLUSHDB
   * <p>
   * Remove all keys from the current database
   * <p>
   * Same as FLUSHALL for this implementation
   */
  FLUSHDB {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new FlushAllExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * KEYS pattern
   * <p>
   * Find all keys matching the given pattern
   */
  KEYS {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new KeysExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * PERSIST key
   * <p>
   * Remove the expiration from a key
   */
  PERSIST {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PersistExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * PEXPIRE key milliseconds
   * <p>
   * Set a key's time to live in milliseconds
   */
  PEXPIRE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PExpireExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * PEXPIREAT key milliseconds-timestamp
   * <p>
   * Set the expiration for a key as a UNIX timestamp specified in milliseconds
   */
  PEXPIREAT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PExpireAtExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * PTTL key
   * <p>
   * Get the time to live for a key in milliseconds
   */
  PTTL {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PTTLExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * RENAME key
   * <p>
   * Renames a key
   */
  RENAME {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new RenameExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SCAN cursor [MATCH pattern] [COUNT count]
   * <p>
   * Incrementally iterate the keys space
   */
  SCAN {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ScanExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * TTL key
   * <p>
   * Get the time to live for a key
   */
  TTL {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new TTLExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * TYPE key
   * <p>
   * Determine the type stored at key
   */
  TYPE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new TypeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /***************************************
   ************** Strings ****************
   ***************************************/

  /**
   * APPEND key value
   * <p>
   * Append a value to a key
   */
  APPEND {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new AppendExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * BITCOUNT key start end [start end ...]
   * <p>
   * Count set bits in a string
   */
  BITCOUNT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new BitCountExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * BITOP operation destkey key [key ...]
   * <p>
   * Perform bitwise operations between strings
   */
  BITOP {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new BitOpExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * BITPOS key bit [start] [end]
   * <p>
   * Find first bit set or clear in a string
   */
  BITPOS {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new BitPosExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * DECR key
   * <p>
   * Decrement the integer value of a key by one
   */
  DECR {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new DecrExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * DECRBY key decrement
   * <p>
   * Decrement the integer value of a key by the given number
   */
  DECRBY {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new DecrByExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * GET key
   * <p>
   * Get the value of a key
   */
  GET {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new GetExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * GETBIT key offset
   * <p>
   * Returns the bit value at offset in the string value stored at key
   */
  GETBIT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new GetBitExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * GETRANGE key start end
   * <p>
   * Get a substring of the string stored at a key
   */
  GETRANGE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new GetRangeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * GETSET key value
   * <p>
   * Set the string value of a key and return its old value
   */
  GETSET {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new GetSetExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * INCR key
   * <p>
   * Increment the integer value of a key by one
   */
  INCR {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new IncrExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * INCRBY key increment
   * <p>
   * Increment the integer value of a key by the given amount
   */
  INCRBY {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new IncrByExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * INCRBYFLOAT key increment
   * <p>
   * Increment the float value of a key by the given amount
   */
  INCRBYFLOAT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new IncrByFloatExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * MGET key [key ...]
   * <p>
   * Get the values of all the given keys
   */
  MGET {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new MGetExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * MSET key value [key value ...]
   * <p>
   * Set multiple keys to multiple values
   */
  MSET {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new MSetExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * MSETNX key value [key value ...]
   * <p>
   * Set multiple keys to multiple values, only if none of the keys exist
   */
  MSETNX {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new MSetNXExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * PSETEX key milliseconds value
   * <p>
   * Set the value and expiration in milliseconds of a key
   */
  PSETEX {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PSetEXExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SETEX key seconds value
   * <p>
   * Set the value and expiration of a key
   */
  SETEX {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SetEXExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SET key value [EX seconds] [PX milliseconds] [NX|XX]
   * <p>
   * Set the string value of a key
   */
  SET {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SetExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SETBIT key offset value
   * <p>
   * Sets or clears the bit at offset in the string value stored at key
   */
  SETBIT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SetBitExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SETNX key value
   * <p>
   * Set the value of a key, only if the key does not exist
   */
  SETNX {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SetNXExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SETRANGE key offset value
   * <p>
   * Overwrite part of a string at key starting at the specified offset
   */
  SETRANGE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SetRangeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * STRLEN key
   * <p>
   * Get the length of the value stored in a key
   */
  STRLEN {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new StrlenExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_STRING;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /***************************************
   **************** Hashes ***************
   ***************************************/
  /**
   * HDEL key field [field ...]
   * <p>
   * Delete one or more hash fields
   */
  HDEL {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HDelExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HEXISTS key field
   * <p>
   * Determine if a hash field exists
   */
  HEXISTS {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HExistsExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HGET key field
   * <p>
   * Get the value of a hash field
   */
  HGET {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HGetExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HGETALL key
   * <p>
   * Get all the fields and values in a hash
   */
  HGETALL {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HGetAllExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HINCRBY key field increment
   * <p>
   * Increment the integer value of a hash field by the given number
   */
  HINCRBY {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HIncrByExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HINCRBYFLOAT key field increment
   * <p>
   * Increment the float value of a hash field by the given amount
   */
  HINCRBYFLOAT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HIncrByFloatExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HKEYS key
   * <p>
   * Get all the fields in a hash
   */
  HKEYS {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HKeysExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HLEN key
   * <p>
   * Get the number of fields in a hash
   */
  HLEN {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HLenExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HMGET key field [field ...]
   * <p>
   * Get the values of all the given hash fields
   */
  HMGET {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HMGetExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HMSET key field value [field value ...]
   * <p>
   * Set multiple hash fields to multiple values
   */
  HMSET {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HMSetExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HSCAN key cursor [MATCH pattern] [COUNT count]
   * <p>
   * Incrementally iterate hash fields and associated values
   */
  HSCAN {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HScanExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HSET key field value
   * <p>
   * Set the string value of a hash field
   */
  HSET {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HSetExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HSETNX key field value
   * <p>
   * Set the value of a hash field, only if the field does not exist
   */
  HSETNX {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HSetNXExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * HVALS key
   * <p>
   * Get all the values in a hash
   */
  HVALS {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new HValsExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HASH;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /***************************************
   *********** HyperLogLogs **************
   ***************************************/

  /**
   * PFADD key element [element ...]
   * <p>
   * Adds the specified elements to the specified HyperLogLog
   */
  PFADD {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PFAddExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HLL;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * PFCOUNT key [key ...]
   * <p>
   * Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s)
   */
  PFCOUNT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PFCountExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HLL;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * PFMERGE destkey sourcekey [sourcekey ...]
   * <p>
   * Merge N different HyperLogLogs into a single one
   */
  PFMERGE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PFMergeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_HLL;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /***************************************
   *************** Lists *****************
   ***************************************/

  /**
   * LINDEX key index
   * <p>
   * Get an element from a list by its index
   */
  LINDEX {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new LIndexExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * LINSERT key BEFORE|AFTER pivot value
   * <p>
   * Insert an element before or after another element in a list
   */
  LINSERT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new LInsertExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * LLEN key
   * <p>
   * Get the length of a list
   */
  LLEN {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new LLenExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * LPOP key
   * <p>
   * Remove and get the first element in a list
   */
  LPOP {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new LPopExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * LPUSH key value [value ...]
   * <p>
   * Prepend one or multiple values to a list
   */
  LPUSH {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new LPushExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * LPUSHX key value
   * <p>
   * Prepend a value to a list, only if the list exists
   */
  LPUSHX {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new LPushXExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * LRANGE key start stop
   * <p>
   * Get a range of elements from a list
   */
  LRANGE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new LRangeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * LREM key count value
   * <p>
   * Remove elements from a list
   */
  LREM {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new LRemExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * LSET key index value
   * <p>
   * Set the value of an element in a list by its index
   */
  LSET {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new LSetExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * LTRIM key start stop
   * <p>
   * Trim a list to the specified range
   */
  LTRIM {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new LTrimExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * RPOP key
   * <p>
   * Remove and get the last element in a list
   */
  RPOP {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new RPopExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * RPUSH key value [value ...]
   * <p>
   * Append one or multiple values to a list
   */
  RPUSH {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new RPushExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * RPUSHX key value
   * <p>
   * Append a value to a list, only if the list exists
   */
  RPUSHX {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new RPushXExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_LIST;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /***************************************
   **************** Sets *****************
   ***************************************/

  /**
   * SADD key member [member ...]
   * <p>
   * Add one or more members to a set
   */
  SADD {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SAddExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SCARD key
   * <p>
   * Get the number of members in a set
   */
  SCARD {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SCardExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SDIFF key [key ...]
   * <p>
   * Subtract multiple sets
   */
  SDIFF {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SDiffExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SDIFFSTORE destination key [key ...]
   * <p>
   * Subtract multiple sets and store the resulting set in a key
   */
  SDIFFSTORE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SDiffStoreExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SISMEMBER key member
   * <p>
   * Determine if a given value is a member of a set
   */
  SISMEMBER {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SIsMemberExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SINTER key [key ...]
   * <p>
   * Intersect multiple sets
   */
  SINTER {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SInterExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SINTERSTORE destination key [key ...]
   * <p>
   * Intersect multiple sets and store the resulting set in a key
   */
  SINTERSTORE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SInterStoreExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SMEMBERS key
   * <p>
   * Get all the members in a set
   */
  SMEMBERS {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SMembersExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SMOVE source destination member
   * <p>
   * Move a member from one set to another
   */
  SMOVE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SMoveExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SPOP key
   * <p>
   * Remove and return a random member from a set
   */
  SPOP {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SPopExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SRANDMEMBER key [count]
   * <p>
   * Get one or multiple random members from a set
   */
  SRANDMEMBER {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SRandMemberExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SUNION key [key ...]
   * <p>
   * Add multiple sets
   */
  SUNION {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SUnionExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SUNIONSTORE destination key [key ...]
   * <p>
   * Add multiple sets and store the resulting set in a key
   */
  SUNIONSTORE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SUnionStoreExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SSCAN key cursor [MATCH pattern] [COUNT count]
   * <p>
   * Incrementally iterate Set elements
   */
  SSCAN {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SScanExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SREM key member [member ...]
   * <p>
   * Remove one or more members from a set
   */
  SREM {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SRemExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /***************************************
   ************* Sorted Sets *************
   ***************************************/

  /**
   * ZADD key score member [score member ...]
   * <p>
   * Add one or more members to a sorted set, or update its score if it already exists
   */
  ZADD {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZAddExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZCARD key
   * <p>
   * Get the number of members in a sorted set
   */
  ZCARD {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZCardExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZCOUNT key min max
   * <p>
   * Count the members in a sorted set with scores within the given values
   */
  ZCOUNT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZCountExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZINCRBY key increment member
   * <p>
   * Increment the score of a member in a sorted set
   */
  ZINCRBY {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZIncrByExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZLEXCOUNT key min max
   * <p>
   * Count the number of members in a sorted set between a given lexicographical range
   */
  ZLEXCOUNT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZLexCountExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZRANGE key start stop [WITHSCORES]
   * <p>
   * Return a range of members in a sorted set, by index
   */
  ZRANGE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZRangeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZRANGEBYLEX key min max [LIMIT offset count]
   * <p>
   * Return a range of members in a sorted set, by lexicographical range
   */
  ZRANGEBYLEX {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZRangeByLexExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
   * <p>
   * Return a range of members in a sorted set, by score
   */
  ZRANGEBYSCORE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZRangeByScoreExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZREVRANGE key start stop [WITHSCORES]
   * <p>
   * Return a range of members in a sorted set, by index, with scores ordered from high to low
   */
  ZREVRANGE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZRevRangeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZRANK key member
   * <p>
   * Determine the index of a member in a sorted set
   */
  ZRANK {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZRankExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZREM key member [member ...]
   * <p>
   * Remove one or more members from a sorted set
   */
  ZREM {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZRemExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZREMRANGEBYLEX key min max
   * <p>
   * Remove all members in a sorted set between the given lexicographical range
   */
  ZREMRANGEBYLEX {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZRemRangeByLexExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZREMRANGEBYRANK key start stop
   * <p>
   * Remove all members in a sorted set within the given indexes
   */
  ZREMRANGEBYRANK {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZRemRangeByRankExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZREMRANGEBYSCORE key min max
   * <p>
   * Remove all members in a sorted set within the given scores
   */
  ZREMRANGEBYSCORE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZRemRangeByScoreExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
   * <p>
   * Return a range of members in a sorted set, by score, with scores ordered from high to low
   */
  ZREVRANGEBYSCORE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZRevRangeByScoreExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZREVRANK key member
   * <p>
   * Determine the index of a member in a sorted set, with scores ordered from high to low
   */
  ZREVRANK {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZRevRankExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZSCAN key cursor [MATCH pattern] [COUNT count]
   * <p>
   * Incrementally iterate sorted sets elements and associated scores
   */
  ZSCAN {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZScanExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ZSCORE key member
   * <p>
   * Get the score associated with the given member in a sorted set
   */
  ZSCORE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ZScoreExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /***************************************
   ********** Publish Subscribe **********
   ***************************************/
  /**
   * SUBSCRIBE channel...
   * <p>
   * subscribe to channel
   */
  SUBSCRIBE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new SubscribeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_PUBSUB;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * PUBLISH channel message
   */
  PUBLISH {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PublishExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_PUBSUB;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * UNSUBSCRIBE channel...
   */
  UNSUBSCRIBE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new UnsubscribeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_PUBSUB;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * PSUBSCRIBE channel-pattern...
   * <p>
   * subscribe to channel
   */
  PSUBSCRIBE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PsubscribeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_PUBSUB;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * PUNSUBSCRIBE channel...
   */
  PUNSUBSCRIBE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PunsubscribeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_PUBSUB;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**************************************
   * Geospatial commands ****************
   **************************************/

  GEOADD {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new GeoAddExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  GEOHASH {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new GeoHashExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  GEOPOS {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new GeoPosExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  GEODIST {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new GeoDistExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  GEORADIUS {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new GeoRadiusExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  GEORADIUSBYMEMBER {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new GeoRadiusByMemberExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.REDIS_SORTEDSET;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /***************************************
   ************ Transactions *************
   ***************************************/

  /**
   * DISCARD
   * <p>
   * Discard all commands issued after MULTI
   */
  DISCARD {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new DiscardExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * EXEC
   * <p>
   * Execute all commands issued after MULTI
   */
  EXEC {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ExecExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * MULTI
   * <p>
   * Mark the start of a transaction block
   */
  MULTI {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new MultiExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * UNWATCH
   * <p>
   * Forget about all watched keys
   */
  UNWATCH {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new UnwatchExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * WATCH key [key ...]
   * <p>
   * Watch the given keys to determine execution of the MULTI/EXEC block
   */
  WATCH {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new WatchExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /***************************************
   *************** Server ****************
   ***************************************/

  /**
   * DBSIZE
   * <p>
   * Return the number of keys in the selected database
   */
  DBSIZE {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new DBSizeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * ECHO message
   * <p>
   * Echo the given string
   */
  ECHO {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new EchoExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * TIME
   * <p>
   * Return the current server time
   */
  TIME {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new TimeExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * PING
   * <p>
   * Ping the server
   */
  PING {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new PingExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * QUIT
   * <p>
   * Close the connection
   */
  QUIT {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new QuitExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  /**
   * SHUTDOWN
   * <p>
   * Shut down the server
   */
  SHUTDOWN {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new ShutDownExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  },

  // UNKNOWN
  UNKNOWN {
    private Executor executor;

    @Override
    public Executor getExecutor() {
      if (executor == null) {
        executor = new UnkownExecutor();
      }
      return executor;
    }

    private final RedisDataType dataType = RedisDataType.NONE;

    @Override
    public RedisDataType getDataType() {
      return this.dataType;
    }
  };

  /**
   * Abstract method overridden by each value in enum to get the executor associated with that
   * command type
   *
   * @return {@link Executor} for command type
   */
  public abstract Executor getExecutor();

  public abstract RedisDataType getDataType();
  /*
   * private RedisCommandType (RedisDataType dataType) { this.dataType = dataType; }
   */
}
