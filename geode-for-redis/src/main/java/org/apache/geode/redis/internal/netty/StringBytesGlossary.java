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
package org.apache.geode.redis.internal.netty;

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeImmutable;
import org.apache.geode.redis.internal.data.RedisSortedSet;

public class StringBytesGlossary {

  // ********** Single byte RedisResponse identifier constants **********
  /**
   * byte identifier of a bulk string
   */
  public static final byte BULK_STRING_ID = '$';

  /**
   * byte identifier of an array
   */
  public static final byte ARRAY_ID = '*';

  /**
   * byte identifier of a simple string
   */
  public static final byte SIMPLE_STRING_ID = '+';

  /**
   * byte identifier of an error
   */
  public static final byte ERROR_ID = '-';

  /**
   * byte identifier of an integer
   */
  public static final byte INTEGER_ID = ':';

  // ********** RedisResponse constants **********
  /**
   * byte array of an OK response
   */
  @MakeImmutable
  public static final byte[] OK = stringToBytes("+OK\r\n");

  /**
   * byte array of a nil response
   */
  @MakeImmutable
  public static final byte[] NIL = stringToBytes("$-1\r\n");

  /**
   * byte array of an empty array
   */
  @MakeImmutable
  public static final byte[] EMPTY_ARRAY = stringToBytes("*0\r\n");

  /**
   * byte array of an empty string
   */
  @MakeImmutable
  public static final byte[] EMPTY_STRING = stringToBytes("$0\r\n\r\n");

  @MakeImmutable
  public static final byte[] CRLF = stringToBytes("\r\n");

  @Immutable
  public static final byte[] ZERO_INT = stringToBytes(":0\r\n");

  @Immutable
  public static final byte[] ONE_INT = stringToBytes(":1\r\n");

  @MakeImmutable
  public static final byte[] ERR = stringToBytes("ERR ");

  @MakeImmutable
  public static final byte[] OOM = stringToBytes("OOM ");

  @MakeImmutable
  public static final byte[] WRONGTYPE = stringToBytes("WRONGTYPE ");

  @MakeImmutable
  public static final byte[] MOVED = stringToBytes("MOVED ");

  @MakeImmutable
  public static final byte[] BUSYKEY = stringToBytes("BUSYKEY ");

  @MakeImmutable
  public static final byte[] CROSSSLOT = stringToBytes("CROSSSLOT ");

  @MakeImmutable
  public static final byte[] WRONGPASS = stringToBytes("WRONGPASS ");

  @MakeImmutable
  public static final byte[] NOAUTH = stringToBytes("NOAUTH ");

  // ********** Redis Command constants **********

  // ClientExecutor
  @MakeImmutable
  public static final byte[] SETNAME = stringToBytes("SETNAME");
  @MakeImmutable
  public static final byte[] GETNAME = stringToBytes("GETNAME");

  // ClusterExecutor
  @MakeImmutable
  public static final byte[] INFO = stringToBytes("INFO");
  @MakeImmutable
  public static final byte[] SLOTS = stringToBytes("SLOTS");
  @MakeImmutable
  public static final byte[] NODES = stringToBytes("NODES");
  @MakeImmutable
  public static final byte[] KEYSLOT = stringToBytes("KEYSLOT");

  // ScanExecutor, HScanExecutor and SScanExecutor
  @MakeImmutable
  public static final byte[] MATCH = stringToBytes("MATCH");
  @MakeImmutable
  public static final byte[] COUNT = stringToBytes("COUNT");

  // PubSubExecutor
  @MakeImmutable
  public static final byte[] CHANNELS = stringToBytes("CHANNELS");
  @MakeImmutable
  public static final byte[] NUMSUB = stringToBytes("NUMSUB");
  @MakeImmutable
  public static final byte[] NUMPAT = stringToBytes("NUMPAT");

  // InfoExecutor
  @MakeImmutable
  public static final byte[] SERVER = stringToBytes("SERVER");
  @MakeImmutable
  public static final byte[] CLUSTER = stringToBytes("CLUSTER");
  @MakeImmutable
  public static final byte[] PERSISTENCE = stringToBytes("PERSISTENCE");
  @MakeImmutable
  public static final byte[] REPLICATION = stringToBytes("REPLICATION");
  @MakeImmutable
  public static final byte[] STATS = stringToBytes("STATS");
  @MakeImmutable
  public static final byte[] CLIENTS = stringToBytes("CLIENTS");
  @MakeImmutable
  public static final byte[] MEMORY = stringToBytes("MEMORY");
  @MakeImmutable
  public static final byte[] KEYSPACE = stringToBytes("KEYSPACE");
  @MakeImmutable
  public static final byte[] DEFAULT = stringToBytes("DEFAULT");
  @MakeImmutable
  public static final byte[] ALL = stringToBytes("ALL");

  // SlowlogExecutor and SlowlogParameterRequirements
  @MakeImmutable
  public static final byte[] GET = stringToBytes("GET");
  @MakeImmutable
  public static final byte[] LEN = stringToBytes("LEN");
  @MakeImmutable
  public static final byte[] RESET = stringToBytes("RESET");

  // ZAddExecutor and SetExecutor
  @MakeImmutable
  public static final byte[] NX = stringToBytes("NX");
  @MakeImmutable
  public static final byte[] XX = stringToBytes("XX");

  // ZAddExecutor
  @MakeImmutable
  public static final byte[] CH = stringToBytes("CH");
  @MakeImmutable
  public static final byte[] INCR = stringToBytes("INCR");

  // ZUnionStoreExecutor
  @MakeImmutable
  public static final byte[] WEIGHTS = stringToBytes("WEIGHTS");
  @MakeImmutable
  public static final byte[] AGGREGATE = stringToBytes("AGGREGATE");

  // SetExecutor
  @MakeImmutable
  public static final byte[] EX = stringToBytes("EX");
  @MakeImmutable
  public static final byte[] PX = stringToBytes("PX");

  // RestoreExecutor
  @MakeImmutable
  public static final byte[] REPLACE = stringToBytes("REPLACE");
  @MakeImmutable
  public static final byte[] ABSTTL = stringToBytes("ABSTTL");

  // Various ZRangeExecutors
  @MakeImmutable
  public static final byte[] WITHSCORES = stringToBytes("WITHSCORES");
  @MakeImmutable
  public static final byte[] LIMIT = stringToBytes("LIMIT");

  // LolWutExecutor
  @MakeImmutable
  public static final byte[] VERSION = stringToBytes("VERSION");

  // ********** Constants for Double Infinity comparisons **********
  public static final String P_INF_STRING = "+inf";
  public static final String INF_STRING = "inf";
  public static final String P_INFINITY_STRING = "+Infinity";
  public static final String INFINITY_STRING = "Infinity";
  public static final String N_INF_STRING = "-inf";
  public static final String N_INFINITY_STRING = "-Infinity";
  public static final String NaN_STRING = "NaN";

  @MakeImmutable
  public static final byte[] P_INF = stringToBytes(P_INF_STRING);

  @MakeImmutable
  public static final byte[] INF = stringToBytes(INF_STRING);

  @MakeImmutable
  public static final byte[] P_INFINITY = stringToBytes(P_INFINITY_STRING);

  @MakeImmutable
  public static final byte[] INFINITY = stringToBytes(INFINITY_STRING);

  @MakeImmutable
  public static final byte[] N_INF = stringToBytes(N_INF_STRING);

  @MakeImmutable
  public static final byte[] N_INFINITY = stringToBytes(N_INFINITY_STRING);

  @MakeImmutable
  public static final byte[] NaN = stringToBytes(NaN_STRING);

  // ********** Miscellaneous constants for convenience **********

  public static final String PING_RESPONSE_STRING = "PONG";

  @MakeImmutable
  public static final byte[] PING_RESPONSE = stringToBytes(PING_RESPONSE_STRING);

  @MakeImmutable
  public static final byte[] PING_RESPONSE_LOWERCASE =
      stringToBytes(PING_RESPONSE_STRING.toLowerCase());

  @MakeImmutable
  public static final byte[] RADISH_DUMP_HEADER = stringToBytes("RADISH");

  /**
   * These member names will always be evaluated to be "greater than" or "less than" any other when
   * using the {@link RedisSortedSet#checkDummyMemberNames(byte[], byte[])} method, so the rank of
   * an entry using these names will be less than or greater than all other members with the same
   * score.
   * These values should always be compared using {@code ==} rather than {@code Array.equals()} so
   * that we can differentiate between the use of these constants and a value potentially entered by
   * the user, which while equal in content, will not share the same memory address.
   */
  public static final byte[] GREATEST_MEMBER_NAME = new byte[] {-1};

  public static final byte[] LEAST_MEMBER_NAME = new byte[] {-2};

  public static final byte[] NEGATIVE_ZERO = stringToBytes("-0");

  ///////////////////// Response Message Types /////////////////////////

  @Immutable
  public static final byte[] SUBSCRIBE = Coder.stringToBytes("subscribe");

  @Immutable
  public static final byte[] PSUBSCRIBE = Coder.stringToBytes("psubscribe");

  @Immutable
  public static final byte[] MESSAGE = Coder.stringToBytes("message");

  @Immutable
  public static final byte[] PMESSAGE = Coder.stringToBytes("pmessage");

  @Immutable
  public static final byte[] UNSUBSCRIBE = Coder.stringToBytes("unsubscribe");

  @Immutable
  public static final byte[] PUNSUBSCRIBE = Coder.stringToBytes("punsubscribe");

}
