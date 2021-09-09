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

import org.apache.geode.annotations.internal.MakeImmutable;
import org.apache.geode.redis.internal.data.RedisSortedSet;

/**
 * Important note
 * <p>
 * Do not use '' <-- java primitive chars. Redis uses {@link Coder#CHARSET} encoding so we should
 * not risk java handling char to byte conversions, rather just hard code {@link Coder#CHARSET}
 * chars as bytes
 */
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

  /**
   * byte identifier of an left paren
   */
  public static final byte LEFT_BRACE_ID = '{';

  /**
   * byte identifier of an right paren
   */
  public static final byte RIGHT_BRACE_ID = '}';

  // ********** RedisResponse constants **********
  /**
   * byte array of an OK response
   */
  @MakeImmutable
  public static final byte[] bOK = stringToBytes("+OK\r\n");

  /**
   * byte array of a nil response
   */
  @MakeImmutable
  public static final byte[] bNIL = stringToBytes("$-1\r\n");

  /**
   * byte array of an empty array
   */
  @MakeImmutable
  public static final byte[] bEMPTY_ARRAY = stringToBytes("*0\r\n");

  /**
   * byte array of an empty string
   */
  @MakeImmutable
  public static final byte[] bEMPTY_STRING = stringToBytes("$0\r\n\r\n");

  @MakeImmutable
  public static final byte[] bCRLF = stringToBytes("\r\n");

  @MakeImmutable
  public static final byte[] bERR = stringToBytes("ERR ");

  @MakeImmutable
  public static final byte[] bOOM = stringToBytes("OOM ");

  @MakeImmutable
  public static final byte[] bWRONGTYPE = stringToBytes("WRONGTYPE ");

  @MakeImmutable
  public static final byte[] bMOVED = stringToBytes("MOVED ");

  @MakeImmutable
  public static final byte[] bBUSYKEY = stringToBytes("BUSYKEY ");

  @MakeImmutable
  public static final byte[] bCROSSSLOT = stringToBytes("CROSSSLOT ");

  // ********** Redis Command constants **********

  // ClusterExecutor
  @MakeImmutable
  public static final byte[] bINFO = stringToBytes("INFO");
  @MakeImmutable
  public static final byte[] bSLOTS = stringToBytes("SLOTS");
  @MakeImmutable
  public static final byte[] bNODES = stringToBytes("NODES");
  @MakeImmutable
  public static final byte[] bKEYSLOT = stringToBytes("KEYSLOT");

  // ScanExecutor, HScanExecutor and SScanExecutor
  @MakeImmutable
  public static final byte[] bMATCH = stringToBytes("MATCH");
  @MakeImmutable
  public static final byte[] bCOUNT = stringToBytes("COUNT");

  // PubSubExecutor
  @MakeImmutable
  public static final byte[] bCHANNELS = stringToBytes("CHANNELS");
  @MakeImmutable
  public static final byte[] bNUMSUB = stringToBytes("NUMSUB");
  @MakeImmutable
  public static final byte[] bNUMPAT = stringToBytes("NUMPAT");

  // InfoExecutor
  @MakeImmutable
  public static final byte[] bSERVER = stringToBytes("SERVER");
  @MakeImmutable
  public static final byte[] bCLUSTER = stringToBytes("CLUSTER");
  @MakeImmutable
  public static final byte[] bPERSISTENCE = stringToBytes("PERSISTENCE");
  @MakeImmutable
  public static final byte[] bREPLICATION = stringToBytes("REPLICATION");
  @MakeImmutable
  public static final byte[] bSTATS = stringToBytes("STATS");
  @MakeImmutable
  public static final byte[] bCLIENTS = stringToBytes("CLIENTS");
  @MakeImmutable
  public static final byte[] bMEMORY = stringToBytes("MEMORY");
  @MakeImmutable
  public static final byte[] bKEYSPACE = stringToBytes("KEYSPACE");
  @MakeImmutable
  public static final byte[] bDEFAULT = stringToBytes("DEFAULT");
  @MakeImmutable
  public static final byte[] bALL = stringToBytes("ALL");

  // SlowlogExecutor and SlowlogParameterRequirements
  @MakeImmutable
  public static final byte[] bGET = stringToBytes("GET");
  @MakeImmutable
  public static final byte[] bLEN = stringToBytes("LEN");
  @MakeImmutable
  public static final byte[] bRESET = stringToBytes("RESET");

  // ZAddExecutor and SetExecutor
  @MakeImmutable
  public static final byte[] bNX = stringToBytes("NX");
  @MakeImmutable
  public static final byte[] bXX = stringToBytes("XX");

  // ZAddExecutor
  @MakeImmutable
  public static final byte[] bCH = stringToBytes("CH");
  @MakeImmutable
  public static final byte[] bINCR = stringToBytes("INCR");

  // ZUnionStoreExecutor
  @MakeImmutable
  public static final byte[] bWEIGHTS = stringToBytes("WEIGHTS");
  @MakeImmutable
  public static final byte[] bAGGREGATE = stringToBytes("AGGREGATE");

  // SetExecutor
  @MakeImmutable
  public static final byte[] bEX = stringToBytes("EX");
  @MakeImmutable
  public static final byte[] bPX = stringToBytes("PX");

  // RestoreExecutor
  @MakeImmutable
  public static final byte[] bREPLACE = stringToBytes("REPLACE");
  @MakeImmutable
  public static final byte[] bABSTTL = stringToBytes("ABSTTL");

  // Various ZRangeExecutors
  @MakeImmutable
  public static final byte[] bWITHSCORES = stringToBytes("WITHSCORES");
  @MakeImmutable
  public static final byte[] bLIMIT = stringToBytes("LIMIT");
  public static final byte bPLUS = SIMPLE_STRING_ID; // +
  public static final byte bMINUS = ERROR_ID; // -

  public static final byte[] bZERO = stringToBytes("0");

  // ********** Constants for Double Infinity comparisons **********
  public static final String P_INF = "+inf";
  public static final String INF = "inf";
  public static final String P_INFINITY = "+Infinity";
  public static final String INFINITY = "Infinity";
  public static final String N_INF = "-inf";
  public static final String N_INFINITY = "-Infinity";
  public static final String NaN = "NaN";

  @MakeImmutable
  public static final byte[] bP_INF = stringToBytes(P_INF);

  @MakeImmutable
  public static final byte[] bINF = stringToBytes(INF);

  @MakeImmutable
  public static final byte[] bP_INFINITY = stringToBytes(P_INFINITY);

  @MakeImmutable
  public static final byte[] bINFINITY = stringToBytes(INFINITY);

  @MakeImmutable
  public static final byte[] bN_INF = stringToBytes(N_INF);

  @MakeImmutable
  public static final byte[] bN_INFINITY = stringToBytes(N_INFINITY);

  @MakeImmutable
  public static final byte[] bNaN = stringToBytes(NaN);

  // ********** Miscellaneous constants for convenience **********
  /**
   * byte value of the number 0
   */
  public static final byte NUMBER_0_BYTE = 48; // '0'

  /**
   * byte value of the number 1
   */
  public static final byte NUMBER_1_BYTE = 49; // '1'

  public static final byte bLOWERCASE_A = 97; // a

  public static final byte bLOWERCASE_Z = 122; // z

  public static final byte bLEFT_PAREN = 40; // (

  public static final byte bLEFT_SQUARE_BRACKET = 91; // [

  public static final byte bPERIOD = 46; // .

  public static final String PING_RESPONSE = "PONG";

  @MakeImmutable
  public static final byte[] bPING_RESPONSE = stringToBytes(PING_RESPONSE);

  @MakeImmutable
  public static final byte[] bPING_RESPONSE_LOWERCASE = stringToBytes(PING_RESPONSE.toLowerCase());

  @MakeImmutable
  public static final byte[] bRADISH_DUMP_HEADER = stringToBytes("RADISH");

  /**
   * These member names will always be evaluated to be "greater than" or "less than" any other when
   * using the {@link RedisSortedSet#checkDummyMemberNames(byte[], byte[])} method, so the rank of
   * an entry using these names will be less than or greater than all other members with the same
   * score.
   * These values should always be compared using {@code ==} rather than {@code Array.equals()} so
   * that we can differentiate between the use of these constants and a value potentially entered by
   * the user, which while equal in content, will not share the same memory address.
   */
  public static final byte[] bGREATEST_MEMBER_NAME = new byte[] {-1};

  public static final byte[] bLEAST_MEMBER_NAME = new byte[] {-2};

  public static final byte[] bNEGATIVE_ZERO = stringToBytes("-0");
}
