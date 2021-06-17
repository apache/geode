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
  public static final byte BULK_STRING_ID = 36; // '$'

  /**
   * byte identifier of an array
   */
  public static final byte ARRAY_ID = 42; // '*'

  /**
   * byte identifier of a simple string
   */
  public static final byte SIMPLE_STRING_ID = 43; // '+'

  /**
   * byte identifier of an error
   */
  public static final byte ERROR_ID = 45; // '-'

  /**
   * byte identifier of an integer
   */
  public static final byte INTEGER_ID = 58; // ':'

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

  // ********** Redis Command constants **********

  // ClusterExecutor
  @MakeImmutable
  public static final byte[] bINFO = stringToBytes("INFO");
  @MakeImmutable
  public static final byte[] bSLOTS = stringToBytes("SLOTS");
  @MakeImmutable
  public static final byte[] bNODES = stringToBytes("NODES");

  // ScanExecutor, HScanExecutor and SScanExecutor
  @MakeImmutable
  public static final byte[] bMATCH = stringToBytes("MATCH");
  @MakeImmutable
  public static final byte[] bCOUNT = stringToBytes("COUNT");

  // PubSubExecutor
  @MakeImmutable
  public static final byte[] bCHANNELS = stringToBytes("CHANNELS");

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

  // SetExecutor
  @MakeImmutable
  public static final byte[] bEX = stringToBytes("EX");
  @MakeImmutable
  public static final byte[] bPX = stringToBytes("PX");

  // ********** Constants for Double Infinity comparisons **********
  public static final String P_INF = "+inf";
  public static final String INF = "inf";
  public static final String P_INFINITY = "+Infinity";
  public static final String INFINITY = "Infinity";
  public static final String N_INF = "-inf";
  public static final String N_INFINITY = "-Infinity";

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

  public static final String PING_RESPONSE = "PONG";

  @MakeImmutable
  public static final byte[] bPING_RESPONSE = stringToBytes(PING_RESPONSE);

  @MakeImmutable
  public static final byte[] bPING_RESPONSE_LOWERCASE = stringToBytes(PING_RESPONSE.toLowerCase());
}
