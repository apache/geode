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


public class RedisConstants {

  /*
   * Responses
   */
  public static final String QUIT_RESPONSE = "OK";

  /*
   * Error responses
   */
  public static final String PARSING_EXCEPTION_MESSAGE =
      "The command received by GeodeRedisServer was improperly formatted";
  public static final String SERVER_ERROR_MESSAGE =
      "The server had an internal error please try again";
  public static final String SERVER_ERROR_SHUTDOWN = "The server is shutting down";
  public static final String ERROR_UNKNOWN_COMMAND = "Unable to process unknown command";
  public static final String ERROR_CURSOR = "invalid cursor";
  public static final String ERROR_UNSUPPORTED_COMMAND =
      " is not supported. To enable all unsupported commands use GFSH to execute: 'redis --enable-unsupported-commands'. Unsupported commands have not been fully tested.";
  public static final String ERROR_OUT_OF_RANGE = "The number provided is out of range";
  public static final String ERROR_NO_PASS = "Client sent AUTH, but no password is set";
  public static final String ERROR_INVALID_PWD = "invalid password";
  public static final String ERROR_NOT_AUTH = "NOAUTH Authentication required.";
  public static final String ERROR_WRONG_TYPE =
      "Operation against a key holding the wrong kind of value";
  public static final String ERROR_NOT_INTEGER = "value is not an integer or out of range";
  public static final String ERROR_OVERFLOW = "increment or decrement would overflow";
  public static final String ERROR_NO_SUCH_KEY = "no such key";
  public static final String ERROR_SYNTAX = "syntax error";
  public static final String ERROR_INVALID_EXPIRE_TIME = "invalid expire time in set";

  public static class ArityDef {

    /*
     * General
     */
    public static final String ECHO =
        "The wrong number of arguments or syntax was provided, the format for the ECHO command is \"ECHO message\"";
    public static final String PERSIST =
        "The wrong number of arguments or syntax was provided, the format for the PERSIST command is \"PERSIST key\"";
    /*
     * String
     */
    public static final String BITCOUNT =
        "The wrong number of arguments or syntax was provided, the format for the BITCOUNT command is \"BITCOUNT key [start end]\"";
    public static final String BITOP =
        "The wrong number of arguments or syntax was provided, the format for the BITOP command is \"BITOP operation destkey key [key ...]\"";
    public static final String BITPOS =
        "The wrong number of arguments or syntax was provided, the format for the BITOPS command is \"BITPOS key bit [start] [end]\"";
    public static final String DECRBY =
        "The wrong number of arguments or syntax was provided, the format for the DECRBY command is \"DECRRBY key decrement\"";
    public static final String DECR =
        "The wrong number of arguments or syntax was provided, the format for the DECR command is \"DECR key\"";
    public static final String GETBIT =
        "The wrong number of arguments or syntax was provided, the format for the GETBIT command is \"GETBIT key offset\"";
    public static final String GETRANGE =
        "The wrong number of arguments or syntax was provided, the format for the GETRANGE command is \"GETRANGE key start end\"";
    public static final String GETSET =
        "The wrong number of arguments or syntax was provided, the format for the GETSET command is \"GETSET key value\"";
    public static final String INCRBY =
        "The wrong number of arguments or syntax was provided, the format for the INCRBY command is \"INCRBY key increment\"";
    public static final String INCRBYFLOAT =
        "The wrong number of arguments or syntax was provided, the format for the INCRBY command is \"INCRBY key increment\"";
    public static final String INCR =
        "The wrong number of arguments or syntax was provided, the format for the INCR command is \"INCR key\"";
    public static final String MGET =
        "The wrong number of arguments or syntax was provided, the format for the MGET command is \"MGET key [key ...]\"";
    public static final String MSET =
        "The wrong number of arguments or syntax was provided, the format for the MSET command is \"MSET key value [key value ...]\", or not every key matches a value";
    public static final String MSETNX =
        "The wrong number of arguments or syntax was provided, the format for the MSETNX command is \"MSETNX key value [key value ...]\", or not every key matches a value";
    public static final String PSETEX =
        "The wrong number of arguments or syntax was provided, the format for the PSETEX command is \"PSETEX key milliseconds value\"";
    public static final String SETBIT =
        "The wrong number of arguments or syntax was provided, the format for the SETBIT command is \"SETBIT key offset value\"";
    public static final String SETEX =
        "The wrong number of arguments or syntax was provided, the format for the SETEX command is \"SETEX key seconds value\"";
    public static final String SETNX =
        "The wrong number of arguments or syntax was provided, the format for the SETNX command is \"SETNX key value\"";
    public static final String SETRANGE =
        "The wrong number of arguments or syntax was provided, the format for the SETRANGE command is \"SETRANGE key offset value\"";
    public static final String STRLEN =
        "The wrong number of arguments or syntax was provided, the format for the STRELEN command is \"STRLEN key\"";
  }

}
