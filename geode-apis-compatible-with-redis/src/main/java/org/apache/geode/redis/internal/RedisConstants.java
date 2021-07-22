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
   * Error responses
   */
  public static final String PARSING_EXCEPTION_MESSAGE =
      "The command received by GeodeRedisServer was improperly formatted";
  public static final String SERVER_ERROR_MESSAGE =
      "The server had an internal error please try again";
  public static final String SERVER_ERROR_SHUTDOWN = "The server is shutting down";
  public static final String ERROR_SELECT = "Only DB 0 supported";
  public static final String ERROR_CURSOR = "invalid cursor";
  public static final String ERROR_UNKNOWN_COMMAND =
      "unknown command `%s`, with args beginning with: %s";
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
  public static final String ERROR_NAN_OR_INFINITY = "increment would produce NaN or Infinity";
  public static final String ERROR_OPERATION_PRODUCED_NAN = "resulting score is not a number (NaN)";
  public static final String ERROR_NO_SUCH_KEY = "no such key";
  public static final String ERROR_SYNTAX = "syntax error";
  public static final String ERROR_INVALID_EXPIRE_TIME = "invalid expire time in set";
  public static final String ERROR_NOT_A_VALID_FLOAT = "value is not a valid float";
  public static final String ERROR_MIN_MAX_NOT_A_VALID_STRING =
      "min or max not valid string range item";
  public static final String ERROR_MIN_MAX_NOT_A_FLOAT = "min or max is not a float";
  public static final String ERROR_OOM_COMMAND_NOT_ALLOWED =
      "command not allowed when used memory > 'maxmemory'";

  public static final String ERROR_UNKNOWN_SLOWLOG_SUBCOMMAND =
      "Unknown subcommand or wrong number of arguments for '%s'. Try SLOWLOG HELP.";
  public static final String ERROR_UNKNOWN_CLUSTER_SUBCOMMAND =
      "Unknown subcommand or wrong number of arguments for '%s'. Try CLUSTER HELP.";
  public static final String ERROR_INVALID_ZADD_OPTION_NX_XX =
      "XX and NX options at the same time are not compatible";
  public static final String ERROR_UNKNOWN_PUBSUB_SUBCOMMAND =
      "Unknown subcommand or wrong number of arguments for '%s'";
  public static final String ERROR_ZADD_OPTION_TOO_MANY_INCR_PAIR =
      "INCR option supports a single increment-element pair";
  public static final String ERROR_RESTORE_KEY_EXISTS =
      "Target key name already exists.";
  public static final String ERROR_INVALID_TTL = "Invalid TTL value, must be >= 0";
  public static final String ERROR_RESTORE_INVALID_PAYLOAD =
      "DUMP payload version or checksum are wrong";
}
