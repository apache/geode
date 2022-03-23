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


import org.apache.geode.redis.internal.commands.executor.cluster.ClusterExecutor;
import org.apache.geode.redis.internal.commands.executor.connection.ClientExecutor;
import org.apache.geode.redis.internal.commands.executor.server.CommandCommandExecutor;

public class RedisConstants {

  /*
   * Error responses
   */
  public static final String PARSING_EXCEPTION_MESSAGE =
      "ERR The command received by GeodeRedisServer was improperly formatted";
  public static final String SERVER_ERROR_MESSAGE =
      "ERR The server had an internal error please try again";
  public static final String ERROR_SELECT = "ERR Only DB 0 supported";
  public static final String ERROR_CURSOR = "ERR invalid cursor";
  public static final String ERROR_UNKNOWN_COMMAND =
      "ERR unknown command `%s`, with args beginning with: %s";
  public static final String ERROR_OUT_OF_RANGE = "ERR The number provided is out of range";
  public static final String ERROR_NOT_AUTHENTICATED = "NOAUTH Authentication required.";
  public static final String ERROR_NOT_AUTHORIZED = "ERR Authorization failed.";
  public static final String ERROR_WRONG_TYPE =
      "WRONGTYPE Operation against a key holding the wrong kind of value";
  public static final String ERROR_NOT_INTEGER = "ERR value is not an integer or out of range";
  public static final String ERROR_VALUE_MUST_BE_POSITIVE =
      "ERR value is out of range, must be positive";
  public static final String ERROR_OVERFLOW = "ERR increment or decrement would overflow";
  public static final String ERROR_NAN_OR_INFINITY = "ERR increment would produce NaN or Infinity";
  public static final String ERROR_OPERATION_PRODUCED_NAN =
      "ERR resulting score is not a number (NaN)";
  public static final String ERROR_NO_SUCH_KEY = "ERR no such key";
  public static final String ERROR_INDEX_OUT_OF_RANGE = "ERR index out of range";
  public static final String ERROR_SYNTAX = "ERR syntax error";
  public static final String ERROR_INVALID_EXPIRE_TIME = "ERR invalid expire time in %s";
  public static final String ERROR_NOT_A_VALID_FLOAT = "ERR value is not a valid float";
  public static final String ERROR_MIN_MAX_NOT_A_VALID_STRING =
      "ERR min or max not valid string range item";
  public static final String ERROR_MIN_MAX_NOT_A_FLOAT = "ERR min or max is not a float";
  public static final String ERROR_OOM_COMMAND_NOT_ALLOWED =
      "OOM command not allowed when used memory > 'maxmemory'";
  public static final String ERROR_UNKNOWN_SLOWLOG_SUBCOMMAND =
      "ERR Unknown subcommand or wrong number of arguments for '%s'. Try SLOWLOG HELP.";
  public static final String ERROR_UNKNOWN_CLIENT_SUBCOMMAND =
      "ERR Unknown subcommand or wrong number of arguments for '%s'. Supported subcommands are: "
          + ClientExecutor.getSupportedSubcommands();
  public static final String ERROR_INVALID_CLIENT_NAME =
      "ERR Client names cannot contain spaces, newlines or special characters.";
  public static final String ERROR_UNKNOWN_CLUSTER_SUBCOMMAND =
      "ERR Unknown subcommand or wrong number of arguments for '%s'. Supported subcommands are: "
          + ClusterExecutor.getSupportedSubcommands();
  public static final String ERROR_UNKNOWN_COMMAND_COMMAND_SUBCOMMAND =
      "ERR Unknown subcommand or wrong number of arguments for '%s'. Supported subcommands are: "
          + CommandCommandExecutor.getSupportedSubcommands();
  public static final String ERROR_INVALID_ZADD_OPTION_NX_XX =
      "ERR XX and NX options at the same time are not compatible";
  public static final String ERROR_UNKNOWN_PUBSUB_SUBCOMMAND =
      "ERR Unknown subcommand or wrong number of arguments for '%s'";
  public static final String ERROR_ZADD_OPTION_TOO_MANY_INCR_PAIR =
      "ERR INCR option supports a single increment-element pair";
  public static final String ERROR_KEY_EXISTS =
      "BUSYKEY Target key name already exists.";
  public static final String ERROR_INVALID_TTL = "ERR Invalid TTL value, must be >= 0";
  public static final String ERROR_RESTORE_INVALID_PAYLOAD =
      "ERR DUMP payload version or checksum are wrong";
  public static final String ERROR_WRONG_SLOT =
      "CROSSSLOT Keys in request don't hash to the same slot";
  public static final String ERROR_WEIGHT_NOT_A_FLOAT =
      "ERR weight value is not a float";
  public static final String ERROR_INVALID_USERNAME_OR_PASSWORD =
      "WRONGPASS invalid username-password pair or user is disabled.";
  public static final String ERROR_AUTH_CALLED_WITHOUT_SECURITY_CONFIGURED =
      "ERR AUTH called without a Security Manager configured.";
  public static final String ERROR_KEY_REQUIRED_ZINTERSTORE =
      "ERR at least 1 input key is needed for zinterstore";
  public static final String ERROR_KEY_REQUIRED_ZUNIONSTORE =
      "ERR at least 1 input key is needed for zunionstore";
  public static final String ERROR_UNAUTHENTICATED_MULTIBULK =
      "ERR Protocol error: unauthenticated multibulk length";
  public static final String ERROR_UNAUTHENTICATED_BULK =
      "ERR Protocol error: unauthenticated bulk length";
  public static final String INTERNAL_SERVER_ERROR = "ERR Internal server error: ";
  public static final String ERROR_PUBSUB_WRONG_COMMAND =
      "ERR Can't execute '%s': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context";
  public static final String HASH_VALUE_NOT_FLOAT = "ERR hash value is not a float";
  public static final String WRONG_NUMBER_OF_ARGUMENTS_FOR_MSET =
      "ERR wrong number of arguments for MSET";
  public static final String WRONG_NUMBER_OF_ARGUMENTS_FOR_COMMAND =
      "ERR wrong number of arguments for '%s' command";
  public static final String ERROR_BITOP_NOT_MUST_USE_SINGLE_KEY =
      "ERR BITOP NOT must be called with a single source key.";
  public static final String ERROR_TIMEOUT_INVALID = "ERR timeout is not a float or out of range";
  public static final String ERROR_NEGATIVE_TIMEOUT = "ERR timeout is negative";
}
