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
import org.apache.geode.redis.internal.commands.executor.server.COMMANDCommandExecutor;

public class RedisConstants {

  /*
   * Error responses
   */
  public static final String PARSING_EXCEPTION_MESSAGE =
      "The command received by GeodeRedisServer was improperly formatted";
  public static final String SERVER_ERROR_MESSAGE =
      "The server had an internal error please try again";
  public static final String ERROR_SELECT = "Only DB 0 supported";
  public static final String ERROR_CURSOR = "invalid cursor";
  public static final String ERROR_UNKNOWN_COMMAND =
      "unknown command `%s`, with args beginning with: %s";
  public static final String ERROR_OUT_OF_RANGE = "The number provided is out of range";
  public static final String ERROR_NOT_AUTHENTICATED = "Authentication required.";
  public static final String ERROR_NOT_AUTHORIZED = "Authorization failed.";
  public static final String ERROR_WRONG_TYPE =
      "Operation against a key holding the wrong kind of value";
  public static final String ERROR_NOT_INTEGER = "value is not an integer or out of range";
  public static final String ERROR_VALUE_MUST_BE_POSITIVE =
      "value is out of range, must be positive";
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
  public static final String ERROR_UNKNOWN_CLIENT_SUBCOMMAND =
      "Unknown subcommand or wrong number of arguments for '%s'. Supported subcommands are: "
          + ClientExecutor.getSupportedSubcommands();
  public static final String ERROR_INVALID_CLIENT_NAME =
      "Client names cannot contain spaces, newlines or special characters.";
  public static final String ERROR_UNKNOWN_CLUSTER_SUBCOMMAND =
      "Unknown subcommand or wrong number of arguments for '%s'. Supported subcommands are: "
          + ClusterExecutor.getSupportedSubcommands();
  public static final String ERROR_UNKNOWN_COMMAND_COMMAND_SUBCOMMAND =
      "Unknown subcommand or wrong number of arguments for '%s'. Supported subcommands are: "
          + COMMANDCommandExecutor.getSupportedSubcommands();
  public static final String ERROR_INVALID_ZADD_OPTION_NX_XX =
      "XX and NX options at the same time are not compatible";
  public static final String ERROR_UNKNOWN_PUBSUB_SUBCOMMAND =
      "Unknown subcommand or wrong number of arguments for '%s'";
  public static final String ERROR_ZADD_OPTION_TOO_MANY_INCR_PAIR =
      "INCR option supports a single increment-element pair";
  public static final String ERROR_KEY_EXISTS =
      "Target key name already exists.";
  public static final String ERROR_INVALID_TTL = "Invalid TTL value, must be >= 0";
  public static final String ERROR_RESTORE_INVALID_PAYLOAD =
      "DUMP payload version or checksum are wrong";
  public static final String ERROR_WRONG_SLOT =
      "Keys in request don't hash to the same slot";
  public static final String ERROR_DIFFERENT_SLOTS =
      "No way to dispatch this command to Redis Cluster because keys have different slots.";
  public static final String ERROR_WEIGHT_NOT_A_FLOAT =
      "weight value is not a float";
  public static final String ERROR_INVALID_USERNAME_OR_PASSWORD =
      "invalid username-password pair or user is disabled.";
  public static final String ERROR_AUTH_CALLED_WITHOUT_SECURITY_CONFIGURED =
      "AUTH called without a Security Manager configured.";
  public static final String ERROR_KEY_REQUIRED_ZINTERSTORE =
      "at least 1 input key is needed for zinterstore";
  public static final String ERROR_KEY_REQUIRED_ZUNIONSTORE =
      "at least 1 input key is needed for zunionstore";
  public static final String ERROR_UNAUTHENTICATED_MULTIBULK =
      "Protocol error: unauthenticated multibulk length";
  public static final String ERROR_UNAUTHENTICATED_BULK =
      "Protocol error: unauthenticated bulk length";
  public static final String INTERNAL_SERVER_ERROR = "Internal server error: ";
  public static final String ERROR_PUBSUB_WRONG_COMMAND =
      "Can't execute '%s': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context";
}
