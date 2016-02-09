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
package com.gemstone.gemfire.internal.redis;

/**
 * Exception thrown by CommandParser (non-existent class) when a command has illegal syntax
 * 
 * @author Vitaliy Gavrilov
 *
 */
public class RedisCommandParserException extends Exception {

  private static final long serialVersionUID = 4707944288714910949L;

  public RedisCommandParserException() {
    super();
  }

  public RedisCommandParserException(String message) {
    super(message);
  }

  public RedisCommandParserException(Throwable cause) {
    super(cause);
  }

  public RedisCommandParserException(String message, Throwable cause) {
    super(message, cause);
  }

}
