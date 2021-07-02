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

/**
 * A general exception that can be thrown during command execution and will result in a Redis
 * protocol exception message being returned to the client.
 */
public class RedisException extends RuntimeException {

  private static final long serialVersionUID = 6530956425269820304L;

  public RedisException() {
    super();
  }

  public RedisException(String message) {
    super(message);
  }

  public RedisException(String message, Throwable throwable) {
    super(message, throwable);
  }

}
