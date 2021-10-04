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
package org.apache.geode.redis.internal.executor.hash;


/**
 * <pre>
 * Implements the HSETNX Redis command.
 * This command sets field in the hash stored at key with the given value
 *
 * A new key holding a hash is created if key does not exist, .
 * Nothing will be changed if field already exists.
 *
 * Examples:
 *
 * redis> HSETNX myhash field "Hello"
 * (integer) 1
 * redis> HSETNX myhash field "World"
 * (integer) 0
 * redis> HGET myhash field
 * </pre>
 */
public class HSetNXExecutor extends HSetExecutor {

  @Override
  protected boolean onlySetOnAbsent() {
    return true;
  }
}
