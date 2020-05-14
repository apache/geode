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

package org.apache.geode.redis.internal.executor.set;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.geode.redis.internal.ByteArrayWrapper;

public interface RedisSetCommands {

  long sadd(ByteArrayWrapper key, ArrayList<ByteArrayWrapper> membersToAdd);

  long srem(ByteArrayWrapper key, ArrayList<ByteArrayWrapper> membersToRemove);

  Set<ByteArrayWrapper> smembers(ByteArrayWrapper key);

  boolean del(ByteArrayWrapper key);

  int scard(ByteArrayWrapper key);

  boolean sismember(ByteArrayWrapper key, ByteArrayWrapper member);

  Collection<ByteArrayWrapper> srandmember(ByteArrayWrapper key, int count);

  Collection<ByteArrayWrapper> spop(ByteArrayWrapper key, int popCount);

  List<Object> sscan(ByteArrayWrapper key, Pattern matchPattern, int count, int cursor);
}
