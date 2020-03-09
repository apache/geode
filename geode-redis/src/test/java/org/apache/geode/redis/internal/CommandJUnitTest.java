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
 *
 */
package org.apache.geode.redis.internal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;



/**
 * Test case for the command
 *
 *
 */
public class CommandJUnitTest {

  /**
   * Test method for {@link org.apache.geode.redis.internal.Command#Command(java.util.List)}.
   */
  @Test
  public void testCommand() {
    List<byte[]> list = null;
    try {
      new Command(list);
      Assert.fail("Expected exception");
    } catch (IllegalArgumentException e) {
    }

    list = new ArrayList<byte[]>();
    try {
      new Command(list);
      Assert.fail("Expected exception");
    } catch (IllegalArgumentException e) {
    }

    list = new ArrayList<byte[]>();
    list.add("Garbage".getBytes(StandardCharsets.UTF_8));

    Command cmd = new Command(list);
    Assert.assertNotNull(cmd.getCommandType());

    Assert.assertEquals(RedisCommandType.UNKNOWN, cmd.getCommandType());
    list.clear();
    list.add(RedisCommandType.HEXISTS.toString().getBytes(StandardCharsets.UTF_8));
    cmd = new Command(list);
    Assert.assertNotNull(cmd.getCommandType());
    Assert.assertNotEquals(RedisCommandType.UNKNOWN, cmd.getCommandType());
    Assert.assertEquals(RedisCommandType.HEXISTS, cmd.getCommandType());
    Assert.assertEquals(list, cmd.getProcessedCommand());
    Assert.assertNull(cmd.getKey());

    list.add("Arg1".getBytes(StandardCharsets.UTF_8));
    cmd = new Command(list);
    Assert.assertNotNull(cmd.getKey());
    Assert.assertEquals("Arg1", cmd.getStringKey());
  }



}
