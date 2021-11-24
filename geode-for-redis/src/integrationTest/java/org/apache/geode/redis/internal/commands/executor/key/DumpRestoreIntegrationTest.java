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

package org.apache.geode.redis.internal.commands.executor.key;

import static org.apache.geode.redis.internal.netty.StringBytesGlossary.RADISH_DUMP_HEADER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.GeodeRedisServerRule;

public class DumpRestoreIntegrationTest extends AbstractDumpRestoreIntegrationTest {

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Override
  public int getPort() {
    return server.getPort();
  }

  @Test
  public void restoreWithIdletime_isNotSupported() {
    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.RESTORE, "key", "0", "", "IDLETIME", "1"))
            .hasMessageContaining("ERR syntax error");
  }

  @Test
  public void restoreWithFreq_isNotSupported() {
    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.RESTORE, "key", "0", "", "FREQ", "1"))
            .hasMessageContaining("ERR syntax error");
  }

  @Test
  public void restoreWithUnknownVersion_isNotSupported() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(RADISH_DUMP_HEADER.length + 2);
    DataOutputStream output = new DataOutputStream(baos);
    output.write(RADISH_DUMP_HEADER);
    output.writeShort(0);

    assertThatThrownBy(
        () -> jedis.restore("key", 0L, baos.toByteArray()))
            .hasMessageContaining("ERR DUMP payload version or checksum are wrong");
  }

}
