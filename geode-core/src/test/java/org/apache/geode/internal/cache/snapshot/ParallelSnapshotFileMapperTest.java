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
package org.apache.geode.internal.cache.snapshot;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class ParallelSnapshotFileMapperTest {
  private static final int PORT = 1234;
  private static final String BASE_LOCATION = "/test/snapshot";
  private static final String FILE_TYPE = ".gfd";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private SnapshotFileMapper mapper;

  @Before
  public void setup() {
    mapper = new ParallelSnapshotFileMapper();
  }

  @Test
  public void mapExportPathWithIpv4() throws UnknownHostException {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getInetAddress()).thenReturn(InetAddress.getByName("127.0.0.1"));
    when(member.getMembershipPort()).thenReturn(PORT);
    File mappedFile = mapper.mapExportPath(member, new File(BASE_LOCATION + FILE_TYPE));
    File expectedFile = new File(BASE_LOCATION + "-" + 1270011234 + FILE_TYPE);
    assertEquals(expectedFile, mappedFile);
  }

  @Test
  public void mapExportPathWithIpv6() throws UnknownHostException {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getInetAddress()).thenReturn(InetAddress.getByName("2001:db8::2"));
    when(member.getMembershipPort()).thenReturn(PORT);
    File mappedFile = mapper.mapExportPath(member, new File(BASE_LOCATION + FILE_TYPE));
    // db8 == db800000
    File expectedFile = new File(BASE_LOCATION + "-" + "2001db80000021234" + FILE_TYPE);
    assertEquals(expectedFile, mappedFile);
  }

  @Test
  public void mapImportReturnsUnchangedPath() {
    File file = new File(BASE_LOCATION + FILE_TYPE);
    File[] mappedFiles = mapper.mapImportPath(null, file);
    assertEquals(file, mappedFiles[0]);
  }

  @Test
  public void filesWithoutCorrectExtensionGiveUsefulException() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    mapper.mapExportPath(null, new File(BASE_LOCATION));
  }
}
