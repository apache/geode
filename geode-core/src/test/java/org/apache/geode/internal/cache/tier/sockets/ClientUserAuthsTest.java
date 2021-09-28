/*
 *
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

package org.apache.geode.internal.cache.tier.sockets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

public class ClientUserAuthsTest {
  private ClientUserAuths auth;
  private Subject subject1;
  private Subject subject2;

  @Before
  public void before() throws Exception {
    subject1 = mock(Subject.class);
    subject2 = mock(Subject.class);
    when(subject1.getPrincipal()).thenReturn("user1");
    when(subject2.getPrincipal()).thenReturn("user2");
    auth = spy(new ClientUserAuths(0));
    doReturn(123L).when(auth).getNextID();
  }

  @Test
  public void putSubjectWithNegativeOneWillProduceNewId() throws Exception {
    Long uniqueId = auth.putSubject(subject1, -1);
    verify(auth).removeSubject((Subject) null);
    assertThat(uniqueId).isEqualTo(123L);
  }

  @Test
  public void putSubjectWithZeroWillProduceNewId() throws Exception {
    Long uniqueId = auth.putSubject(subject1, 0);
    verify(auth).removeSubject((Subject) null);
    assertThat(uniqueId).isEqualTo(123L);
  }

  @Test
  public void putSubjectWithExistingId() throws Exception {
    Long uniqueId = auth.putSubject(subject1, 456L);
    verify(auth).removeSubject((Subject) null);
    assertThat(uniqueId).isEqualTo(456L);
  }

  @Test
  public void replacedSubjectShouldLogout() throws Exception {
    Long id1 = auth.putSubject(subject1, -1);
    Long id2 = auth.putSubject(subject2, id1);
    assertThat(id1).isEqualTo(id2);
    verify(auth).removeSubject(eq(subject1));
    verify(subject1).logout();
  }

  @Test
  public void cqSubjectIsTracked() throws Exception {
    Long id1 = auth.putSubject(subject1, -1);
    auth.setUserAuthAttributesForCq("cq1", id1, true);
    Subject subject = auth.getSubject("cq1");
    assertThat(subject).isEqualTo(subject1);

    auth.removeUserAuthAttributesForCq("cq1", true);
    assertThat(auth.getSubject("cq1")).isNull();
  }
}
