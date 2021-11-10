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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
  public void putSubjectWithNegativeOneWillProduceNewId() {
    Long uniqueId = auth.putSubject(subject1, -1);
    verify(auth, never()).removeSubject(anyLong());
    assertThat(uniqueId).isEqualTo(123L);
  }

  @Test
  public void putSubjectWithZeroWillProduceNewId() {
    Long uniqueId = auth.putSubject(subject1, 0);
    verify(auth, never()).removeSubject(anyLong());
    assertThat(uniqueId).isEqualTo(123L);
    verify(subject1, never()).logout();
  }

  @Test
  public void putSubjectWithExistingId() {
    Long uniqueId = auth.putSubject(subject1, 456L);
    verify(auth, never()).removeSubject(anyLong());
    assertThat(uniqueId).isEqualTo(456L);
    verify(subject1, never()).logout();
  }

  @Test
  public void replacedSubjectShouldNotLogout() {
    Long id1 = auth.putSubject(subject1, -1);
    Long id2 = auth.putSubject(subject2, id1);
    assertThat(id1).isEqualTo(id2);
    verify(auth, never()).removeSubject(anyLong());
    verify(subject1, never()).logout();

  }

  @Test
  public void getSubjectReturnsTheLatestOne() {
    Long id1 = auth.putSubject(subject1, -1);
    assertThat(auth.getSubject(id1)).isSameAs(subject1);
    auth.putSubject(subject2, id1);
    assertThat(auth.getSubject(id1)).isSameAs(subject2);
  }

  @Test
  public void getSubjectOfNonExistentId() {
    assertThat(auth.getSubject(5678L)).isNull();
  }

  @Test
  public void removeSubjectOfNonExistentId() {
    assertThatNoException().isThrownBy(() -> auth.removeSubject(5678L));
  }

  @Test
  public void getSubjectWithCq() {
    long id = auth.putSubject(subject1, -1);
    auth.setUserAuthAttributesForCq("cq", id, true);
    assertThat(auth.getSubject("cq")).isSameAs(subject1);

    auth.removeSubject(id);
    assertThat(auth.getSubject("cq")).isNull();
  }

  @Test
  public void removeSubject() {
    Long id1 = auth.putSubject(subject1, -1);
    auth.putSubject(subject2, id1);
    auth.removeSubject(id1);
    assertThat(auth.getSubject(id1)).isNull();
    verify(subject1).logout();
    verify(subject2).logout();
  }

  @Test
  public void cqSubjectIsTracked() {
    Long id1 = auth.putSubject(subject1, -1);
    auth.setUserAuthAttributesForCq("cq1", id1, true);
    Subject subject = auth.getSubject("cq1");
    assertThat(subject).isEqualTo(subject1);

    auth.removeUserAuthAttributesForCq("cq1", true);
    assertThat(auth.getSubject("cq1")).isNull();
  }
}
