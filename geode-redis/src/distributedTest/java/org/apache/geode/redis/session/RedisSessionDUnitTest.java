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
package org.apache.geode.redis.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class RedisSessionDUnitTest extends SessionDUnitTest {

  @BeforeClass
  public static void setup() {
    SessionDUnitTest.setup();
    startSpringApp(APP1, DEFAULT_SESSION_TIMEOUT, ports.get(SERVER1), ports.get(SERVER2));
    startSpringApp(APP2, DEFAULT_SESSION_TIMEOUT, ports.get(SERVER2), ports.get(SERVER1));
  }

  @Test
  public void should_beAbleToCreateASession_storedInRedis() {
    String sessionCookie = createNewSessionWithNote(APP1, "note1");
    String sessionId = getSessionId(sessionCookie);

    Map<String, String> sessionInfo =
        jedisConnetedToServer1.hgetAll("spring:session:sessions:" + sessionId);

    assertThat(sessionInfo.get("sessionAttr:NOTES")).contains("note1");
  }

  @Test
  public void should_storeSession() {
    String sessionCookie = createNewSessionWithNote(APP1, "note1");

    String[] sessionNotes = getSessionNotes(APP2, sessionCookie);

    assertThat(sessionNotes).containsExactly("note1");
  }

  @Test
  public void should_propagateSession_toOtherServers() {
    String sessionCookie = createNewSessionWithNote(APP1, "noteFromClient1");

    String[] sessionNotes = getSessionNotes(APP2, sessionCookie);

    assertThat(sessionNotes).containsExactly("noteFromClient1");
  }

  @Test
  public void should_getSessionFromServer1_whenServer2GoesDown() {
    String sessionCookie = createNewSessionWithNote(APP2, "noteFromClient2");
    cluster.crashVM(SERVER2);
    try {
      String[] sessionNotes = getSessionNotes(APP1, sessionCookie);

      assertThat(sessionNotes).containsExactly("noteFromClient2");
    } finally {
      startRedisServer(SERVER2);
    }
  }

  @Test
  public void should_getSessionFromServer_whenServerGoesDownAndIsRestarted() {
    String sessionCookie = createNewSessionWithNote(APP2, "noteFromClient2");
    cluster.crashVM(SERVER2);
    addNoteToSession(APP1, sessionCookie, "noteFromClient1");
    startRedisServer(SERVER2);

    String[] sessionNotes = getSessionNotes(APP2, sessionCookie);

    assertThat(sessionNotes).containsExactlyInAnyOrder("noteFromClient2", "noteFromClient1");
  }

  @Test
  public void should_getSession_whenServer2GoesDown_andAppFailsOverToServer1() {
    String sessionCookie = createNewSessionWithNote(APP2, "noteFromClient2");
    cluster.crashVM(SERVER2);

    try {
      String[] sessionNotes = getSessionNotes(APP2, sessionCookie);

      assertThat(sessionNotes).containsExactly("noteFromClient2");
    } finally {
      startRedisServer(SERVER2);
    }
  }

  @Test
  public void should_getSessionCreatedByApp2_whenApp2GoesDown_andClientConnectsToApp1() {
    String sessionCookie = createNewSessionWithNote(APP2, "noteFromClient2");
    stopSpringApp(APP2);

    try {
      String[] sessionNotes = getSessionNotes(APP1, sessionCookie);

      assertThat(sessionNotes).containsExactly("noteFromClient2");
    } finally {
      if (ports.get(SERVER2) == null) {
        startSpringApp(APP2, SERVER1, DEFAULT_SESSION_TIMEOUT);
      } else {
        startSpringApp(APP2, SERVER1, SERVER2, DEFAULT_SESSION_TIMEOUT);
      }
    }
  }
}
