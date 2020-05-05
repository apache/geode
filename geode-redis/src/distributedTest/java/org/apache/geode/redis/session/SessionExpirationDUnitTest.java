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

import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.test.awaitility.GeodeAwaitility;

public class SessionExpirationDUnitTest extends SessionDUnitTest {

  @Test
  public void sessionShouldTimeout_whenRequestedFromSameServer() {
    String sessionCookie = createNewSessionWithNote(APP1, "note1");
    String sessionId = getSessionId(sessionCookie);

    waitForTheSessionToExpire(sessionId);

    assertThat(getSessionNotes(APP1, sessionCookie)).isNull();
  }

  @Test
  public void sessionShouldTimeout_OnSecondaryServer() {
    String sessionCookie = createNewSessionWithNote(APP1, "note1");
    String sessionId = getSessionId(sessionCookie);

    waitForTheSessionToExpire(sessionId);

    assertThat(getSessionNotes(APP2, sessionCookie)).isNull();
  }

  @Test
  @Ignore("GEODE-8058: this test needs to pass to have feature parity with native redis")
  public void sessionShouldNotTimeoutOnFirstServer_whenAccessedOnSecondaryServer() {
    String sessionCookie = createNewSessionWithNote(APP1, "note1");
    String sessionId = getSessionId(sessionCookie);

    refreshSession(sessionCookie, APP2);

    assertThat(jedisConnetedToServer1.ttl("spring:session:sessions:expires:" + sessionId))
        .isGreaterThan(0);

    assertThat(getSessionNotes(APP1, sessionCookie)).isNotNull();
  }

  @Test
  public void sessionShouldTimeout_whenAppFailsOverToAnotherRedisServer() {
    String sessionCookie = createNewSessionWithNote(APP2, "note1");
    String sessionId = getSessionId(sessionCookie);

    cluster.crashVM(SERVER2);

    try {
      waitForTheSessionToExpire(sessionId);

      assertThat(getSessionNotes(APP1, sessionCookie)).isNull();
      assertThat(getSessionNotes(APP2, sessionCookie)).isNull();

    } finally {
      startRedisServer(SERVER2);
    }
  }

  private void waitForTheSessionToExpire(String sessionId) {
    GeodeAwaitility.await().atMost((SESSION_TIMEOUT + 5), TimeUnit.SECONDS)
        .until(
            () -> jedisConnetedToServer1.ttl("spring:session:sessions:expires:" + sessionId) < 0);
  }

  private void refreshSession(String sessionCookie, int sessionApp) {
    GeodeAwaitility.await()
        .during(SESSION_TIMEOUT + 2, TimeUnit.SECONDS)
        .until(() -> getSessionNotes(sessionApp, sessionCookie) != null);
  }
}
