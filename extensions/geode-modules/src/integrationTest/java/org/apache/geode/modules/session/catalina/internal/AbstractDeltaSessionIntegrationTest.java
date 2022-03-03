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
package org.apache.geode.modules.session.catalina.internal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.UUID;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Context;
import org.apache.catalina.Manager;
import org.apache.juli.logging.Log;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.modules.session.catalina.DeltaSession;
import org.apache.geode.modules.session.catalina.DeltaSessionManager;
import org.apache.geode.modules.session.catalina.SessionCache;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public abstract class AbstractDeltaSessionIntegrationTest {
  static final String REGION_NAME = "geode_modules_sessions";
  static final String TEST_SESSION_ID = UUID.randomUUID().toString();
  static final String FIRST_ATTRIBUTE_KEY = "FIRST_ATTRIBUTE_KEY";
  static final String SECOND_ATTRIBUTE_KEY = "SECOND_ATTRIBUTE_KEY";
  static final String FIRST_ATTRIBUTE_VALUE = "FIRST_ATTRIBUTE_VALUE";
  static final String SECOND_ATTRIBUTE_VALUE = "SECOND_ATTRIBUTE_VALUE";

  DeltaSessionManager deltaSessionManager;

  Region<String, HttpSession> httpSessionRegion;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  void mockDeltaSessionManager() {
    deltaSessionManager = mock(DeltaSessionManager.class);

    when(deltaSessionManager.getLogger()).thenReturn(mock(Log.class));
    when(deltaSessionManager.getRegionName()).thenReturn(REGION_NAME);
    when(deltaSessionManager.isBackingCacheAvailable()).thenReturn(true);
    when(deltaSessionManager.getContainer()).thenReturn(mock(Context.class));
    when(deltaSessionManager.getSessionCache()).thenReturn(mock(SessionCache.class));
    when(deltaSessionManager.getSessionCache().getOperatingRegion()).thenReturn(httpSessionRegion);
  }

  void parameterizedSetUp(RegionShortcut regionShortcut) {
    httpSessionRegion = server.getCache()
        .<String, HttpSession>createRegionFactory(regionShortcut)
        .create(REGION_NAME);

    mockDeltaSessionManager();
    TestDeltaSession deltaSession = new TestDeltaSession(deltaSessionManager, TEST_SESSION_ID);
    deltaSession.setAttribute(FIRST_ATTRIBUTE_KEY, FIRST_ATTRIBUTE_VALUE);
    deltaSession.setAttribute(SECOND_ATTRIBUTE_KEY, SECOND_ATTRIBUTE_VALUE);

    httpSessionRegion.put(deltaSession.getId(), deltaSession);
  }

  DataSerializable serializeDeserializeObject(DataSerializable originalObject)
      throws IOException, IllegalAccessException, InstantiationException, ClassNotFoundException {
    File tempFile = temporaryFolder.newFile();
    OutputStream outputStream = new FileOutputStream(tempFile);
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
    originalObject.toData(objectOutputStream);
    objectOutputStream.close();
    outputStream.close();

    DataSerializable deserializeObject = originalObject.getClass().newInstance();
    FileInputStream inputStream = new FileInputStream(tempFile);
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
    deserializeObject.fromData(objectInputStream);
    objectInputStream.close();
    inputStream.close();

    return deserializeObject;
  }

  static class TestDeltaSession extends DeltaSession {

    TestDeltaSession(Manager manager, String sessionId) {
      super(manager);
      id = sessionId;
    }

    @Override
    protected boolean isValidInternal() {
      return true;
    }
  }
}
