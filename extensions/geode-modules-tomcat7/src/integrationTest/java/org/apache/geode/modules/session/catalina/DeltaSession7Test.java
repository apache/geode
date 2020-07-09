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
package org.apache.geode.modules.session.catalina;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;



public class DeltaSession7Test
    extends AbstractDeltaSessionIntegrationTest<Tomcat7DeltaSessionManager, DeltaSession7> {

  public DeltaSession7Test() {
    super(mock(Tomcat7DeltaSessionManager.class));
  }

  @Override
  public void before() {
    super.before();
    when(manager.getContainer()).thenReturn(context);
  }

  @Override
  protected DeltaSession7 newSession(Tomcat7DeltaSessionManager manager) {
    return new DeltaSession7(manager);
  }

}
