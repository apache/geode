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

import org.apache.catalina.Manager;


@SuppressWarnings("serial")
public class DeltaSession9 extends DeltaSession {
  /**
   * Construct a new <code>Session</code> associated with no <code>Manager</code>. The
   * <code>Manager</code> will be assigned later using {@link #setOwner(Object)}.
   */
  public DeltaSession9() {
    super();
  }

  /**
   * Construct a new Session associated with the specified Manager.
   *
   * @param manager The manager with which this Session is associated
   */
  public DeltaSession9(Manager manager) {
    super(manager);
  }

}
