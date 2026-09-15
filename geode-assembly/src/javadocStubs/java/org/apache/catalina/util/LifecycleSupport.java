/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Javadoc stub for legacy Tomcat 6 class org.apache.catalina.util.LifecycleSupport.
 * This is provided solely to satisfy aggregate Javadoc generation for deprecated
 * session manager implementations that still reference the Tomcat 6 API. It is NOT
 * a functional replacement and MUST NOT be placed on any runtime classpath.
 */
package org.apache.catalina.util;

import org.apache.catalina.LifecycleListener;

/**
 * Minimal no-op implementation exposing only the methods invoked by Geode's
 * deprecated Tomcat6/7 session manager code during Javadoc compilation.
 * All operations are no-ops.
 */
@SuppressWarnings({"unused", "deprecation"})
public class LifecycleSupport {
  private final Object source;

  public LifecycleSupport(Object source) {
    this.source = source;
  }

  public void addLifecycleListener(LifecycleListener listener) {
    // no-op
  }

  public LifecycleListener[] findLifecycleListeners() {
    return new LifecycleListener[0];
  }

  public void fireLifecycleEvent(String type, Object data) {
    // no-op
  }

  public void removeLifecycleListener(LifecycleListener listener) {
    // no-op
  }
}
