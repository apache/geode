/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.gemstone.gemfire.modules.util;

import com.gemstone.gemfire.modules.session.catalina.DeltaSessionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * This basic singleton class maps context paths to manager instances.
 * <p>
 * This class exists for a particular corner case described here. Consider a client-server environment with empty client
 * regions *and* the need to fire HttpSessionListener destroy events. When a session expires, in this scenario, the
 * Gemfire destroy events originate on the server and, with some Gemfire hackery, the destroyed object ends up as the
 * event's callback argument. At the point that the CacheListener then gets the event, the re-constituted session object
 * has no manager associated and so we need to re-attach a manager to it so that events can be fired correctly.
 */

public class ContextMapper {

  private static Map<String, DeltaSessionManager> managers = new HashMap<String, DeltaSessionManager>();

  private ContextMapper() {
    // This is a singleton
  }

  public static void addContext(String path, DeltaSessionManager manager) {
    managers.put(path, manager);
  }

  public static DeltaSessionManager getContext(String path) {
    return managers.get(path);
  }

  public static DeltaSessionManager removeContext(String path) {
    return managers.remove(path);
  }
}
