package com.gemstone.gemfire.modules.util;

import com.gemstone.gemfire.modules.session.catalina.DeltaSessionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * This basic singleton class maps context paths to manager instances.
 *
 * This class exists for a particular corner case described here. Consider a
 * client-server environment with empty client regions *and* the need to fire
 * HttpSessionListener destroy events. When a session expires, in this scenario,
 * the Gemfire destroy events originate on the server and, with some Gemfire
 * hackery, the destroyed object ends up as the event's callback argument. At
 * the point that the CacheListener then gets the event, the re-constituted
 * session object has no manager associated and so we need to re-attach a
 * manager to it so that events can be fired correctly.
 */

public class ContextMapper {

  private static Map<String, DeltaSessionManager> managers =
      new HashMap<String, DeltaSessionManager>();

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
