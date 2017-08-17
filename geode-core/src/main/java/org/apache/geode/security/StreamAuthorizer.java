package org.apache.geode.security;

/**
 * Created by bschuchardt on 8/17/17.
 */
public interface StreamAuthorizer {
  boolean authorize(ResourcePermission permissionRequested);
}
