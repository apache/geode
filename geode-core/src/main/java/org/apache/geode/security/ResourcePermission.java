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
package org.apache.geode.security;

import org.apache.commons.lang.StringUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.shiro.authz.permission.WildcardPermission;

/**
 * ResourcePermission defines the resource, the operation, the region and the key involved in the
 * action to be authorized.
 *
 * It is passed to the SecurityManager for the implementation to decide whether to grant a user this
 * permission or not.
 */
public class ResourcePermission extends WildcardPermission {

  public static String ALL = "*";

  public enum Resource {
    NULL, CLUSTER, DATA
  }

  public enum Operation {
    NULL, MANAGE, WRITE, READ
  }

  // when ALL is specified, we need it to convert to string "*" instead of "ALL".
  public enum Target {
    ALL(ResourcePermission.ALL), DISK, GATEWAY, QUERY, JAR;

    private String name;

    Target() {}

    Target(String name) {
      this.name = name;
    }

    public String getName() {
      if (name != null) {
        return name;
      }
      return name();
    }
  }

  // these default values are used when creating a lock around an operation
  private Resource resource = Resource.NULL;
  private Operation operation = Operation.NULL;
  private String target = ALL;
  private String key = ALL;

  public ResourcePermission() {
    this(Resource.NULL, Operation.NULL, ALL, ALL);
  }

  public ResourcePermission(Resource resource, Operation operation) {
    this(resource, operation, ALL, ALL);
  }

  public ResourcePermission(Resource resource, Operation operation, String target) {
    this(resource, operation, target, ALL);
  }

  public ResourcePermission(Resource resource, Operation operation, Target target) {
    this(resource, operation, target, ALL);
  }

  public ResourcePermission(Resource resource, Operation operation, Target target,
      String targetKey) {
    this(resource, operation, target.getName(), targetKey);
  }

  public ResourcePermission(Resource resource, Operation operation, String target, String key) {
    if (resource != null)
      this.resource = resource;
    if (operation != null)
      this.operation = operation;
    if (target != null)
      this.target = StringUtils.stripStart(target, Region.SEPARATOR);
    if (key != null)
      this.key = key;

    setParts(this.resource + ":" + this.operation + ":" + this.target + ":" + this.key, true);
  }

  /**
   * Returns the resource, could be either DATA or CLUSTER
   */
  public Resource getResource() {
    return resource;
  }

  /**
   * Returns the operation, could be either MANAGE, WRITE or READ
   */
  public Operation getOperation() {
    return operation;
  }

  /**
   * returns the regionName, could be "*", meaning all regions
   */
  public String getTarget() {
    return target;
  }

  /**
   * returns the key, could be "*" meaning all keys.
   */
  public String getKey() {
    return key;
  }

  @Override
  public String toString() {
    if (ALL.equals(target)) {
      return getResource() + ":" + getOperation();
    } else if (ALL.equals(key)) {
      return resource + ":" + operation + ":" + target;
    } else {
      return resource + ":" + operation + ":" + target + ":" + key;
    }
  }

}
