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

import java.util.function.UnaryOperator;

import org.apache.commons.lang.StringUtils;
import org.apache.shiro.authz.permission.WildcardPermission;

import org.apache.geode.cache.Region;

/**
 * ResourcePermission defines the resource, the operation, the region and the key involved in the
 * action to be authorized.
 *
 * It is passed to the SecurityManager for the implementation to decide whether to grant a user this
 * permission or not.
 */
public class ResourcePermission extends WildcardPermission {

  public static String ALL = "*";
  public static String NULL = "NULL";

  /**
   * @deprecated use ALL
   */
  public static String ALL_REGIONS = "*";
  /**
   * @deprecated use All
   */
  public static String ALL_KEYS = "*";

  public enum Resource {
    ALL, NULL, CLUSTER, DATA;

    public String getName() {
      if (this == ALL) {
        return ResourcePermission.ALL;
      }
      return name();
    }
  }

  public enum Operation {
    ALL, NULL, MANAGE, WRITE, READ;
    public String getName() {
      if (this == ALL) {
        return ResourcePermission.ALL;
      }
      return name();
    }
  }

  public enum Target {
    ALL, DISK, GATEWAY, QUERY, DEPLOY;
    public String getName() {
      if (this == ALL) {
        return ResourcePermission.ALL;
      }
      return name();
    }
  }

  // these default values are used when creating an allow-all lock around an operation
  private String resource = NULL;
  private String operation = NULL;
  private String target = ALL;
  private String key = ALL;

  public ResourcePermission() {}

  public ResourcePermission(Resource resource, Operation operation) {
    this(resource, operation, ALL, ALL);
  }

  public ResourcePermission(Resource resource, Operation operation, String target) {
    this(resource, operation, target, ALL);
  }

  public ResourcePermission(Resource resource, Operation operation, Target target) {
    this(resource, operation, target, ALL);
  }

  public ResourcePermission(Resource resource, Operation operation, Target target, String key) {
    this(resource == null ? null : resource.getName(),
        operation == null ? null : operation.getName(), target == null ? null : target.getName(),
        key);
  }

  public ResourcePermission(Resource resource, Operation operation, String target, String key) {
    this(resource == null ? null : resource.getName(),
        operation == null ? null : operation.getName(), target, key);
  }

  public ResourcePermission(String resource, String operation) {
    this(resource, operation, ALL, ALL);
  }

  public ResourcePermission(String resource, String operation, String target) {
    this(resource, operation, target, ALL);
  }

  public ResourcePermission(String resource, String operation, String target, String key) {
    // what's eventually stored are either "*", "NULL" or a valid enum except ALL.
    // Fields are never null.
    this.resource = parsePart(resource, r -> Resource.valueOf(r).getName());
    this.operation = parsePart(operation, o -> Operation.valueOf(o).getName());

    if (target != null) {
      this.target = StringUtils.stripStart(target, Region.SEPARATOR);
    }
    if (key != null) {
      this.key = key;
    }

    setParts(this.resource + ":" + this.operation + ":" + this.target + ":" + this.key, true);
  }

  private String parsePart(String part, UnaryOperator<String> operator) {
    if (part == null) {
      return NULL;
    }
    if (part.equals(ALL)) {
      return ALL;
    }
    return operator.apply(part.toUpperCase());
  }

  /**
   * Returns the resource, could be either ALL, NULL, DATA or CLUSTER
   */
  public Resource getResource() {
    if (ALL.equals(resource)) {
      return Resource.ALL;
    }
    return Resource.valueOf(resource);
  }

  /**
   * Returns the operation, could be either ALL, NULL, MANAGE, WRITE or READ
   */
  public Operation getOperation() {
    if (ALL.equals(operation)) {
      return Operation.ALL;
    }
    return Operation.valueOf(operation);
  }


  /**
   * could be either "*", "NULL", "DATA", "CLUSTER"
   */
  public String getResourceString() {
    return resource;
  }

  /**
   * Returns the operation, could be either "*", "NULL", "MANAGE", "WRITE" or "READ"
   */
  public String getOperationString() {
    return operation;
  }

  /**
   * returns the regionName, or cluster target, could be "*", meaning all regions or all targets
   */
  public String getTarget() {
    return target;
  }

  /**
   * @deprecated use getTarget()
   */
  public String getRegionName() {
    return getTarget();
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
      return resource + ":" + operation;
    } else if (ALL.equals(key)) {
      return resource + ":" + operation + ":" + target;
    } else {
      return resource + ":" + operation + ":" + target + ":" + key;
    }
  }

}
