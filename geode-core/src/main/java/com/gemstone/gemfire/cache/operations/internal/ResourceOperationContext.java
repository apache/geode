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
package com.gemstone.gemfire.cache.operations.internal;

import com.gemstone.gemfire.cache.operations.OperationContext;
import org.apache.shiro.authz.permission.WildcardPermission;

/**
 * This is the base class for all {@link OperationContext}s. For JMX and CLI operations this is the
 * concrete class which is passed in to {@link com.gemstone.gemfire.security.AccessControl#authorizeOperation}.
 */
public class ResourceOperationContext extends WildcardPermission implements OperationContext {

  private boolean isPostOperation = false;
  private Object opResult = null;

  // these default values are used when creating a lock around an operation
  private final Resource resource;
  private final OperationCode operation;
  private final String regionName;

  public ResourceOperationContext() {
    this(Resource.NULL, OperationCode.NULL);
  }

  public ResourceOperationContext(Resource resource, OperationCode code, boolean isPost) {
    this(resource.toString(), code.toString(), ALL_REGIONS.toString(), isPost);
  }

  public ResourceOperationContext(Resource resource, OperationCode code) {
    this(resource.toString(), code.toString(), ALL_REGIONS.toString(), false);
  }

  // When only specified a resource and operation, it's assumed that you need access to all regions
  // in order to perform the operations guarded by this ResourceOperationContext
  public ResourceOperationContext(String resource, String operation) {
    this(resource, operation, ALL_REGIONS, false);
  }

  public ResourceOperationContext(String resource, String operation, String region) {
    this(resource, operation, region, false);
  }

  public ResourceOperationContext(String resource, String operation, String regionName, boolean isPost) {
    this((resource != null) ? Resource.valueOf(resource) : Resource.NULL,
        (operation != null) ? OperationCode.valueOf(operation) : OperationCode.NULL,
        (regionName != null) ? regionName : ALL_REGIONS, isPost);
  }

  public ResourceOperationContext(Resource resource, OperationCode operation, String regionName, boolean isPost) {
    this.resource = (resource != null) ? resource : Resource.NULL;
    String resourcePart = (this.resource != Resource.NULL) ? resource.toString() : "*";

    this.operation = (operation != null) ? operation : OperationCode.NULL;
    String operationPart = (this.operation != OperationCode.NULL) ? operation.toString() : "*";

    this.regionName = (regionName != null) ? regionName : ALL_REGIONS;
    this.isPostOperation = isPost;

    String shiroPermission = String.format("%s:%s:%s", resourcePart, operationPart, this.regionName);
    setParts(shiroPermission, true);
  }

  @Override
  public final boolean isClientUpdate() {
    return false;
  }

  @Override
  public final OperationCode getOperationCode() {
    return operation;
  }

  @Override
  public final Resource getResource() {
    return resource;
  }

  @Override
  public final String getRegionName() {
    return this.regionName;
  }

  @Override
  public final boolean isPostOperation() {
    return isPostOperation;
  }

  public final void setPostOperationResult(Object result) {
    this.isPostOperation = true;
    this.opResult = result;
  }

  /**
   * Set the post-operation flag to true.
   */
  public final void setPostOperation() {
    this.isPostOperation = true;
  }

  public final Object getOperationResult() {
    return this.opResult;
  }

  @Override
  public String toString() {
    if (ALL_REGIONS.equals(getRegionName())) {
      return getResource() + ":" + getOperationCode();
    } else {
      return getResource() + ":" + getOperationCode() + ":" + getRegionName();
    }
  }

}