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
package org.apache.geode.management.api;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheElement;

/**
 * This base class provides the common attributes returned from all {@link ClusterManagementService}
 * methods
 */
@Experimental
public class ClusterManagementResult {
  /**
   * these status codes generally have a one-to-one mapping to the http status code returned by the
   * REST controller
   */
  public enum StatusCode {
    /** configuration failed validation */
    ILLEGAL_ARGUMENT,
    /** user is not authenticated */
    UNAUTHENTICATED,
    /** user is not authorized to do this operation */
    UNAUTHORIZED,
    /** entity you are trying to create already exists */
    ENTITY_EXISTS,
    /** entity you are trying to modify/delete is not found */
    ENTITY_NOT_FOUND,
    /**
     * operation not successful, this includes precondition is not met (either service is not
     * running or no servers available, or the configuration encountered some error when trying to
     * be realized on one member (configuration is not fully realized on all applicable members).
     */
    ERROR,
    /**
     * configuration is realized on members, but encountered some error when persisting the
     * configuration. the operation is still deemed unsuccessful.
     */
    FAIL_TO_PERSIST,
    /** async operation launched successfully */
    ACCEPTED,
    /** async operation has not yet completed */
    IN_PROGRESS,
    /** operation is successful, configuration is realized and persisted */
    OK
  }

  private List<RealizationResult> memberStatuses = new ArrayList<>();

  // we will always have statusCode when the object is created
  private StatusCode statusCode = StatusCode.OK;
  private String statusMessage;
  private String uri;

  public ClusterManagementResult() {}

  public ClusterManagementResult(boolean success, String message) {
    setStatus(success, message);
  }

  public ClusterManagementResult(StatusCode statusCode, String message) {
    this.statusCode = statusCode;
    this.statusMessage = message;
  }

  public ClusterManagementResult(ClusterManagementResult copyFrom) {
    this.memberStatuses = copyFrom.memberStatuses;
    this.statusCode = copyFrom.statusCode;
    this.statusMessage = copyFrom.statusMessage;
    this.uri = copyFrom.uri;
  }

  public void addMemberStatus(String member, boolean success, String message) {
    addMemberStatus(new RealizationResult().setMemberName(member)
        .setSuccess(success).setMessage(message));
  }

  public void addMemberStatus(RealizationResult result) {
    this.memberStatuses.add(result);
    // if any member failed, status code will be error
    if (!result.isSuccess()) {
      statusCode = StatusCode.ERROR;
    }
  }

  public void setStatus(boolean success, String message) {
    if (!success) {
      statusCode = StatusCode.ERROR;
    }
    this.statusMessage = message;
  }

  public void setStatus(StatusCode code, String message) {
    this.statusCode = code;
    this.statusMessage = message;
  }

  /**
   * For a {@link ClusterManagementService#create(CacheElement)} operation, this will return
   * per-member status of the create.
   */
  public List<RealizationResult> getMemberStatuses() {
    return memberStatuses;
  }

  /**
   * Returns an optional message to accompany {@link #getStatusCode()}
   */
  public String getStatusMessage() {
    return statusMessage;
  }

  /**
   * Returns the full path (not including http://server:port) by which this result can be referenced
   * via REST
   */
  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  /**
   * Returns true if {@link #getStatusCode()} has a non-error value
   */
  @JsonIgnore
  public boolean isSuccessful() {
    return statusCode == StatusCode.OK || statusCode == StatusCode.ACCEPTED
        || statusCode == StatusCode.IN_PROGRESS;
  }

  /**
   * Returns the {@link StatusCode} for this request, such as ERROR or OK.
   */
  public StatusCode getStatusCode() {
    return statusCode;
  }
}
