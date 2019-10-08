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

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.internal.Links;

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

  // we will always have statusCode when the object is created
  protected StatusCode statusCode = StatusCode.OK;
  private String statusMessage;
  private Map<String, String> links = new LinkedHashMap<>();

  @JsonIgnore
  private boolean topLevel = true;

  /**
   * for internal use only
   */
  @JsonIgnore
  protected void setTopLevel(boolean topLevel) {
    this.topLevel = topLevel;
  }

  /**
   * for internal use only
   */
  public ClusterManagementResult() {}

  /**
   * for internal use only
   */
  public ClusterManagementResult(StatusCode statusCode, String message) {
    setStatus(statusCode, message);
  }

  /**
   * for internal use only
   */
  public ClusterManagementResult(ClusterManagementResult copyFrom) {
    this.statusCode = copyFrom.statusCode;
    this.statusMessage = copyFrom.statusMessage;
    this.links = copyFrom.links;
  }

  /**
   * for internal use only
   */
  public void setStatus(StatusCode statusCode, String message) {
    this.statusCode = statusCode;
    this.statusMessage = formatErrorMessage(message);
  }

  /**
   * Capitalized the first letter and adds a period, if needed.
   * This helps the CMS API give a consistent format to error messages, even when they sometimes
   * come from parts of the system beyond our control.
   */
  private static String formatErrorMessage(String message) {
    if (isBlank(message)) {
      return message;
    }
    message = message.trim();

    // capitalize the first letter
    char firstChar = message.charAt(0);
    char newFirstChar = Character.toUpperCase(firstChar);
    if (newFirstChar != firstChar) {
      message = newFirstChar + message.substring(1);
    }

    // add a period if the sentence is not already punctuated
    if (!message.endsWith(".") && !message.endsWith("!") && !message.endsWith("?")) {
      message += ".";
    }

    return message;
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
  @JsonIgnore
  public String getUri() {
    return (links == null) ? null : links.get(Links.SELF);
  }

  /**
   * for internal use only
   */
  public void setUri(RestfulEndpoint config) {
    this.links = Links.singleItem(config);
  }

  /**
   * Returns the HATEOAS rel links for each static configuration
   */
  @JsonProperty(value = "_links")
  public Map<String, String> getLinks() {
    if (topLevel) {
      Links.addApiRoot(links);
    }
    return links;
  }

  /**
   * for internal use only
   */
  @JsonProperty(value = "_links")
  public void setLinks(Map<String, String> links) {
    this.links = links;
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

  /**
   * Returns the status code and message
   */
  @Override
  public String toString() {
    if (isBlank(getStatusMessage())) {
      return getStatusCode().toString();
    } else {
      return getStatusCode() + ": " + getStatusMessage();
    }
  }
}
