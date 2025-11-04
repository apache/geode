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

import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.configuration.Links;

/**
 * This base class provides the common attributes returned from all {@link ClusterManagementService}
 * methods
 *
 * <p>
 * <strong>Implementation Note: Serializable Interface</strong>
 * </p>
 * <p>
 * This class implements {@link Serializable} to support Apache Geode's DUnit distributed testing
 * framework, which runs tests across multiple JVMs. When test methods return instances of this
 * class from VM.invoke() calls, DUnit requires the objects to be serializable for cross-JVM
 * communication via RMI.
 * </p>
 *
 * <p>
 * <strong>Why This Was Added:</strong>
 * </p>
 * <ul>
 * <li><strong>DUnit Architecture:</strong> DUnit tests execute code in separate JVM processes
 * (client VM, locator VM, server VM). When a test in one VM invokes a method in another VM
 * and that method returns a result object, DUnit serializes the object to transmit it across
 * the JVM boundary.</li>
 *
 * <li><strong>Test Pattern Change:</strong> Prior to Jetty 12 migration, authentication tests
 * expected exceptions (no return values). With Jetty 12 and Spring Security 6 integration,
 * some tests now validate successful operations and return ClusterManagementResult objects,
 * requiring serialization support.</li>
 *
 * <li><strong>Serialization Verification:</strong> DUnit's MethodInvokerResult.checkSerializable()
 * validates that all return values implement Serializable, throwing NotSerializableException
 * if they don't.</li>
 * </ul>
 *
 * <p>
 * <strong>Serialization Compatibility:</strong>
 * </p>
 * <ul>
 * <li>All fields (statusCode, statusMessage, links) are either primitives, Strings, enums, or
 * Serializable objects</li>
 * <li>The {@link Links} class also implements Serializable for complete object graph support</li>
 * <li>serialVersionUID is explicitly defined to maintain serialization compatibility across
 * code changes</li>
 * </ul>
 *
 * <p>
 * This change does not affect production usage - it only enables comprehensive testing in
 * DUnit's multi-JVM environment.
 * </p>
 */
@Experimental
public class ClusterManagementResult implements Serializable {
  /**
   * Serial version UID for serialization compatibility.
   * Required for Serializable interface to maintain compatibility across code changes.
   */
  private static final long serialVersionUID = 1L;

  /**
   * these status codes generally have a one-to-one mapping to the http status code returned by the
   * REST controller
   */
  public enum StatusCode {
    /** operation is successful, configuration is realized and persisted */
    OK,
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
    IN_PROGRESS
  }

  // we will always have statusCode when the object is created
  private StatusCode statusCode = StatusCode.OK;
  private String statusMessage;
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Links links;

  /**
   * for internal use only
   */
  public ClusterManagementResult() {}

  /**
   * for internal use only
   *
   * @param statusCode the {@link StatusCode} to set
   * @param message the status message to set
   */
  public ClusterManagementResult(StatusCode statusCode, String message) {
    setStatus(statusCode, message);
  }

  /**
   * for internal use only
   *
   * @param copyFrom the {@link ClusterManagementResult} to copy from
   */
  public ClusterManagementResult(ClusterManagementResult copyFrom) {
    statusCode = copyFrom.statusCode;
    statusMessage = copyFrom.statusMessage;
    links = copyFrom.links;
  }

  /**
   * for internal use only
   *
   * @param statusCode the {@link StatusCode} to set
   * @param message the status message to set
   */
  public void setStatus(StatusCode statusCode, String message) {
    this.statusCode = statusCode;
    statusMessage = formatErrorMessage(message);
  }

  /*
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
   *
   * @return an optional message to accompany {@link #getStatusCode()}
   */
  public String getStatusMessage() {
    return statusMessage;
  }

  /**
   * for internal use only
   *
   * @param links the {@link Links} to set
   */
  public void setLinks(Links links) {
    this.links = links;
  }

  /**
   * Returns true if {@link #getStatusCode()} has a non-error value
   *
   * @return true if {@link #getStatusCode()} has a non-error value
   */
  @JsonIgnore
  public boolean isSuccessful() {
    return statusCode == StatusCode.OK || statusCode == StatusCode.ACCEPTED
        || statusCode == StatusCode.IN_PROGRESS;
  }

  /**
   * Returns the {@link StatusCode} for this request, such as ERROR or OK.
   *
   * @return the {@link StatusCode} for this request
   */
  public StatusCode getStatusCode() {
    return statusCode;
  }

  /**
   * Returns the status code and message
   *
   * @return the status code and message as a String
   */
  @Override
  public String toString() {
    if (isBlank(getStatusMessage())) {
      return getStatusCode().toString();
    } else {
      return getStatusCode() + ": " + getStatusMessage();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusterManagementResult that = (ClusterManagementResult) o;
    return statusCode == that.statusCode &&
        Objects.equals(statusMessage, that.statusMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statusCode, statusMessage);
  }
}
