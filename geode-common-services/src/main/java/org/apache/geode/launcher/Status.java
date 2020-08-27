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
 */
package org.apache.geode.launcher;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;

/**
 * The Status enumerated type represents the various lifecycle states of a GemFire service (such as
 * a Cache Server, a Locator or a Manager).
 */
public enum Status {
  NOT_RESPONDING("not responding"),
  ONLINE("online"),
  STARTING("starting"),
  STOPPED("stopped");

  private final String description;

  Status(final String description) {
    assert isNotBlank(description) : "The Status description must be specified!";
    this.description = lowerCase(description);
  }

  /**
   * Looks up the Status enum type by description. The lookup operation is case-insensitive.
   *
   * @param description a String value describing the Locator's status.
   * @return a Status enumerated type matching the description.
   */
  public static Status valueOfDescription(final String description) {
    for (Status status : values()) {
      if (status.getDescription().equalsIgnoreCase(description)) {
        return status;
      }
    }

    return null;
  }

  /**
   * Gets the description of the Status enum type.
   *
   * @return a String describing the Status enum type.
   */
  public String getDescription() {
    return description;
  }

  /**
   * Gets a String representation of the Status enum type.
   *
   * @return a String representing the Status enum type.
   * @see #getDescription()
   */
  @Override
  public String toString() {
    return getDescription();
  }
}
