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
package org.apache.geode.alerting.internal;

import org.apache.logging.log4j.Level;

import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.distributed.DistributedMember;

/**
 * Simple value object which holds a {@link DistributedMember} and {@link Level} pair.
 */
public class AlertListener {

  private final AlertLevel level;
  private final DistributedMember member;

  public AlertListener(final AlertLevel level, final DistributedMember member) {
    this.level = level;
    this.member = member;
  }

  public AlertLevel getLevel() {
    return level;
  }

  public DistributedMember getMember() {
    return member;
  }

  /**
   * Never used, but maintain the hashCode/equals contract.
   */
  @Override
  public int hashCode() {
    return 31 + (member == null ? 0 : member.hashCode());
  }

  /**
   * Ignore the level when determining equality.
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AlertListener)) {
      return false;
    }
    AlertListener other = (AlertListener) obj;
    return member.equals(other.member);
  }

  @Override
  public String toString() {
    return "Listener [level=" + level + ", member=" + member + "]";
  }
}
