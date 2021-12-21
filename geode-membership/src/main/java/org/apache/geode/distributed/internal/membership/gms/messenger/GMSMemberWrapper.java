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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;

/**
 * A wrapper for MemberIdentifier objects used in encryption services
 */
public class GMSMemberWrapper {

  MemberIdentifier mbr;

  public GMSMemberWrapper(MemberIdentifier m) {
    mbr = m;
  }

  public MemberIdentifier getMbr() {
    return mbr;
  }

  @Override
  public int hashCode() {
    return mbr.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof GMSMemberWrapper)) {
      return false;
    }
    MemberIdentifier other = ((GMSMemberWrapper) obj).mbr;
    // here we must compare member data rather than identifiers since the view identifiers and
    // UUID identifiers need to be ignored
    return mbr.getMemberData().compareTo(other.getMemberData(), false, false) == 0;
  }

  @Override
  public String toString() {
    return "GMSMemberWrapper [mbr=" + mbr + "]";
  }
}
