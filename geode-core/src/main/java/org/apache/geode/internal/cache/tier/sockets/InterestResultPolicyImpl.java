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


package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

import java.io.*;

/**
 * Used to make InterestResultPolicy implement DataSerializableFixedID
 *
 *
 * @since GemFire 5.7
 */
public final class InterestResultPolicyImpl extends InterestResultPolicy
  implements DataSerializableFixedID {
  private static final long serialVersionUID = -7456596794818237831L;
  /** Should only be called by static field initialization in InterestResultPolicy */
  public InterestResultPolicyImpl(String name) {
    super(name);
  }

  public int getDSFID() {
    return INTEREST_RESULT_POLICY;
  }

  public void toData(DataOutput out) throws IOException {
    out.writeByte(getOrdinal());
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // should never be called since DSFIDFactory.readInterestResultPolicy is used
    throw new UnsupportedOperationException();
  }

  @Override
  public Version[] getSerializationVersions() {
     return null;
  }
}
