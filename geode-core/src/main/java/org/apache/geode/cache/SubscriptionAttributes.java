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
package org.apache.geode.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.geode.DataSerializable;

/**
 * Configuration attributes for defining subscriber requirements and behavior for a
 * <code>Region</code>.
 *
 * <p>
 * The {@link InterestPolicy} defines what remote operation's data/event are of interest to this
 * cache's region.
 * </p>
 *
 * @since GemFire 5.0
 */
public class SubscriptionAttributes implements DataSerializable, Externalizable {

  /**
   * this subscriber's interest policy
   */
  private /* final */ InterestPolicy interestPolicy;

  /**
   * Creates a new <code>SubscriptionAttributes</code> with the default configuration
   */
  public SubscriptionAttributes() {
    this.interestPolicy = InterestPolicy.DEFAULT;
  }

  /**
   * Creates a new <code>SubscriptionAttributes</code> with the given interest policy.
   *
   * @param interestPolicy the interest policy this subscriber will use
   */
  public SubscriptionAttributes(InterestPolicy interestPolicy) {
    this.interestPolicy = interestPolicy;
  }

  /**
   * Returns the interest policy of this subscriber.
   */
  public InterestPolicy getInterestPolicy() {
    return this.interestPolicy;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (!(other instanceof SubscriptionAttributes)) {
      return false;
    }
    final SubscriptionAttributes that = (SubscriptionAttributes) other;

    if (this.interestPolicy != that.interestPolicy
        && !(this.interestPolicy != null && this.interestPolicy.equals(that.interestPolicy))) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;

    result = mult * result + this.interestPolicy.hashCode();

    return result;
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append("InterestPolicy=");
    sb.append(this.interestPolicy.toString());
    return sb.toString();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeByte(this.interestPolicy.ordinal);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.interestPolicy = InterestPolicy.fromOrdinal(in.readByte());
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // added to fix bug 36619
    toData(out);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // added to fix bug 36619
    fromData(in);
  }
}
