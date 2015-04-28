/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.DataSerializable;
import java.io.*;

/**
 * Configuration attributes for defining subscriber requirements and behavior
 * for a <code>Region</code>.
 * 
 * <p>The {@link InterestPolicy} defines what remote operation's data/event
 * are of interest to this cache's region.</p>
 * 
 * @author Darrel
 * @since 5.0
 */
public class SubscriptionAttributes implements DataSerializable, Externalizable {
  
  /** 
   * this subscriber's interest policy
   */
  private /*final*/ InterestPolicy interestPolicy;

  /**
   * Creates a new <code>SubscriptionAttributes</code> with the default
   * configuration
   */
  public SubscriptionAttributes() {
    this.interestPolicy = InterestPolicy.DEFAULT;
  }
  /**
   * Creates a new <code>SubscriptionAttributes</code> with the given
   * interest policy.
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
    if (other == this) return true;
    if (other == null) return false;
    if (!(other instanceof SubscriptionAttributes)) return  false;
    final SubscriptionAttributes that = (SubscriptionAttributes) other;

    if (this.interestPolicy != that.interestPolicy &&
        !(this.interestPolicy != null &&
          this.interestPolicy.equals(that.interestPolicy))) return false;

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
  
  public void toData(DataOutput out) throws IOException {
    out.writeByte(this.interestPolicy.ordinal);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    this.interestPolicy = InterestPolicy.fromOrdinal(in.readByte());
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    // added to fix bug 36619
    toData(out);
  }
  
  public void readExternal(ObjectInput in)
    throws IOException, ClassNotFoundException {
    // added to fix bug 36619
    fromData(in);
  }
}
