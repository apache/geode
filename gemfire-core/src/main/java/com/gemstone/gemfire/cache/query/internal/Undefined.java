/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: Undefined.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;

import java.io.*;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * An UNDEFINED value is the result of accessing an attribute of a null-valued
 * attribute. If you access an attribute that has an explicit value of null,
 * then it is not undefined. For example, if a query accesses the attribute
 * address.city and address is null, then the result is undefined. If the query
 * accesses address, then the result is not undefined, it is null.
 * 
 * @version $Revision: 1.1 $
 * @author ericz
 * 
 */

public final class Undefined implements DataSerializableFixedID, Comparable {

  public Undefined() {
    Support.assertState(QueryService.UNDEFINED == null,
        "UNDEFINED constant already instantiated");

    // com.gemstone.persistence.CanonicalizationHelper
    // .putCanonicalObj("com/gemstone/persistence/query/QueryService.UNDEFINED",
    // this);
  }

  @Override
  public String toString() {
    return "UNDEFINED";
  }

  public int getDSFID() {
    return UNDEFINED;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // Serialized simply as a one-byte class id, as a class well known to
    // DataSerializer
  }

  public void toData(DataOutput out) throws IOException {
  }

  @Override
  public int compareTo(Object o) {
    // An Undefined is equal to other Undefined and less than any other object
    if (o instanceof Undefined) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof Undefined);
  }

    @Override
    public Version[] getSerializationVersions() {
       return null;
    }

}
