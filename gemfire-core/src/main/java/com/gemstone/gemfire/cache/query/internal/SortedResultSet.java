/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;

import java.util.*;
import java.io.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.types.*;
import com.gemstone.gemfire.cache.query.internal.types.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * Implementation of SelectResults that extends TreeSet This is the sorted
 * version of ResultSet used for order by clause If the elements are Structs,
 * then use SortedStructSet instead.
 * 
 * @author Yogesh Mahajan
 * @since 4.0
 */
public final class SortedResultSet extends TreeSet implements SelectResults,
    DataSerializableFixedID {
  private static final long serialVersionUID = 5184711453750319224L;

  private ObjectType elementType;

  public SortedResultSet() {
  }

  SortedResultSet(Collection c) {
    super(c);
  }

  SortedResultSet(SelectResults sr) {
    super(sr);
    setElementType(sr.getCollectionType().getElementType());
  }

  SortedResultSet(ObjectType elementType) {
    super();
    setElementType(elementType);
  }

  SortedResultSet(Comparator c) {
    super(c);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SortedResultSet)) {
      return false;
    }
    if (!this.elementType.equals(((SortedResultSet)other).elementType)) {
      return false;
    }
    return super.equals(other);
  }

  public void setElementType(ObjectType elementType) {
    if (elementType instanceof StructType)
      throw new IllegalArgumentException(
          LocalizedStrings.SortedResultSet_THIS_COLLECTION_DOES_NOT_SUPPORT_STRUCT_ELEMENTS
              .toLocalizedString());
    this.elementType = elementType;
  }

  public List asList() {
    return new ArrayList(this);
  }

  public Set asSet() {
    return this;
  }

  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(TreeSet.class, this.elementType);
  }

  public boolean isModifiable() {
    return true;
  }

  public int occurrences(Object element) {
    return contains(element) ? 1 : 0;
  }

  public int getDSFID() {
    return SORTED_RESULT_SET;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    this.elementType = (ObjectType)DataSerializer.readObject(in);
    for (int j = size; j > 0; j--) {
      this.add(DataSerializer.readObject(in));
    }
  }

  public void toData(DataOutput out) throws IOException {
    // how do we serialize the comparator?
    out.writeInt(this.size());
    DataSerializer.writeObject(this.elementType, out);
    for (Iterator i = this.iterator(); i.hasNext();) {
      DataSerializer.writeObject(i.next(), out);
    }
  }

  @Override
  public Version[] getSerializationVersions() {
     return null;
  }
}
