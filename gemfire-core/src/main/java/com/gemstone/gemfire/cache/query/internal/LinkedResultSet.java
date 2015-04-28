/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.internal.types.CollectionTypeImpl;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public class LinkedResultSet extends java.util.LinkedHashSet implements
    SelectResults, DataSerializableFixedID {

  private static final long serialVersionUID = 5184711453750319225L;

  private ObjectType elementType;

  public LinkedResultSet() {
  }

  LinkedResultSet(Collection c) {
    super(c);
  }

  LinkedResultSet(SelectResults sr) {
    super(sr);
    setElementType(sr.getCollectionType().getElementType());
  }

  LinkedResultSet(ObjectType elementType) {
    super();
    setElementType(elementType);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LinkedResultSet)) {
      return false;
    }
    if (!this.elementType.equals(((LinkedResultSet)other).elementType)) {
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
    return new CollectionTypeImpl(LinkedHashSet.class, this.elementType);
  }

  public boolean isModifiable() {
    return true;
  }

  public int occurrences(Object element) {
    return contains(element) ? 1 : 0;
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    int size = in.readInt();
    this.elementType = (ObjectType)DataSerializer.readObject(in);
    for (int j = size; j > 0; j--) {
      this.add(DataSerializer.readObject(in));
    }
  }
  
  public void toData(DataOutput out) throws IOException
  {    
    // how do we serialize the comparator?
    out.writeInt(this.size());
    DataSerializer.writeObject(this.elementType, out);
    for (Iterator i = this.iterator(); i.hasNext();) {
      DataSerializer.writeObject(i.next(), out);
    }
  }

  public int getDSFID() {
    
    return LINKED_RESULTSET;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

}
