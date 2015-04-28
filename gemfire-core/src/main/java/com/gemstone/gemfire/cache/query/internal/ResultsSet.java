/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.types.*;
import com.gemstone.gemfire.cache.query.internal.types.*;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

// @todo probably should assert element type when elements added
/**
 * Implementation of SelectResults that extends HashSet
 * If the elements are Structs, then use a StructSet instead.
 *
 * @author Eric Zoerner
 * @since 4.0
 */
public final class ResultsSet  extends HashSet implements SelectResults, DataSerializableFixedID {
  private static final long serialVersionUID = -5423281031630216824L;
  private ObjectType elementType;
  
  /**
   * Empty constructor to satisfy <code>DataSerializer</code> requirements
   */
  public ResultsSet() {
  }
  
  ResultsSet(Collection c) {
    super(c);
  }
  
  public ResultsSet(SelectResults sr) {
    super(sr);
    // grab type info
    setElementType(sr.getCollectionType().getElementType());
  }
  
  ResultsSet(ObjectType elementType) {
    super();
    setElementType(elementType);
  }
  
  ResultsSet(ObjectType elementType, int initialCapacity) {
    super(initialCapacity);
    setElementType(elementType);
  }
  
  ResultsSet(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
  }
  
  ResultsSet(int initialCapacity) {
    super(initialCapacity);
  }  
  
  public void setElementType(ObjectType elementType) {
    if (elementType instanceof StructType)
      throw new IllegalArgumentException(LocalizedStrings.ResultsSet_THIS_COLLECTION_DOES_NOT_SUPPORT_STRUCT_ELEMENTS.toLocalizedString());
    this.elementType = elementType;
  }
    
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ResultsSet)) {
      return false;
    }
    if (!this.elementType.equals(((ResultsSet)other).elementType)) {
      return false;
    }
    return super.equals(other);
  }
  
  
  public List asList() {
    return new ArrayList(this);
  }
  
  public Set asSet() {
    return this;
  }
  
  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(Set.class, this.elementType);
  }
  
  public boolean isModifiable() {
    return true;
  }
    
  public int occurrences(Object element) {
    return contains(element) ? 1 : 0;
  }
  
  
  public int getDSFID() {
    return RESULTS_SET;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    ObjectTypeImpl clt = new ObjectTypeImpl();
    InternalDataSerializer.invokeFromData(clt, in);
    setElementType(clt);                
    for(int k=size; k>0; k--) {
      this.add(DataSerializer.readObject(in));
    }
  }
  
  public void toData(DataOutput out) throws IOException {
    out.writeInt(size());
    ObjectTypeImpl ctImpl = (ObjectTypeImpl)this.getCollectionType()
      .getElementType();
    Assert.assertTrue(ctImpl != null, "ctImpl can not be null");
    InternalDataSerializer.invokeToData(ctImpl, out);
    for (Iterator itr = this.iterator(); itr.hasNext();) {
      DataSerializer.writeObject(itr.next(), out);
    }
  }

  @Override
  public Version[] getSerializationVersions() {
     return null;
  }   
}
