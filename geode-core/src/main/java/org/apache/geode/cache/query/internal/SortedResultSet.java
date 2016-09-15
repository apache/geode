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

package org.apache.geode.cache.query.internal;

import java.util.*;
import java.io.*;
import org.apache.geode.*;
import org.apache.geode.cache.query.*;
import org.apache.geode.cache.query.types.*;
import org.apache.geode.cache.query.internal.types.*;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

/**
 * Implementation of SelectResults that extends TreeSet This is the sorted
 * version of ResultSet used for order by clause If the elements are Structs,
 * then use SortedStructSet instead.
 * 
 * @since GemFire 4.0
 */
public final class SortedResultSet extends TreeSet implements SelectResults, Ordered, 
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
    return new CollectionTypeImpl(SortedResultSet.class, this.elementType);
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

  @Override
  public boolean dataPreordered() {    
    return false;
  }
}
