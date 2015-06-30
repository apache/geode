package com.gemstone.gemfire.cache.query.internal;

import java.util.Iterator;

import com.gemstone.gemfire.cache.query.types.CollectionType;

/**
 * This interface is to be implemented by all SelectResults implementation which
 * can hold struct using field values array ( Object[]) 
 * 
 * @see SortedStructSet
 * @see StructSet
 * @see StructBag
 * @see SortedStructBag
 * 
 * @author ashahid
 *
 */
public interface StructFields {

  public boolean addFieldValues(Object[] fieldValues);
  public boolean removeFieldValues(Object[] fieldValues);
  public Iterator fieldValuesIterator();
  public CollectionType getCollectionType();
  public boolean containsFieldValues(Object[] fieldValues);
}
