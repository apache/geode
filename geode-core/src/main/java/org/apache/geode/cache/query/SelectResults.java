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

package com.gemstone.gemfire.cache.query;

import java.util.*;
import com.gemstone.gemfire.cache.query.types.*;

/**
 * Contains the results of a {@linkplain com.gemstone.gemfire.cache.query.Query#execute() executing} a
 * <code>SELECT</code> expression within a query. A <code>SELECT</code>
 * expression results in <code>SelectResults</code> that contain instances of
 * {@link Struct} if: (a) there is more than one projection in the projection
 * attributes, or (b) if the projection is <code>*</code> and there is more than
 * one collection specified in the <code>FROM</code> clause.<p>
 *
 * Otherwise, a <code>SELECT</code> expression over a collection of domain
 * objects results in <code>SelectResults</code> that contain domain objects,
 * i.e. instances of domain classes such as {@link String} or
 * <code>Address</code>.<p>
 *
 * <pre>
 * QueryService qs = cacheView.getQueryService();
 *
 * String select = "SELECT DISTINCT * FROM /root/employees " +
 *   "WHERE salary > 50000";
 * Query query = qs.newQuery(select);
 * SelectResults results = query.execute();
 *
 * for (Iterator iter = results.iterator(); iter.hasNext(); ) {
 *   Employee emp = (Employee) iter.next();
 *   System.out.println("Highly compensated: " + emp);
 * }
 *
 * select = "SELECT DISTINCT age, address.zipCode FROM /root/employees " +
 *    "WHERE salary > 50000";
 * query = qs.newQuery(select);
 * results = query.execute();
 *
 * for (Iterator iter = results.iterator(); iter.hasNext(); ) {
 *   Struct struct = (Struct) iter.next();
 *   int age = ((Integer) struct.get("age")).intValue();
 *   String zipCode = (String) struct.get("zipCode");
 *   System.out.println(age + " -> " + zipCode);
 * }
 *
 * </pre>
 *
 * @see com.gemstone.gemfire.cache.query.Query#execute()
 *
 * @since GemFire 4.0
 */
public interface SelectResults<E> extends Collection<E> {  
  
  /**
   * Return whether this collection is modifiable. The result of this
   * method has no bearing on whether the elements in the collection themselves
   * are modifiable.
   * @return true if this collection is modifiable, false if not.
   */
  public boolean isModifiable();
  
  /**
   * Return the number of times element occurs in this collection, that is
   * the number of duplicates <code>element</code> has in this collection as defined by
   * the <code>equals></code> method. If <code>element</code> is not present in this
   * collection, then 0 is returned.
   * @param element the element
   * @return the number of occurrances of element
   * @since GemFire 5.1
   */
  public int occurrences(E element);
  
  /**
   * Returns this <code>SelectResults</code> as a
   * <code>java.util.Set</code>. If this collection is 
   * distinct and unordered, then no copying is necessary. Otherwise, the
   * contents of this collection will be copied into a new instance of
   * java.util.HashSet.
   * @return Is this collection as a <code>java.util.Set</code>?
   */
  public Set<E> asSet();
  
  /**
   * Returns this <code>SelectedResults</code> as a
   * <code>java.util.List</code>. If this collection is
   * ordered, then no copying is necessary. Otherwise, the
   * contents of this collection will be copied into a new instance of
   * java.util.ArrayList.
   * @return this collection as a java.util.List
   */
  public List<E> asList();
  
  /** Return the ObjectType for the type of collection this represents.
   *  @return the CollectionType for the type of collection this represents
   */
  public CollectionType getCollectionType();  
  
  /**
   * Specify a new elementType, overriding any existing known elementType.
   * This modifies the CollectionType for this object to be the same collection type
   * but with the newly specified element type.
   * @param elementType the new elementType
   */
  public void setElementType(ObjectType elementType);
  
}
