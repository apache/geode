/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * IndexCreationHelper.java
 *
 * Created on March 16, 2005, 6:20 PM
 */
package com.gemstone.gemfire.cache.query.internal.index;

import com.gemstone.gemfire.cache.query.internal.ExecutionContext;


/**
 * Interface to support plug-able hashing strategies in maps and sets.
 * Implementors can use this interface to make the hashing
 * algorithms use object values, values provided by the java runtime,
 * or a custom strategy when computing hash codes.
 *
 */

public interface HashIndexStrategy {
    
    /**
     * Computes a hash code for the specified object.  Implementors
     * can use the object's own <tt>hashCode</tt> method, the Java
     * runtime's <tt>identityHashCode</tt>, or a custom scheme.
     * 
     * @param o object for which the hashcode is to be computed
     * @return the hashCode
     */
    public int computeHashCode(Object o);
    

    /**
     * VMware Addition
     * Computes a hash code for the specified object.  Implementors
     * can use the object's own <tt>hashCode</tt> method, the Java
     * runtime's <tt>identityHashCode</tt>, or a custom scheme.
     * Used when resizing the internal set structure.  Due to not storing
     * the indexKey, we have to recompute the indexKey from the object
     * at this point.
     * @param o object for which the hashcode is to be computed
     * @param recomputeKey 
     * @return the hashCode
     */
    public int computeHashCode(Object o, boolean recomputeKey);
    
    /**
     * VMware Addition
     * Computes the object's key
     * @param o object for which the key is to be computed
     * @return the key
     */
    public Object computeKey(Object o);

    /**
     * Compares o1 and o2 for equality.  Strategy implementors may use
     * the objects' own equals() methods, compare object references,
     * or implement some custom scheme.
     *
     * @param o1 an <code>Object</code> value
     * @param o2 an <code>Object</code> value
     * @return true if the objects are equal according to this strategy.
     */
    public boolean equalsOnAdd(Object o1, Object o2);
    
    /**
     * Compares o1 and o2 for equality.  Strategy implementors may use
     * the objects' own equals() methods, compare object references,
     * or implement some custom scheme.
     *
     * @return true if the objects are equal according to this strategy.
     */
    public boolean equalsOnGet(Object getValue, Object o);
    
} 
