/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */


package com.gemstone.gemfire.cache;

import java.io.*;

/**
 * Enumerated type for expiration actions.
 *
 * @author Eric Zoerner
 *
 *
 * @see ExpirationAttributes
 * @since 3.0
 */
public class ExpirationAction implements Serializable {
    private static final long serialVersionUID = 658925707882047900L;
    
    /** When the region or cached object expires, it is invalidated. */
    public final static ExpirationAction INVALIDATE = new ExpirationAction("INVALIDATE");
    /** When expired, invalidated locally only. Not supported for partitioned regions. */
    public final static ExpirationAction LOCAL_INVALIDATE = new ExpirationAction("LOCAL_INVALIDATE");
    
    /** When the region or cached object expires, it is destroyed. */
    public final static ExpirationAction DESTROY = new ExpirationAction("DESTROY");
    /** When expired, destroyed locally only. Not supported for partitioned regions. Use DESTROY instead. */
    public final static ExpirationAction LOCAL_DESTROY = new ExpirationAction("LOCAL_DESTROY");
    
    /** The name of this action */
    private final transient String name;
    
    /** Creates a new instance of ExpirationAction.
     * 
     * @param name the name of the expiration action
     * @see #toString
     */
    private ExpirationAction(String name) {
        this.name = name;
    }
    
    /**
     * Returns whether this is the action for distributed invalidate.
     * @return true if this in INVALIDATE
     */
    public boolean isInvalidate() {
      return this == INVALIDATE;
    }
    
    /**
     * Returns whether this is the action for local invalidate.
     * @return true if this is LOCAL_INVALIDATE
     */
    public boolean isLocalInvalidate() {
      return this == LOCAL_INVALIDATE;
    }
    
    /** Returns whether this is the action for distributed destroy.
     * @return true if this is DESTROY
     */
    public boolean isDestroy() {
      return this == DESTROY;
    }
    
    /** Returns whether this is the action for local destroy.
     * @return true if thisis LOCAL_DESTROY
     */
    public boolean isLocalDestroy() {
      return this == LOCAL_DESTROY;
    }
    
    /** Returns whether this action is local.
     * @return true if this is LOCAL_INVALIDATE or LOCAL_DESTROY
     */
    public boolean isLocal() {
      return this == LOCAL_INVALIDATE || this == LOCAL_DESTROY;
    }
    
    /** Returns whether this action is distributed.
     * @return true if this is INVALIDATE or DESTROY
     */
    public boolean isDistributed() {
      return !isLocal();
    }    
        
    /** Returns a string representation for this action
     * @return the name of this action
     */
    @Override
    public String toString() {
        return this.name;
    }

    // The 4 declarations below are necessary for serialization
    private static int nextOrdinal = 0;
    public final int ordinal = nextOrdinal++;
    private static final ExpirationAction[] VALUES =
      { INVALIDATE, LOCAL_INVALIDATE, DESTROY, LOCAL_DESTROY};
    private Object readResolve() throws ObjectStreamException {
      return fromOrdinal(ordinal);  // Canonicalize
    }

    /** Return the ExpirationAction represented by specified ordinal */
    public static ExpirationAction fromOrdinal(int ordinal) {
      return VALUES[ordinal];
    }

}
