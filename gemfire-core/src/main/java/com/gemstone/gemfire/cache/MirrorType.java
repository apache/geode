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
 * Enumerated type for region mirroring.
 *
 * @author Eric Zoerner
 *
 *
 * @see AttributesFactory#setMirrorType
 * @see RegionAttributes#getMirrorType
 *
 * @deprecated as of GemFire 5.0, use {@link DataPolicy} instead.
 *
 * @since 3.0
 */
@Deprecated
public class MirrorType implements java.io.Serializable {
    private static final long serialVersionUID = -6632651349646672540L;
    
    /** New entries created in other caches for this region are
     * not automatically propagated to this region in this cache.
     * @deprecated as of GemFire 5.0, use {@link DataPolicy#NORMAL} instead.
     */
    @Deprecated
    public static final MirrorType NONE = new MirrorType("NONE", DataPolicy.NORMAL);

    /**
     * New entries created in other caches for this region are
     * propagated to this region in this cache, but the value is not
     * necessarily copied to this cache with the key.
     * @deprecated as of GemFire 5.0, use {@link DataPolicy#REPLICATE} instead.
     */
    @Deprecated
    public static final MirrorType KEYS = new MirrorType("KEYS", DataPolicy.REPLICATE);

    /**
     * New entries created in other caches for this region
     * are propagated to this region in this cache and the value
     * is also copied to this cache.
     * @deprecated as of GemFire 5.0, use {@link DataPolicy#REPLICATE} instead.
     */
    @Deprecated
    public static final MirrorType KEYS_VALUES = new MirrorType("KEYS_VALUES", DataPolicy.REPLICATE);
    
    
    /** The name of this mirror type. */
    private final transient String name;

    /**
     * The data policy that corresponds to this mirror type.
     */
    private final transient DataPolicy dataPolicy;
    
        // The 4 declarations below are necessary for serialization
    /** int used as ordinal to represent this Scope */
    public final int ordinal = nextOrdinal++;

    private static int nextOrdinal = 0;
    
    private static final MirrorType[] VALUES =
      { NONE, KEYS, KEYS_VALUES };

    private Object readResolve() throws ObjectStreamException {
      return VALUES[ordinal];  // Canonicalize
    }
    
    
    /** Creates a new instance of MirrorType. */
    private MirrorType(String name, DataPolicy dataPolicy) {
        this.name = name;
        this.dataPolicy = dataPolicy;
    }
    
    /** Return the MirrorType represented by specified ordinal */
    public static MirrorType fromOrdinal(int ordinal) {
      return VALUES[ordinal];
    }
    

    /**
     * Returns the {@link DataPolicy} that corresponds to this mirror type.
     * @since 5.0
     */
    public DataPolicy getDataPolicy() {
      return this.dataPolicy;
    }
  
    /** Return whether this is <code>KEYS</code>. */
    public boolean isKeys() {
      return this == KEYS;
    }
    
    /** Return whether this is <code>KEYS_VALUES</code>. */
    public boolean isKeysValues() {
      return this == KEYS_VALUES;
    }
    
    /** Return whether this is <code>NONE</code>. */
    public boolean isNone() {
      return this == NONE;
    }
    
    /** Return whether this indicates a mirrored type.
     * @return true if <code>KEYS</code> or <code>KEYS_VALUES</code>
     */
    public boolean isMirrored() {
      return this != NONE;
    }
    
    /** Returns a string representation for this mirror type.
     * @return the name of this mirror type
     */
    @Override
    public String toString() {
        return this.name;
    }
}
