/*=========================================================================
 * Copyright (c) 2011-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

/**
 * Marker interface for object used in PdxSerializer Tests that are in the
 * com.gemstone package. If an object implements this interface, it will be
 * passed to a PdxSerializer even if it is in the com.gemstone package.
 * 
 * This is necessary because we exclude all other objects from the com.gemstone
 * package.
 * See {@link InternalDataSerializer#writePdx(java.io.DataOutput, com.gemstone.gemfire.internal.cache.GemFireCacheImpl, Object, com.gemstone.gemfire.pdx.PdxSerializer)} 
 * 
 * @author dsmith
 * 
 */
public interface PdxSerializerObject {

}
