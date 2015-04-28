/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

/**
 * Enumerated type for an event operation.
 * 
 * @author Amey Barve
 * 
 * 
 * @see com.gemstone.gemfire.cache.Operation
 * 
 * @since 6.6
 */
public final class OpType {

	private OpType() {
	}

	public static final byte CREATE = 1;

	public static final byte UPDATE = 2;

	public static final byte GET = 3;

	public static final byte INVALIDATE = 4;

	public static final byte GET_ENTRY = 5;

	public static final byte CONTAINS_KEY = 6;

	public static final byte CONTAINS_VALUE = 7;

	public static final byte DESTROY = 8;

	public static final byte CONTAINS_VALUE_FOR_KEY = 9;

	public static final byte FUNCTION_EXECUTION = 10;

        public static final byte UPDATE_ENTRY_VERSION = 11;

	public static final byte CLEAR = 16;

	public static final byte MARKER = 32;
}
