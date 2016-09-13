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
package com.gemstone.gemfire.internal.cache;

/**
 * Enumerated type for an event operation.
 * 
 * 
 * 
 * @see com.gemstone.gemfire.cache.Operation
 * 
 * @since GemFire 6.6
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
