/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.rest.internal.web.controllers.support;

/**
 * The QueryResultTypes type describes possible query result types 
 * <p/>
 * @author Nilkanth Patel
 * @since 8.0
 */

public enum QueryResultTypes {
  OBJECT_COLLECTION,
  PDX_COLLECTION,
  STRUCT_COLLECTION,
  NESTED_COLLECTION
}
