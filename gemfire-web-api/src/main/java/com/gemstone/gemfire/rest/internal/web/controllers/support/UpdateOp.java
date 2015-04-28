/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.rest.internal.web.controllers.support;

/**
 * UpdateOp contains all posible update operation supported with REST APIs
 * @author Nilkanth Patel
 */

@SuppressWarnings("unused")
public enum UpdateOp {
  CAS,
  PUT,
  REPLACE
}

