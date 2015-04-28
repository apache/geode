/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: PlanInfo.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

/**
 * Encapsulates evaluation info about compiled values
 * 
 * @version $Revision: 1.1 $
 * @author ericz
 * @author asif
 * @since 4.1
 */

import java.util.List;
import java.util.ArrayList;

class PlanInfo  {

  boolean evalAsFilter = false;
  boolean isPreferred = false;
  List indexes = new ArrayList();
}
