/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.data;

import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;

/**
 * Interface having updateData() function which is getting Override by both
 * MockDataUpdater and JMXDataUpdater
 * 
 * @author Anand Hariharan
 * @since  version 7.0.Beta 2012-09-23 
 * 
 */
public interface IClusterUpdater {
  boolean updateData();

  JSONObject executeQuery(String queryText, String members, int limit) throws JSONException;
}
