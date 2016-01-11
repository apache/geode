/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.service;

import javax.servlet.http.HttpServletRequest;

import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;

/**
 * Abstract class PulseService
 * 
 * This is a base class for all services in pulse.
 * 
 * @author azambare
 * @since version 7.5
 */
public interface PulseService {

  String VALUE_NA = "NA";
  String VALUE_ON = "ON";
  String VALUE_OFF = "OFF";

  JSONObject execute(HttpServletRequest httpServletRequest)
      throws Exception;
}
