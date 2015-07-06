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

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.vmware.gemfire.tools.pulse.internal.controllers.PulseController;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;

/**
 * Class PulseVersionService
 * 
 * This class contains implementations of getting Pulse Applications Version's
 * details (like version details, build details, source details, etc) from
 * properties file
 * 
 * @author Sachin K
 * @since version 7.0.Beta
 */

@Component
@Service("PulseVersion")
@Scope("singleton")
public class PulseVersionService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {
      // Response
      responseJSON.put("pulseVersion",
          PulseController.pulseVersion.getPulseVersion());
      responseJSON.put("buildId",
          PulseController.pulseVersion.getPulseBuildId());
      responseJSON.put("buildDate",
          PulseController.pulseVersion.getPulseBuildDate());
      responseJSON.put("sourceDate",
          PulseController.pulseVersion.getPulseSourceDate());
      responseJSON.put("sourceRevision",
          PulseController.pulseVersion.getPulseSourceRevision());
      responseJSON.put("sourceRepository",
          PulseController.pulseVersion.getPulseSourceRepository());
    } catch (JSONException e) {
      throw new Exception(e);
    }
    // Send json response
    return responseJSON;
  }

}
