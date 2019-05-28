package org.apache.geode.management.internal.rest.controllers;

import static org.apache.geode.cache.configuration.GatewayReceiverConfig.GATEWAY_RECEIVERS_ENDPOINTS;
import static org.apache.geode.management.internal.rest.controllers.AbstractManagementController.MANAGEMENT_API_VERSION;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.management.api.ClusterManagementResult;

@Controller("gatewayManagement")
@RequestMapping(MANAGEMENT_API_VERSION)
public class GatewayManagementController extends AbstractManagementController {

  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @RequestMapping(method = RequestMethod.GET, value = GATEWAY_RECEIVERS_ENDPOINTS)
  @ResponseBody
  public ClusterManagementResult listGatewayReceivers(
      @RequestParam(required = false) String group) {
    GatewayReceiverConfig filter = new GatewayReceiverConfig();
    if (StringUtils.isNotBlank(group)) {
      filter.setGroup(group);
    }
    return clusterManagementService.list(filter);
  }
}
