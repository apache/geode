/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.management.internal.rest.controllers;

import static org.apache.geode.management.internal.rest.controllers.AbstractManagementController.MANAGEMENT_API_VERSION;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.geode.management.api.CliResponse;

@Controller("cli")
@RequestMapping(MANAGEMENT_API_VERSION)
public class GenericCLIController extends AbstractManagementController{

  private static Map<String, CliResponse> commandsToCliResponse = new HashMap<String, CliResponse>(){{
    put("list_regions", new CliResponse("/v2/regions", "get"));
    put("list_members", new CliResponse("/v2/members", "get"));
  }};

  @RequestMapping(method = RequestMethod.GET, value = "/cli" )
  @ResponseBody
  public CliResponse get(@RequestParam String command) {
    return commandsToCliResponse.get(command);
  }
}


