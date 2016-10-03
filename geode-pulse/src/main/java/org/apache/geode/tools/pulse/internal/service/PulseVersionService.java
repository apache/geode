/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.geode.tools.pulse.internal.controllers.PulseController;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;

/**
 * Class PulseVersionService
 * 
 * This class contains implementations of getting Pulse Applications Version's
 * details (like version details, build details, source details, etc) from
 * properties file
 * 
 * @since GemFire version 7.0.Beta
 */

@Component
@Service("PulseVersion")
@Scope("singleton")
public class PulseVersionService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    // Response
    responseJSON.put("pulseVersion", PulseController.pulseVersion.getPulseVersion());
    responseJSON.put("buildId", PulseController.pulseVersion.getPulseBuildId());
    responseJSON.put("buildDate", PulseController.pulseVersion.getPulseBuildDate());
    responseJSON.put("sourceDate", PulseController.pulseVersion.getPulseSourceDate());
    responseJSON.put("sourceRevision", PulseController.pulseVersion.getPulseSourceRevision());
    responseJSON.put("sourceRepository", PulseController.pulseVersion.getPulseSourceRepository());

    // Send json response
    return responseJSON;
  }

}
