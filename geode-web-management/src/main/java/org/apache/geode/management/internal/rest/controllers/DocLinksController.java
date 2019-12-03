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

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import io.swagger.annotations.ApiOperation;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController("apiDocumentation")
public class DocLinksController {

  @ApiOperation("get documentation-links")
  @GetMapping("/")
  public ResponseEntity<DocLinks> getDocumentationLinks(HttpServletRequest request) {
    DocLinks docLinks = new DocLinks();
    String baseURL = request.getRequestURL().toString();
    List<String> uriList = new ArrayList<>();
    uriList.add(baseURL + "v1/api-docs");
    docLinks.setLatest(uriList.get(0));
    docLinks.setSupported(uriList);
    return new ResponseEntity<>(docLinks, HttpStatus.OK);
  }

  public static class DocLinks {
    private String latest;
    private List<String> supported;

    public String getLatest() {
      return latest;
    }

    public void setLatest(String latest) {
      this.latest = latest;
    }

    public List<String> getSupported() {
      return supported;
    }

    public void setSupported(List<String> supported) {
      this.supported = supported;
    }
  }
}
