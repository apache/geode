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
package org.apache.geode.redis.session.springRedisTestApplication;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SessionController {
  @SuppressWarnings("unchecked")
  @GetMapping("/getSessionNotes")
  public List<String> getSessionNotes(HttpServletRequest request) {
    HttpSession session = request.getSession(false);

    if (session == null) {
      return null;
    } else {
      return (List<String>) session.getAttribute("NOTES");
    }
  }

  @SuppressWarnings("unchecked")
  @PostMapping("/addSessionNote")
  public void addSessionNote(@RequestBody String note, HttpServletRequest request) {
    List<String> notes =
        (List<String>) request.getSession().getAttribute("NOTES");

    if (notes == null) {
      notes = new ArrayList<>();
    }

    notes.add(note);
    request.getSession().setAttribute("NOTES", notes);
  }

  @GetMapping("/getSessionID")
  public String getSession(HttpServletRequest request) {
    return request.getSession().getId();
  }

  @PostMapping("/setMaxInactiveInterval")
  public void setMaxInactiveInterval(@RequestBody int maxInactiveInterval,
      HttpServletRequest request) {
    request.getSession().setMaxInactiveInterval(maxInactiveInterval);
  }

  @PostMapping("/invalidateSession")
  public void invalidateSession(HttpServletRequest request) {
    request.getSession().invalidate();
  }
}
