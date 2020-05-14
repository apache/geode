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
package org.apache.geode.redis.session.springRedisTestApplication.config;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import org.springframework.context.annotation.Configuration;

@Configuration
public class SessionListener implements HttpSessionListener {
  public static AtomicLong sessionCount = new AtomicLong(0);
  public static HashSet<String> sessionIds = new HashSet<>();

  @Override
  public void sessionCreated(HttpSessionEvent event) {
    sessionCount.getAndIncrement();
    sessionIds.add(event.getSession().getId());
    System.out.println("session created: " + event.getSession().getId());
    event.getSession().setMaxInactiveInterval(15);
  }

  @Override
  public void sessionDestroyed(HttpSessionEvent event) {
    sessionIds.remove(event.getSession().getId());
    sessionCount.getAndDecrement();
    System.out.println("session destroyed: " + event.getSession().getId());
  }
}
