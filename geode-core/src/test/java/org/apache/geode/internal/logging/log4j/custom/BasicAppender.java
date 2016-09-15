/*
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
 */
package org.apache.geode.internal.logging.log4j.custom;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

@Plugin(name = "Basic", category = "Core", elementType = "appender", printObject = true)
public class BasicAppender extends AbstractAppender {

  private static volatile BasicAppender instance;

  private final List<LogEvent> events = new ArrayList<>();

  public BasicAppender(final String name, final Filter filter, final Layout<? extends Serializable> layout) {
    super(name, filter, layout);
  }

  @PluginFactory
  public static BasicAppender createAppender(@PluginAttribute("name") String name,
                                             @PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
                                             @PluginElement("Layout") Layout layout,
                                             @PluginElement("Filters") Filter filter) {
    if (layout == null) {
      layout = PatternLayout.createDefaultLayout();
    }
    instance = new BasicAppender(name, filter, layout);
    return instance;
  }

  public static BasicAppender getInstance() {
    return instance;
  }

  public static void clearInstance() {
    instance = null;
  }

  public static void clearEvents() {
    instance.events.clear();
  }

  @Override
  public void append(final LogEvent event) {
    this.events.add(event);
  }

  public List<LogEvent> events() {
    return this.events;
  }
}
