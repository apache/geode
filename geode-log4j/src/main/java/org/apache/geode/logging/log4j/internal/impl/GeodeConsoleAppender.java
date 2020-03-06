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
package org.apache.geode.logging.log4j.internal.impl;

import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.ConsoleAppender.Target;
import org.apache.logging.log4j.core.appender.ManagerFactory;
import org.apache.logging.log4j.core.appender.OutputStreamManager;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;
import org.apache.logging.log4j.core.util.NullOutputStream;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;

/**
 * Wraps {@link ConsoleAppender} with additions defined in {@link PausableAppender} and
 * {@link DebuggableAppender}.
 */
@Plugin(name = GeodeConsoleAppender.PLUGIN_NAME, category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE, printObject = true)
@SuppressWarnings("unused")
public class GeodeConsoleAppender extends AbstractOutputStreamAppender<OutputStreamManager>
    implements PausableAppender, DebuggableAppender {

  public static final String PLUGIN_NAME = "GeodeConsole";

  private static final boolean START_PAUSED_BY_DEFAULT = false;
  @Immutable
  private static final Target DEFAULT_TARGET = Target.SYSTEM_OUT;
  @MakeNotStatic
  private static final AtomicInteger COUNT = new AtomicInteger();
  @Immutable
  private static final GeodeConsoleManagerFactory MANAGER_FACTORY =
      new GeodeConsoleManagerFactory();

  private final ConsoleAppender delegate;
  private final boolean debug;
  private final List<LogEvent> events;

  private volatile boolean paused;

  protected GeodeConsoleAppender(final String name,
      final Layout<? extends Serializable> layout,
      final Filter filter,
      final OutputStreamManager manager,
      final ConsoleAppender delegate) {
    this(name, layout, filter, manager, true, START_PAUSED_BY_DEFAULT, false, delegate);
  }

  protected GeodeConsoleAppender(final String name,
      final Layout<? extends Serializable> layout,
      final Filter filter,
      final OutputStreamManager manager,
      final boolean ignoreExceptions,
      final boolean startPaused,
      final boolean debug,
      final ConsoleAppender delegate) {
    super(name, layout, filter, ignoreExceptions, true, Property.EMPTY_ARRAY, manager);
    this.delegate = delegate;
    this.debug = debug;
    if (debug) {
      events = Collections.synchronizedList(new ArrayList<>());
    } else {
      events = Collections.emptyList();
    }
    paused = startPaused;
  }

  public static GeodeConsoleAppender createDefaultAppenderForLayout(
      final Layout<? extends Serializable> layout) {
    // this method cannot use the builder class without introducing an infinite loop due to
    // DefaultConfiguration
    return new GeodeConsoleAppender(PLUGIN_NAME + "-" + COUNT.incrementAndGet(), layout, null,
        getDefaultManager(DEFAULT_TARGET, false, false, layout), true, false, false,
        ConsoleAppender.createDefaultAppenderForLayout(layout));
  }

  @PluginBuilderFactory
  public static <B extends Builder<B>> B newBuilder() {
    return new Builder<B>().asBuilder();
  }

  /**
   * Builds GeodeConsoleAppender instances.
   *
   * @param <B> The type to build
   */
  public static class Builder<B extends Builder<B>> extends AbstractOutputStreamAppender.Builder<B>
      implements org.apache.logging.log4j.core.util.Builder<GeodeConsoleAppender> {

    @PluginBuilderAttribute
    @Required
    private Target target = DEFAULT_TARGET;

    @PluginBuilderAttribute
    private boolean follow;

    @PluginBuilderAttribute
    private boolean direct;

    @PluginBuilderAttribute
    private boolean startPaused = START_PAUSED_BY_DEFAULT;

    @PluginBuilderAttribute
    private boolean debug;

    public B setTarget(final Target aTarget) {
      target = aTarget;
      return asBuilder();
    }

    public B setFollow(final boolean shouldFollow) {
      follow = shouldFollow;
      return asBuilder();
    }

    public B setDirect(final boolean shouldDirect) {
      direct = shouldDirect;
      return asBuilder();
    }

    public B setStartPaused(final boolean shouldStartPaused) {
      startPaused = shouldStartPaused;
      return asBuilder();
    }

    public boolean isStartPaused() {
      return debug;
    }

    public B setDebug(final boolean shouldDebug) {
      debug = shouldDebug;
      return asBuilder();
    }

    public boolean isDebug() {
      return debug;
    }

    @Override
    public GeodeConsoleAppender build() {
      ConsoleAppender.Builder<?> delegate = new ConsoleAppender.Builder<>();
      // AbstractAppender
      delegate.setFilter(getFilter());
      delegate.setName(getName() + "_DELEGATE");
      delegate.setIgnoreExceptions(isIgnoreExceptions());
      delegate.setLayout(getLayout());
      // AbstractOutputStreamAppender
      delegate.withImmediateFlush(isImmediateFlush());
      delegate.withBufferedIo(isBufferedIo());
      delegate.withBufferSize(getBufferSize());
      // ConsoleAppender
      delegate.setTarget(target);
      delegate.setFollow(follow);
      delegate.setDirect(direct);

      Layout<? extends Serializable> layout = getOrCreateLayout(target.getDefaultCharset());
      return new GeodeConsoleAppender(getName(), layout,
          getFilter(), getManager(target, follow, direct, layout),
          isIgnoreExceptions(), startPaused, debug, delegate.build());
    }
  }

  @Override
  public void append(final LogEvent event) {
    if (isPaused()) {
      return;
    }
    delegate.append(event);
    if (debug) {
      events.add(event);
    }
  }

  @Override
  public void start() {
    LOGGER.info("Starting {}.", this);
    delegate.start();
    super.start();
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping {}.", this);
    delegate.stop();
    super.stop();
  }

  @Override
  public void pause() {
    LOGGER.debug("Pausing {}.", this);
    paused = true;
  }

  @Override
  public void resume() {
    LOGGER.debug("Resuming {}.", this);
    paused = false;
  }

  @Override
  public boolean isPaused() {
    return paused;
  }

  @Override
  public void clearLogEvents() {
    events.clear();
  }

  @Override
  public List<LogEvent> getLogEvents() {
    return events;
  }

  @Override
  public String toString() {
    return getClass().getName() + "@" + Integer.toHexString(hashCode()) + ":" + getName()
        + " {paused=" + paused + ", debug=" + debug + ", delegate=" + delegate + "}";
  }

  @VisibleForTesting
  ConsoleAppender getDelegate() {
    return delegate;
  }

  private static OutputStreamManager getDefaultManager(final Target target, final boolean follow,
      final boolean direct, final Layout<? extends Serializable> layout) {

    OutputStream os = NullOutputStream.getInstance();
    String managerName = target.name() + '.' + follow + '.' + direct + "-" + COUNT.get();
    return OutputStreamManager.getManager(managerName, new FactoryData(os, managerName, layout),
        MANAGER_FACTORY);
  }

  private static OutputStreamManager getManager(final Target target, final boolean follow,
      final boolean direct, final Layout<? extends Serializable> layout) {
    OutputStream os = NullOutputStream.getInstance();
    String managerName = "null." + target.name() + '.' + follow + '.' + direct;
    return OutputStreamManager.getManager(managerName, new FactoryData(os, managerName, layout),
        MANAGER_FACTORY);
  }

  private static class FactoryData {
    private final OutputStream os;
    private final String name;
    private final Layout<? extends Serializable> layout;

    public FactoryData(final OutputStream os, final String type,
        final Layout<? extends Serializable> layout) {
      this.os = os;
      name = type;
      this.layout = layout;
    }
  }

  private static class AccessibleOutputStreamManager extends OutputStreamManager {

    protected AccessibleOutputStreamManager(final OutputStream os, final String streamName,
        final Layout<?> layout, final boolean writeHeader) {
      super(os, streamName, layout, writeHeader);
    }

    protected AccessibleOutputStreamManager(final OutputStream os, final String streamName,
        final Layout<?> layout,
        final boolean writeHeader, final int bufferSize) {
      super(os, streamName, layout, writeHeader, bufferSize);
    }
  }

  private static class GeodeConsoleManagerFactory implements
      ManagerFactory<OutputStreamManager, FactoryData> {

    @Override
    public OutputStreamManager createManager(final String name, final FactoryData data) {
      return new AccessibleOutputStreamManager(data.os, data.name, data.layout, true);
    }
  }
}
