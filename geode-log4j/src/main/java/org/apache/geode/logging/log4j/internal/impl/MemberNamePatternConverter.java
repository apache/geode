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

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;

import org.apache.geode.annotations.Immutable;

/**
 * Formats the event member name.
 */
@Plugin(name = "MemberNamePatternConverter", category = "Converter")
@ConverterKeys("memberName")
public class MemberNamePatternConverter extends LogEventPatternConverter {
  /**
   * Singleton.
   */
  @Immutable
  static final MemberNamePatternConverter INSTANCE = new MemberNamePatternConverter();

  private final MemberNameSupplier memberNameSupplier;

  /**
   * Private constructor.
   */
  private MemberNamePatternConverter() {
    super("MemberName", "memberName");
    memberNameSupplier = new MemberNameSupplier();
  }

  /**
   * Obtains an instance of MemberNamePatternConverter.
   *
   * @param options options, currently ignored, may be null.
   * @return instance of MemberNamePatternConverter.
   */
  public static MemberNamePatternConverter newInstance(final String[] options) {
    return INSTANCE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void format(final LogEvent event, final StringBuilder toAppendTo) {
    toAppendTo.append(memberNameSupplier.get());
  }

  MemberNameSupplier getMemberNameSupplier() {
    return memberNameSupplier;
  }
}
