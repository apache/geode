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
package org.apache.geode.management.cli;

import org.springframework.shell.core.annotation.CliOption;

/**
 * Used in {@link CliOption} annotations to indicate which converter(s) should or should not be
 * used.
 *
 * @since GemFire 8.0
 */
public interface ConverterHint {
  public static final String DISABLE_STRING_CONVERTER = ":disable-string-converter";
  public static final String DISKSTORE =
      "geode.converter.cluster.diskstore" + DISABLE_STRING_CONVERTER;
  public static final String FILE = "geode.converter.file";
  public static final String FILE_PATH = "geode.converter.file.path" + DISABLE_STRING_CONVERTER;
  public static final String HINT = "geode.converter.hint" + DISABLE_STRING_CONVERTER;
  public static final String HELP = "geode.converter.help" + DISABLE_STRING_CONVERTER;
  public static final String MEMBERGROUP = "geode.converter.member.groups";
  /** Hint to be used for all types of GemFire cluster members */
  public static final String ALL_MEMBER_IDNAME = "geode.converter.all.member.idOrName";
  /** Hint to be used for all non locator GemFire cluster members */
  public static final String MEMBERIDNAME = "geode.converter.member.idOrName";
  /** Hint to be used for GemFire stand-alone locator members */
  public static final String LOCATOR_MEMBER_IDNAME = "geode.converter.locatormember.idOrName";
  /** Hint to be used for configured locators for discovery */
  public static final String LOCATOR_DISCOVERY_CONFIG = "geode.converter.locators.discovery.config";
  public static final String REGION_PATH = "geode.converter.region.path" + DISABLE_STRING_CONVERTER;
  public static final String INDEX_TYPE = "geode.converter.index.type";
  public static final String GATEWAY_SENDER_ID = "geode.converter.gateway.senderid";
  public static final String GATEWAY_RECEIVER_ID = "geode.converter.gateway.receiverid";
  public static final String LOG_LEVEL = "geode.converter.log.levels" + DISABLE_STRING_CONVERTER;

}
