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
  String DISABLE_STRING_CONVERTER = ":disable-string-converter";
  String DISABLE_ENUM_CONVERTER = ":disable-enum-converter";
  String DISKSTORE = "geode.converter.cluster.diskstore" + DISABLE_STRING_CONVERTER;
  String FILE = "geode.converter.file";
  String FILE_PATH = "geode.converter.file.path" + DISABLE_STRING_CONVERTER;
  String HINT = "geode.converter.hint" + DISABLE_STRING_CONVERTER;
  String HELP = "geode.converter.help" + DISABLE_STRING_CONVERTER;
  String MEMBERGROUP = "geode.converter.member.groups" + DISABLE_STRING_CONVERTER;
  /** Hint to be used for all types of GemFire cluster members */
  String ALL_MEMBER_IDNAME = "geode.converter.all.member.idOrName" + DISABLE_STRING_CONVERTER;
  /** Hint to be used for all non locator GemFire cluster members */
  String MEMBERIDNAME = "geode.converter.member.idOrName" + DISABLE_STRING_CONVERTER;
  /** Hint to be used for GemFire stand-alone locator members */
  String LOCATOR_MEMBER_IDNAME =
      "geode.converter.locatormember.idOrName" + DISABLE_STRING_CONVERTER;
  /** Hint to be used for configured locators for discovery */
  String LOCATOR_DISCOVERY_CONFIG =
      "geode.converter.locators.discovery.config" + DISABLE_STRING_CONVERTER;
  String REGION_PATH = "geode.converter.region.path" + DISABLE_STRING_CONVERTER;
  String INDEX_TYPE = "geode.converter.index.type" + DISABLE_ENUM_CONVERTER;
  String GATEWAY_SENDER_ID = "geode.converter.gateway.senderid" + DISABLE_STRING_CONVERTER;
  String LOG_LEVEL = "geode.converter.log.levels" + DISABLE_STRING_CONVERTER;
  String JARFILES = "geode.converter.jarfiles" + DISABLE_STRING_CONVERTER;
  String JARDIR = "geode.converter.jardir" + DISABLE_STRING_CONVERTER;
  String DEPENDENCIES = "geode.converter.dependencies" + DISABLE_STRING_CONVERTER;
}
