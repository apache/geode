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
package org.apache.geode.management.internal.web.controllers;

import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The PdxCommandsController class implements GemFire Management REST API web service endpoints for Gfsh PDX Commands.
 *
 * @see org.apache.geode.management.internal.cli.commands.PDXCommands
 * @see org.apache.geode.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since GemFire 8.0
 */
@Controller("pdxController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class PdxCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.POST, value = "/pdx")
  @ResponseBody
  public String configurePdx(@RequestParam(value = CliStrings.CONFIGURE_PDX__READ__SERIALIZED, required = false) final Boolean readSerialized,
                             @RequestParam(value = CliStrings.CONFIGURE_PDX__IGNORE__UNREAD_FIELDS, required = false) final Boolean ignoreUnreadFields,
                             @RequestParam(value = CliStrings.CONFIGURE_PDX__DISKSTORE, required = false) final String diskStore,
                             @RequestParam(value = CliStrings.CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES, required = false) final String[] autoSerializerClasses,
                             @RequestParam(value = CliStrings.CONFIGURE_PDX__PORTABLE__AUTO__SERIALIZER__CLASSES, required = false) final String[] portableAutoSerializerClasses)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CONFIGURE_PDX);

    if (Boolean.TRUE.equals(readSerialized)) {
      command.addOption(CliStrings.CONFIGURE_PDX__READ__SERIALIZED, String.valueOf(readSerialized));
    }

    if (Boolean.TRUE.equals(ignoreUnreadFields)) {
      command.addOption(CliStrings.CONFIGURE_PDX__IGNORE__UNREAD_FIELDS, String.valueOf(ignoreUnreadFields));
    }

    if (hasValue(diskStore)) {
      command.addOption(CliStrings.CONFIGURE_PDX__DISKSTORE, diskStore);
    }

    if (hasValue(autoSerializerClasses)) {
      command.addOption(CliStrings.CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES, StringUtils.concat(
        autoSerializerClasses, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(portableAutoSerializerClasses)) {
      command.addOption(CliStrings.CONFIGURE_PDX__PORTABLE__AUTO__SERIALIZER__CLASSES, StringUtils.concat(
        portableAutoSerializerClasses, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  //@RequestMapping(method = RequestMethod.DELETE, value = "/pdx/type/field")
  //@ResponseBody
  public String pdxDeleteField(@RequestParam(value = CliStrings.PDX_CLASS) final String className,
                               @RequestParam(value = CliStrings.PDX_FIELD) final String fieldName,
                               @RequestParam(value = CliStrings.PDX_DISKSTORE) final String diskStore,
                               @RequestParam(value = CliStrings.PDX_DISKDIR, required = false) final String[] diskDirs)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.PDX_DELETE_FIELD);

    command.addOption(CliStrings.PDX_CLASS, className);
    command.addOption(CliStrings.PDX_FIELD, fieldName);
    command.addOption(CliStrings.PDX_DISKSTORE, diskStore);

    if (hasValue(diskDirs)) {
      command.addOption(CliStrings.PDX_DISKDIR, StringUtils.concat(diskDirs, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/pdx/type")
  @ResponseBody
  public String pdxRename(@RequestParam(value = CliStrings.PDX_RENAME_OLD) final String oldClassName,
                          @RequestParam(value = CliStrings.PDX_RENAME_NEW) final String newClassName,
                          @RequestParam(value = CliStrings.PDX_DISKSTORE) final String diskStore,
                          @RequestParam(value = CliStrings.PDX_DISKDIR, required = false) final String[] diskDirs)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.PDX_RENAME);

    command.addOption(CliStrings.PDX_RENAME_OLD, oldClassName);
    command.addOption(CliStrings.PDX_RENAME_NEW, newClassName);
    command.addOption(CliStrings.PDX_DISKSTORE, diskStore);

    if (hasValue(diskDirs)) {
      command.addOption(CliStrings.PDX_DISKDIR, StringUtils.concat(diskDirs, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

}
