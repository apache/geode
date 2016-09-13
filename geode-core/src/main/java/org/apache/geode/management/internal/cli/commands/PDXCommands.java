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
package com.gemstone.gemfire.management.internal.cli.commands;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;

import com.gemstone.gemfire.internal.cache.CacheConfig;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.InfoResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.configuration.SharedConfigurationWriter;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;
import com.gemstone.gemfire.pdx.internal.EnumInfo;
import com.gemstone.gemfire.pdx.internal.PdxType;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

public class PDXCommands extends AbstractCommandsSupport{


  @CliCommand (value = CliStrings.CONFIGURE_PDX, help = CliStrings.CONFIGURE_PDX__HELP)
  @CliMetaData (relatedTopic = CliStrings.TOPIC_GEODE_REGION, writesToSharedConfiguration = true)
  @ResourceOperation( resource= Resource.DATA, operation = Operation.MANAGE)
  public Result configurePDX(
      @CliOption (key = CliStrings.CONFIGURE_PDX__READ__SERIALIZED,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.CONFIGURE_PDX__READ__SERIALIZED__HELP) 
      Boolean readSerialized,

      @CliOption (key = CliStrings.CONFIGURE_PDX__IGNORE__UNREAD_FIELDS,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.CONFIGURE_PDX__IGNORE__UNREAD_FIELDS__HELP) 
      Boolean ignoreUnreadFields,

      @CliOption (key = CliStrings.CONFIGURE_PDX__DISKSTORE,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      specifiedDefaultValue = "",
      help = CliStrings.CONFIGURE_PDX__DISKSTORE__HELP)
      String diskStore, 

      @CliMetaData (valueSeparator = ",")
      @CliOption (key = CliStrings.CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      specifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES__HELP)
      String[] patterns,


      @CliMetaData (valueSeparator = ",")
      @CliOption (key = CliStrings.CONFIGURE_PDX__PORTABLE__AUTO__SERIALIZER__CLASSES,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      specifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.CONFIGURE_PDX__PORTABLE__AUTO__SERIALIZER__CLASSES__HELP)
      String[] portablePatterns){
    Result result = null;

    try {
      InfoResultData ird  = ResultBuilder.createInfoResultData();
      CacheCreation cache = new CacheCreation(true);

      if ((portablePatterns != null && portablePatterns.length > 0) && (patterns != null && patterns.length > 0)) {
        return ResultBuilder.createUserErrorResult(CliStrings.CONFIGURE_PDX__ERROR__MESSAGE);
      }
      if (CliUtil.getAllNormalMembers(CliUtil.getCacheIfExists()).isEmpty()) {
        ird.addLine(CliStrings.CONFIGURE_PDX__NORMAL__MEMBERS__WARNING);
      }
      //Set persistent and the disk-store
      if (diskStore != null) {
        cache.setPdxPersistent(true);
        ird.addLine(CliStrings.CONFIGURE_PDX__PERSISTENT + " = " + cache.getPdxPersistent());
        if (!diskStore.equals("")) {
          cache.setPdxDiskStore(diskStore);
          ird.addLine(CliStrings.CONFIGURE_PDX__DISKSTORE + " = " + cache.getPdxDiskStore());
        } else {
          ird.addLine(CliStrings.CONFIGURE_PDX__DISKSTORE + " = " + "DEFAULT");
        }
      } else {
        cache.setPdxPersistent(CacheConfig.DEFAULT_PDX_PERSISTENT);
        ird.addLine(CliStrings.CONFIGURE_PDX__PERSISTENT + " = " + cache.getPdxPersistent());
      }

      //Set read-serialized
      if (readSerialized != null) {
        cache.setPdxReadSerialized(readSerialized);
      } else {
        cache.setPdxReadSerialized(CacheConfig.DEFAULT_PDX_READ_SERIALIZED);
      }
      ird.addLine(CliStrings.CONFIGURE_PDX__READ__SERIALIZED + " = " + cache.getPdxReadSerialized());


      //Set ingoreUnreadFields
      if (ignoreUnreadFields != null) {
        cache.setPdxIgnoreUnreadFields(ignoreUnreadFields);
      } else {
        cache.setPdxIgnoreUnreadFields(CacheConfig.DEFAULT_PDX_IGNORE_UNREAD_FIELDS);
      }
      ird.addLine(CliStrings.CONFIGURE_PDX__IGNORE__UNREAD_FIELDS + " = " + cache.getPdxIgnoreUnreadFields());


      if (portablePatterns != null) {
        ReflectionBasedAutoSerializer autoSerializer =  new ReflectionBasedAutoSerializer(portablePatterns);
        cache.setPdxSerializer(autoSerializer);
        ird.addLine("PDX Serializer " + cache.getPdxSerializer().getClass().getName());
        ird.addLine("Portable classes " + Arrays.toString(portablePatterns));
      } 

      if (patterns!=null) {
        ReflectionBasedAutoSerializer nonPortableAutoSerializer =  new ReflectionBasedAutoSerializer(true, patterns);
        cache.setPdxSerializer(nonPortableAutoSerializer);
        ird.addLine("PDX Serializer : " + cache.getPdxSerializer().getClass().getName());
        ird.addLine("Non portable classes :" + Arrays.toString(patterns));
      }

      final StringWriter stringWriter = new StringWriter();
      final PrintWriter printWriter = new PrintWriter(stringWriter);
      CacheXmlGenerator.generate(cache, printWriter, true, false, false);
      printWriter.close();
      String xmlDefinition = stringWriter.toString();
      // TODO jbarrett - shouldn't this use the same loadXmlDefinition that other constructors use?
      XmlEntity xmlEntity = XmlEntity.builder().withType(CacheXml.PDX).withConfig(xmlDefinition).build();


      SharedConfigurationWriter scWriter = new SharedConfigurationWriter();
      boolean commandPersisted = scWriter.addXmlEntity(xmlEntity, null);

      result = ResultBuilder.buildResult(ird);
      result.setCommandPersisted(commandPersisted);
    } catch (Exception e) {
      return ResultBuilder.createGemFireErrorResult(e.getMessage());
    }

    return result;
  }

  @CliAvailabilityIndicator({CliStrings.CONFIGURE_PDX})
  public boolean isRegionCommandAvailable() {
    if (!CliUtil.isGfshVM()) {
      return true;
    }
    return (getGfsh() != null && getGfsh().isConnectedAndReady());
  }

  @CliCommand (value = CliStrings.PDX_RENAME, help = CliStrings.PDX_RENAME__HELP)
  @CliMetaData(shellOnly=true, relatedTopic={CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result pdxRename(
      @CliOption (key = CliStrings.PDX_RENAME_OLD,
      mandatory=true,
      help = CliStrings.PDX_RENAME_OLD__HELP) 
      String oldClassName,

      @CliOption (key = CliStrings.PDX_RENAME_NEW,
      mandatory=true,
      help = CliStrings.PDX_RENAME_NEW__HELP) 
      String newClassName,

      @CliOption (key = CliStrings.PDX_DISKSTORE,
      mandatory=true,
      help = CliStrings.PDX_DISKSTORE__HELP)
      String diskStore, 

      @CliOption (key = CliStrings.PDX_DISKDIR,
      mandatory=true,
      help = CliStrings.PDX_DISKDIR__HELP)
      @CliMetaData (valueSeparator = ",")
      String[] diskDirs){
    
    try {
      final File[] dirs = new File[diskDirs.length];
      for (int i = 0; i < diskDirs.length; i++) {
        dirs[i] = new File((diskDirs[i]));
      }
      
      Collection<Object> results = DiskStoreImpl.pdxRename(diskStore, dirs, oldClassName, newClassName);
      
      if(results.isEmpty()) {
        return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.PDX_RENAME__EMPTY));
      }
      
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      PrintStream printStream = new PrintStream(outputStream);
      for(Object p : results) {
        if(p instanceof PdxType) {
          ((PdxType)p).toStream(printStream, false);
        } else {
          ((EnumInfo)p).toStream(printStream);
        }
      }
      String resultString = CliStrings.format(CliStrings.PDX_RENAME__SUCCESS, outputStream.toString());
      return ResultBuilder.createInfoResult(resultString.toString());

    } catch (Exception e) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.PDX_RENAME__ERROR, e.getMessage()));
    }
    
  }

  @CliAvailabilityIndicator({CliStrings.PDX_RENAME})
  public boolean pdxRenameCommandsAvailable() {
    return true;
  }
}
