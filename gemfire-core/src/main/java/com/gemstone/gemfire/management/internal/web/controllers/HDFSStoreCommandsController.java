/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.controllers;

import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

/**
 * The HDFSStoreCommandsController class implements GemFire Management REST API web service endpoints for the
 * Gfsh Hdfs Store Commands.
 * <p/>
 * @author Namrata Thanvi
 * @see com.gemstone.gemfire.management.internal.cli.commands.HDFSStoreCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 9.0
 */
@Controller("hdfsStoreController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class HDFSStoreCommandsController extends AbstractCommandsController {
  @RequestMapping(method = RequestMethod.GET, value = "/hdfsstores")
  @ResponseBody
  public String listHDFSStores() {
    String my= processCommand(CliStrings.LIST_HDFS_STORE);
    return my;
  }
  
  @RequestMapping(method = RequestMethod.POST, value = "/hdfsstores")
  @ResponseBody
  public String createHdfsStore(
		  @RequestParam(CliStrings.CREATE_HDFS_STORE__NAME) final String storeName,		  
		  @RequestParam(value = CliStrings.CREATE_HDFS_STORE__NAMENODE, required=false) final String  namenode,
		  @RequestParam(value = CliStrings.CREATE_HDFS_STORE__HOMEDIR, required=false) final String  homedir,
		  @RequestParam(value = CliStrings.CREATE_HDFS_STORE__BATCHSIZE,required=false) final Integer batchSize,                    
		  @RequestParam(value = CliStrings.CREATE_HDFS_STORE__BATCHINTERVAL, required=false) final Integer batchInterval,          
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__READCACHESIZE, required=false) final Float readCachesize,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__DISPATCHERTHREADS, required=false) final Integer dispatcherThreads,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__MAXMEMORY, required=false) final Integer maxMemory,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__BUFFERPERSISTENT, required=false) final Boolean persistence,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__SYNCDISKWRITE, required=false) final Boolean  synchronousDiskWrite,                    
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__DISKSTORENAME, required=false) final String diskStoreName,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__MINORCOMPACT, required=false) final Boolean minorCompaction,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__MINORCOMPACTIONTHREADS, required=false) final Integer minorCompactionThreads,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__MAJORCOMPACT, required=false) final Boolean majorCompact,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__MAJORCOMPACTINTERVAL, required=false) final Integer majorCompactionInterval,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__MAJORCOMPACTIONTHREADS, required=false) final Integer majorCompactionThreads,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__PURGEINTERVAL, required=false) final Integer purgeInterval,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__WRITEONLYFILESIZE, required=false) final Integer writeOnlyFileSize,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__FILEROLLOVERINTERVAL, required=false) final Integer fileRolloverInterval,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__CLIENTCONFIGFILE, required=false) final String clientConfigFile,
          @RequestParam(value = CliStrings.CREATE_HDFS_STORE__GROUP, required = false) final String[] groups)
  {
		CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_HDFS_STORE);

		command.addOption(CliStrings.CREATE_HDFS_STORE__NAME, storeName);
		
		if (hasValue(namenode))
			command.addOption(CliStrings.CREATE_HDFS_STORE__NAMENODE, namenode);
		
		if (hasValue(homedir))
			command.addOption(CliStrings.CREATE_HDFS_STORE__HOMEDIR, homedir);
		
		if (hasValue(batchSize))
			command.addOption(CliStrings.CREATE_HDFS_STORE__BATCHSIZE, String.valueOf(batchSize));
		
		if (hasValue(batchInterval))
			command.addOption(CliStrings.CREATE_HDFS_STORE__BATCHINTERVAL, String.valueOf(batchInterval));
		
		if (hasValue(readCachesize))
			command.addOption(CliStrings.CREATE_HDFS_STORE__READCACHESIZE, String.valueOf(readCachesize));
		
		if (hasValue(dispatcherThreads))
			command.addOption(CliStrings.CREATE_HDFS_STORE__DISPATCHERTHREADS, String.valueOf(dispatcherThreads));
		
		if (hasValue(maxMemory))
			command.addOption(CliStrings.CREATE_HDFS_STORE__MAXMEMORY,String.valueOf(maxMemory));
		
		if (hasValue(persistence))
			command.addOption(CliStrings.CREATE_HDFS_STORE__BUFFERPERSISTENT,String.valueOf(Boolean.TRUE.equals(persistence)));
		
		if (hasValue(synchronousDiskWrite))
			command.addOption(CliStrings.CREATE_HDFS_STORE__SYNCDISKWRITE,String.valueOf(Boolean.TRUE.equals(synchronousDiskWrite)));
		
		if (hasValue(diskStoreName))
			command.addOption(CliStrings.CREATE_HDFS_STORE__DISKSTORENAME,String.valueOf(diskStoreName));
		
		if (hasValue(minorCompaction))
			command.addOption(CliStrings.CREATE_HDFS_STORE__MINORCOMPACT,String.valueOf(Boolean.TRUE.equals(minorCompaction)));
		
		if (hasValue(minorCompactionThreads))
			command.addOption(CliStrings.CREATE_HDFS_STORE__MINORCOMPACTIONTHREADS,String.valueOf(minorCompactionThreads));
		
		if (hasValue(majorCompact))
			command.addOption(CliStrings.CREATE_HDFS_STORE__MAJORCOMPACT,String.valueOf(Boolean.TRUE.equals(majorCompact)));
		
		if (hasValue(majorCompactionInterval))
			command.addOption(CliStrings.CREATE_HDFS_STORE__MAJORCOMPACTINTERVAL,String.valueOf(majorCompactionInterval));
		
		if (hasValue(majorCompactionThreads))
			command.addOption(CliStrings.CREATE_HDFS_STORE__MAJORCOMPACTIONTHREADS,String.valueOf(majorCompactionThreads));
		
		if (hasValue(purgeInterval))
			command.addOption(CliStrings.CREATE_HDFS_STORE__PURGEINTERVAL,String.valueOf(purgeInterval));
		
		if (hasValue(writeOnlyFileSize))
			command.addOption(CliStrings.CREATE_HDFS_STORE__WRITEONLYFILESIZE,String.valueOf(writeOnlyFileSize));
		
		if (hasValue(fileRolloverInterval))
			command.addOption(CliStrings.CREATE_HDFS_STORE__FILEROLLOVERINTERVAL,String.valueOf(fileRolloverInterval));
		
		if (hasValue(clientConfigFile))
			command.addOption(CliStrings.CREATE_HDFS_STORE__CLIENTCONFIGFILE,String.valueOf(clientConfigFile));		

		if (hasValue(groups)) {
			command.addOption(CliStrings.CREATE_HDFS_STORE__GROUP,StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
		}

		return processCommand(command.toString());
  }  
  
  @RequestMapping(method = RequestMethod.GET, value = "/hdfsstores/{name}")
  @ResponseBody
  public String describeHDFSStore(
		  @PathVariable("name") final String hdfsStoreName,
          @RequestParam(CliStrings.DESCRIBE_HDFS_STORE__MEMBER) final String memberNameId)
  {	  
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESCRIBE_HDFS_STORE);
    command.addOption(CliStrings.DESCRIBE_HDFS_STORE__NAME, decode(hdfsStoreName));
    command.addOption(CliStrings.DESCRIBE_HDFS_STORE__MEMBER, memberNameId);    
    return processCommand(command.toString());
  }  
  
  @RequestMapping(method = RequestMethod.PUT, value = "/hdfsstores/{name}")
  @ResponseBody
  public String alterHdfsStore(
		  @PathVariable("name") final String hdfsStoreName,	  								
		  @RequestParam(value = CliStrings.ALTER_HDFS_STORE__BATCHSIZE, required=false) final Integer batchSize,                                    				                                
		  @RequestParam(value = CliStrings.ALTER_HDFS_STORE__BATCHINTERVAL, required=false) final Integer batchInterval,
          @RequestParam(value = CliStrings.ALTER_HDFS_STORE__MINORCOMPACT, required=false) final Boolean minorCompaction,
          @RequestParam(value = CliStrings.ALTER_HDFS_STORE__MINORCOMPACTIONTHREADS, required=false) final Integer minorCompactionThreads,
          @RequestParam(value = CliStrings.ALTER_HDFS_STORE__MAJORCOMPACT, required=false) final Boolean majorCompact,
          @RequestParam(value = CliStrings.ALTER_HDFS_STORE__MAJORCOMPACTINTERVAL, required=false) final Integer majorCompactionInterval,
          @RequestParam(value = CliStrings.ALTER_HDFS_STORE__MAJORCOMPACTIONTHREADS, required=false) final Integer majorCompactionThreads,
          @RequestParam(value = CliStrings.ALTER_HDFS_STORE__PURGEINTERVAL, required=false) final Integer purgeInterval,
          @RequestParam(value = CliStrings.ALTER_HDFS_STORE__WRITEONLYFILESIZE, required=false) final Integer writeOnlyFileSize,
          @RequestParam(value = CliStrings.ALTER_HDFS_STORE__FILEROLLOVERINTERVAL, required=false) final Integer fileRolloverInterval,
          @RequestParam(value = CliStrings.ALTER_HDFS_STORE__GROUP, required = false) final String[] groups)
  {
	  CommandStringBuilder command = new CommandStringBuilder(CliStrings.ALTER_HDFS_STORE);

		command.addOption(CliStrings.ALTER_HDFS_STORE__NAME, hdfsStoreName);
		
		
		if (hasValue(batchSize))
			command.addOption(CliStrings.ALTER_HDFS_STORE__BATCHSIZE, String.valueOf(batchSize));
		
		if (hasValue(batchInterval))
			command.addOption(CliStrings.ALTER_HDFS_STORE__BATCHINTERVAL, String.valueOf(batchInterval));	
		
		if (hasValue(minorCompaction))
			command.addOption(CliStrings.ALTER_HDFS_STORE__MINORCOMPACT,String.valueOf(Boolean.TRUE.equals(minorCompaction)));
		
		if (hasValue(minorCompactionThreads))
			command.addOption(CliStrings.ALTER_HDFS_STORE__MINORCOMPACTIONTHREADS,String.valueOf(minorCompactionThreads));
		
		if (hasValue(majorCompact))
			command.addOption(CliStrings.ALTER_HDFS_STORE__MAJORCOMPACT,String.valueOf(Boolean.TRUE.equals(majorCompact)));
		
		if (hasValue(majorCompactionInterval))
			command.addOption(CliStrings.ALTER_HDFS_STORE__MAJORCOMPACTINTERVAL,String.valueOf(majorCompactionInterval));
		
		if (hasValue(majorCompactionThreads))
			command.addOption(CliStrings.ALTER_HDFS_STORE__MAJORCOMPACTIONTHREADS,String.valueOf(majorCompactionThreads));
		
		if (hasValue(purgeInterval))
			command.addOption(CliStrings.ALTER_HDFS_STORE__PURGEINTERVAL,String.valueOf(purgeInterval));
		
		if (hasValue(writeOnlyFileSize))
			command.addOption(CliStrings.ALTER_HDFS_STORE__WRITEONLYFILESIZE,String.valueOf(writeOnlyFileSize));
		
		if (hasValue(fileRolloverInterval))
			command.addOption(CliStrings.ALTER_HDFS_STORE__FILEROLLOVERINTERVAL,String.valueOf(fileRolloverInterval));
		
		if (hasValue(groups)) {
			command.addOption(CliStrings.ALTER_HDFS_STORE__GROUP,StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
		}

		return processCommand(command.toString());
  }
  
  @RequestMapping(method = RequestMethod.DELETE, value = "/hdfsstores/{name}")
  @ResponseBody
  public String destroyHDFSStore(
		  @PathVariable("name") final String hdfsStoreName,
          @RequestParam(value = CliStrings.DESTROY_HDFS_STORE__GROUP, required = false) final String[] groups)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESTROY_HDFS_STORE);
    command.addOption(CliStrings.DESTROY_HDFS_STORE__NAME, decode(hdfsStoreName));

    if (hasValue(groups)) {
      command.addOption(CliStrings.DESTROY_HDFS_STORE__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }
    return processCommand(command.toString());
    
  }  
}
