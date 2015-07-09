package com.gemstone.gemfire.management.internal.cli.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributes;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory.HDFSCompactionConfigFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSCompactionConfigMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSEventQueueAttributesMutator;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder.AbstractHDFSCompactionConfigHolder;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreMutatorImpl;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.lang.ClassUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.functions.AlterHDFSStoreFunction;
import com.gemstone.gemfire.management.internal.cli.functions.AlterHDFSStoreFunction.AlterHDFSStoreAttributes;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.CreateHDFSStoreFunction;
import com.gemstone.gemfire.management.internal.cli.functions.DescribeHDFSStoreFunction;
import com.gemstone.gemfire.management.internal.cli.functions.DestroyHDFSStoreFunction;
import com.gemstone.gemfire.management.internal.cli.functions.ListHDFSStoresFunction;
import com.gemstone.gemfire.management.internal.cli.functions.ListHDFSStoresFunction.HdfsStoreDetails;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.ResultDataException;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.util.HDFSStoreNotFoundException;
import com.gemstone.gemfire.management.internal.cli.util.MemberNotFoundException;
import com.gemstone.gemfire.management.internal.configuration.SharedConfigurationWriter;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

/**
 * The HdfsStoreCommands class encapsulates all GemFire Hdfs Store commands in Gfsh.
 *  </p>
 *  
 * @author Namrata Thanvi
 * @see com.gemstone.gemfire.management.internal.cli.commands.AbstractCommandsSupport
 */


public class HDFSStoreCommands   extends AbstractCommandsSupport {  
  @CliCommand (value = CliStrings.CREATE_HDFS_STORE, help = CliStrings.CREATE_HDFS_STORE__HELP)
  @CliMetaData (relatedTopic = CliStrings.TOPIC_GEMFIRE_HDFSSTORE, writesToSharedConfiguration = true)
  public Result createHdfsStore(      
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__NAME,                  
                  mandatory = true,
                  optionContext = ConverterHint.HDFSSTORE_ALL, 
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__NAME__HELP)
      String hdfsUniqueName,
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__NAMENODE,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__NAMENODE__HELP)
      String namenode, 
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__HOMEDIR,
                  optionContext = ConverterHint.DIR_PATHSTRING,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__HOMEDIR__HELP)
      String homeDir,
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__BATCHSIZE,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__BATCHSIZE__HELP)
      Integer batchSize,
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__BATCHINTERVAL,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__BATCHINTERVAL__HELP)
      Integer batchInterval,
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__READCACHESIZE,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__READCACHESIZE__HELP)
      Float readCacheSize,
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__DISPATCHERTHREADS,
          mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.CREATE_HDFS_STORE__DISPATCHERTHREADS__HELP)
      Integer dispatcherThreads,
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__MAXMEMORY,
                  mandatory = false,
                  unspecifiedDefaultValue =CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__MAXMEMORY__HELP)
      Integer maxMemory,
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__BUFFERPERSISTENT,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__BUFFERPERSISTENT__HELP)
      Boolean bufferPersistent,
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__SYNCDISKWRITE,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__SYNCDISKWRITE__HELP)
      Boolean syncDiskWrite,
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__DISKSTORENAME,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__DISKSTORENAME__HELP)
      String diskStoreName,
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__MINORCOMPACT,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__MINORCOMPACT__HELP)
      Boolean minorCompact,            
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__MINORCOMPACTIONTHREADS,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__MINORCOMPACTIONTHREADS__HELP)
      Integer minorCompactionThreads,
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__MAJORCOMPACT,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__MAJORCOMPACT__HELP)
      Boolean majorCompact,   
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__MAJORCOMPACTINTERVAL,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__MAJORCOMPACTINTERVAL__HELP)
      Integer majorCompactionInterval, 
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__MAJORCOMPACTIONTHREADS,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__MAJORCOMPACTIONTHREADS__HELP)
      Integer majorCompactionThreads,  
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__PURGEINTERVAL,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__PURGEINTERVAL__HELP)
      Integer purgeInterval,  
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__WRITEONLYFILESIZE,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__WRITEONLYFILESIZE__HELP)
      Integer maxWriteonlyFileSize,  
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__FILEROLLOVERINTERVAL,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__FILEROLLOVERINTERVAL__HELP)
      Integer fileRolloverInterval,  
      @CliOption (key = CliStrings.CREATE_HDFS_STORE__CLIENTCONFIGFILE,
                  optionContext = ConverterHint.FILE_PATHSTRING,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_HDFS_STORE__CLIENTCONFIGFILE__HELP)      
      String clientConfigFile,
      @CliOption(key=CliStrings.CREATE_HDFS_STORE__GROUP,
                 help=CliStrings.CREATE_HDFS_STORE__GROUP__HELP,
                 optionContext=ConverterHint.MEMBERGROUP)
      @CliMetaData (valueSeparator = ",")
       String[] groups ) {
    try {
      
      return getCreatedHdfsStore(groups, hdfsUniqueName, namenode, homeDir, clientConfigFile, fileRolloverInterval,
          maxWriteonlyFileSize, minorCompact, majorCompact, batchSize, batchInterval, diskStoreName, bufferPersistent,
          dispatcherThreads, syncDiskWrite, readCacheSize, majorCompactionInterval, majorCompactionThreads,
          minorCompactionThreads, purgeInterval, maxMemory);
      
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;

    } catch (Throwable th) {
      String formattedErrString = CliStrings.format(CliStrings.CREATE_HDFS_STORE__ERROR_WHILE_CREATING_REASON_0,
          new Object[] { th.getMessage() });
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(formattedErrString);
    }
  }

  public Result getCreatedHdfsStore(String[] groups, String hdfsUniqueName, String namenode, String homeDir,
      String clientConfigFile, Integer fileRolloverInterval, Integer maxWriteonlyFileSize, Boolean minorCompact,
      Boolean majorCompact, Integer batchSize, Integer batchInterval, String diskStoreName, Boolean bufferPersistent,
      Integer dispatcherThreads, Boolean syncDiskWrite, Float readCacheSize, Integer majorCompactionInterval,
      Integer majorCompactionThreads, Integer minorCompactionThreads, Integer purgeInterval, Integer maxMemory) {

    XmlEntity xmlEntity = null;
    Logger logger = LogService.getLogger();

    Set<DistributedMember> targetMembers = null;

    try {
      targetMembers = getGroupMembers(groups);
    } catch (CommandResultException cre) {
      return cre.getResult();
    }

    HDFSStoreConfigHolder configHolder = new HDFSStoreConfigHolder();
    configHolder.setName(hdfsUniqueName);
    if (readCacheSize != null)
      configHolder.setBlockCacheSize(readCacheSize);

    if (fileRolloverInterval != null)
      configHolder.setFileRolloverInterval(fileRolloverInterval);
    if (clientConfigFile != null)
      configHolder.setHDFSClientConfigFile(clientConfigFile);
    if (homeDir != null)
      configHolder.setHomeDir(homeDir);
    if (maxWriteonlyFileSize != null)
      configHolder.setMaxFileSize(maxWriteonlyFileSize);
    if (namenode != null)
      configHolder.setNameNodeURL(namenode);

    HDFSCompactionConfigFactory compactionConfig = configHolder.createCompactionConfigFactory(null);
    if (minorCompact != null)
      configHolder.setMinorCompaction(minorCompact);
    if (majorCompact != null)
      compactionConfig.setAutoMajorCompaction(majorCompact);
    if (majorCompactionInterval != null)
      compactionConfig.setMajorCompactionIntervalMins(majorCompactionInterval);
    if (majorCompactionThreads != null)
      compactionConfig.setMajorCompactionMaxThreads(majorCompactionThreads);
    if (minorCompactionThreads != null)
      compactionConfig.setMaxThreads(minorCompactionThreads);
    if (purgeInterval != null)
      compactionConfig.setOldFilesCleanupIntervalMins(purgeInterval);

    configHolder.setHDFSCompactionConfig(compactionConfig.create());

    HDFSEventQueueAttributesFactory eventQueue = new HDFSEventQueueAttributesFactory();

    if (batchSize != null)
      eventQueue.setBatchSizeMB(batchSize);
    if (batchInterval != null)
      eventQueue.setBatchTimeInterval(batchInterval);
    if (diskStoreName != null)
      eventQueue.setDiskStoreName(diskStoreName);
    if (syncDiskWrite != null)
      eventQueue.setDiskSynchronous(syncDiskWrite);
    if (dispatcherThreads != null)
      eventQueue.setDispatcherThreads(dispatcherThreads);
    if (maxMemory != null)
      eventQueue.setMaximumQueueMemory(maxMemory);
    if (bufferPersistent != null)
      eventQueue.setPersistent(bufferPersistent);

    configHolder.setHDFSEventQueueAttributes(eventQueue.create());

    
    ResultCollector<?, ?> resultCollector = getMembersFunctionExecutor(targetMembers)
    .withArgs(configHolder).execute(new CreateHDFSStoreFunction());
    
    List<CliFunctionResult> hdfsStoreCreateResults = CliFunctionResult.cleanResults((List<?>)resultCollector
        .getResult());

    TabularResultData tabularResultData = ResultBuilder.createTabularResultData();

    Boolean accumulatedData = false;

    for (CliFunctionResult hdfsStoreCreateResult : hdfsStoreCreateResults) {
      if (hdfsStoreCreateResult.getThrowable() != null) {
        String memberId = hdfsStoreCreateResult.getMemberIdOrName();
        String errorMsg = hdfsStoreCreateResult.getThrowable().getMessage();
        String errClass = hdfsStoreCreateResult.getThrowable().getClass().getName();
        tabularResultData.accumulate("Member", memberId);
        tabularResultData.accumulate("Result", "ERROR: " + errClass + ": " + errorMsg);
        accumulatedData = true;
        tabularResultData.setStatus(Status.ERROR);
      }
      else if (hdfsStoreCreateResult.isSuccessful()) {
        String memberId = hdfsStoreCreateResult.getMemberIdOrName();
        String successMsg = hdfsStoreCreateResult.getMessage();
        tabularResultData.accumulate("Member", memberId);
        tabularResultData.accumulate("Result", successMsg);
        if (xmlEntity == null) {
          xmlEntity = hdfsStoreCreateResult.getXmlEntity();
        }
        accumulatedData = true;
      }
    }

    if (!accumulatedData) {
      return ResultBuilder.createInfoResult("Unable to create hdfs store:" + hdfsUniqueName);
    }

    Result result = ResultBuilder.buildResult(tabularResultData);
    if (xmlEntity != null) {
      result.setCommandPersisted((new SharedConfigurationWriter()).addXmlEntity(xmlEntity, groups));
    }

    return ResultBuilder.buildResult(tabularResultData);
  }
  
  
  @CliCommand(value = CliStrings.DESCRIBE_HDFS_STORE, help = CliStrings.DESCRIBE_HDFS_STORE__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = { CliStrings.TOPIC_GEMFIRE_HDFSSTORE})
  public Result describeHdfsStore(
      @CliOption(key = CliStrings.DESCRIBE_HDFS_STORE__MEMBER, 
                 mandatory = true, optionContext = ConverterHint.MEMBERIDNAME, 
                 help = CliStrings.DESCRIBE_HDFS_STORE__MEMBER__HELP)
      final String memberName,
      @CliOption(key = CliStrings.DESCRIBE_HDFS_STORE__NAME, 
                 mandatory = true, 
                 optionContext = ConverterHint.HDFSSTORE_ALL, 
                 help = CliStrings.DESCRIBE_HDFS_STORE__NAME__HELP)
      final String hdfsStoreName) {
    try{
      return toCompositeResult(getHDFSStoreDescription(memberName , hdfsStoreName));
      
      }catch (HDFSStoreNotFoundException e){
         return ResultBuilder.createShellClientErrorResult(((HDFSStoreNotFoundException)e).getMessage());
      } 
      catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN,
          CliStrings.DESCRIBE_HDFS_STORE));
      
    } catch (MemberNotFoundException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
      
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
      
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(String.format(CliStrings.DESCRIBE_HDFS_STORE__ERROR_MESSAGE,
          memberName, hdfsStoreName, t));
    }
  }        
  
  public HDFSStoreConfigHolder getHDFSStoreDescription(String memberName, String hdfsStoreName) {

    final DistributedMember member = getMember(getCache(), memberName);
    
    ResultCollector<?, ?> resultCollector = getMembersFunctionExecutor(Collections.singleton(member))
    .withArgs(hdfsStoreName).execute(new DescribeHDFSStoreFunction());
    
    Object result = ((List<?>)resultCollector.getResult()).get(0);

    if (result instanceof HDFSStoreConfigHolder) {
      return (HDFSStoreConfigHolder)result;
    }
    if (result instanceof HDFSStoreNotFoundException) {
      throw (HDFSStoreNotFoundException)result;
    }
    else {
      final Throwable cause = (result instanceof Throwable ? (Throwable)result : null);
      throw new RuntimeException(CliStrings.format(CliStrings.UNEXPECTED_RETURN_TYPE_EXECUTING_COMMAND_ERROR_MESSAGE,
          ClassUtils.getClassName(result), CliStrings.DESCRIBE_HDFS_STORE), cause);

    }
  }
  
  public Result toCompositeResult(final HDFSStoreConfigHolder storePrms) {
    final CompositeResultData hdfsStoreCompositeResult = ResultBuilder.createCompositeResultData();
    final CompositeResultData.SectionResultData hdfsStoreSection = hdfsStoreCompositeResult.addSection();

    hdfsStoreSection.addData("Hdfs Store Name", storePrms.getName());
    hdfsStoreSection.addData("Name Node URL", storePrms.getNameNodeURL());
    hdfsStoreSection.addData("Home Dir", storePrms.getHomeDir());
    hdfsStoreSection.addData("Block Cache", storePrms.getBlockCacheSize());
    hdfsStoreSection.addData("File RollOver Interval", storePrms.getFileRolloverInterval());
    hdfsStoreSection.addData("Max WriteOnly File Size", storePrms.getMaxFileSize());

    hdfsStoreSection.addData("Client Configuration File", storePrms.getHDFSClientConfigFile());

    HDFSEventQueueAttributes queueAttr = storePrms.getHDFSEventQueueAttributes();

    hdfsStoreSection.addData("Disk Store Name", queueAttr.getDiskStoreName());
    hdfsStoreSection.addData("Batch Size In MB", queueAttr.getBatchSizeMB());
    hdfsStoreSection.addData("Batch Interval Time", queueAttr.getBatchTimeInterval());
    hdfsStoreSection.addData("Maximum Memory", queueAttr.getMaximumQueueMemory());
    hdfsStoreSection.addData("Dispatcher Threads", queueAttr.getDispatcherThreads());
    hdfsStoreSection.addData("Buffer Persistence", queueAttr.isPersistent());
    hdfsStoreSection.addData("Synchronous Persistence", queueAttr.isDiskSynchronous());

    AbstractHDFSCompactionConfigHolder compaction = storePrms.getHDFSCompactionConfig();

    hdfsStoreSection.addData("Major Compaction Enabled", compaction.getAutoMajorCompaction());
    hdfsStoreSection.addData("Major Compaction Threads", compaction.getMajorCompactionMaxThreads());
    hdfsStoreSection.addData("Major compaction Interval", compaction.getMajorCompactionIntervalMins());
    hdfsStoreSection.addData("Minor Compaction Enabled", storePrms.getMinorCompaction());
    hdfsStoreSection.addData("Minor Compaction Threads", compaction.getMaxThreads());
    hdfsStoreSection.addData("Purge Interval", compaction.getOldFilesCleanupIntervalMins());

    return ResultBuilder.buildResult(hdfsStoreCompositeResult);
  } 
  
  @CliCommand(value = CliStrings.LIST_HDFS_STORE, help = CliStrings.LIST_HDFS_STORE__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = { CliStrings.TOPIC_GEMFIRE_HDFSSTORE })
  public Result listHdfsStore() {  
    try {
      Set<DistributedMember> dataMembers = getNormalMembers(getCache());
      if (dataMembers.isEmpty()) {
        return ResultBuilder.createInfoResult(CliStrings.NO_CACHING_MEMBERS_FOUND_MESSAGE);
      }
      return toTabularResult(getHdfsStoreListing(dataMembers));

    } catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN,
          CliStrings.LIST_HDFS_STORE));

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;

    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(
          String.format(CliStrings.LIST_HDFS_STORE__ERROR_MESSAGE, t.getMessage()));
    }
  }
  
  protected List<HdfsStoreDetails> getHdfsStoreListing(Set<DistributedMember> members) {

    final Execution membersFunctionExecutor = getMembersFunctionExecutor(members);

    if (membersFunctionExecutor instanceof AbstractExecution) {
      ((AbstractExecution)membersFunctionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector<?, ?> resultCollector = membersFunctionExecutor.execute(new ListHDFSStoresFunction());
    final List<?> results = (List<?>)resultCollector.getResult();
    final List<HdfsStoreDetails> hdfsStoreList = new ArrayList<HdfsStoreDetails>(results.size());

    for (final Object result : results) {
      if (result instanceof Set) { // ignore FunctionInvocationTargetExceptions and other Exceptions...
        hdfsStoreList.addAll((Set<HdfsStoreDetails>)result);
      }
    }

    Collections.sort(hdfsStoreList, new Comparator<HdfsStoreDetails>() {
      public <T extends Comparable<T>> int compare(final T obj1, final T obj2) {
        return (obj1 == null && obj2 == null ? 0 : (obj1 == null ? 1 : (obj2 == null ? -1 : obj1.compareTo(obj2))));
      }

      @Override
      public int compare(HdfsStoreDetails store1, HdfsStoreDetails store2) {
        int comparisonValue = compare(store1.getMemberName(), store2.getMemberName());
        comparisonValue = (comparisonValue != 0 ? comparisonValue : compare(store1.getMemberId(), store2.getMemberId()));
        return (comparisonValue != 0 ? comparisonValue : store1.getStoreName().compareTo(store2.getStoreName()));
      }
    });

    return hdfsStoreList;
  }
  

  protected Result toTabularResult(final List<HdfsStoreDetails> hdfsStoreList) throws ResultDataException {
    if (!hdfsStoreList.isEmpty()) {
      final TabularResultData hdfsStoreData = ResultBuilder.createTabularResultData();
      for (final HdfsStoreDetails hdfsStoreDetails : hdfsStoreList) {
        hdfsStoreData.accumulate("Member Name", hdfsStoreDetails.getMemberName());
        hdfsStoreData.accumulate("Member Id", hdfsStoreDetails.getMemberId());
        hdfsStoreData.accumulate("Hdfs Store Name", hdfsStoreDetails.getStoreName());
      }
      return ResultBuilder.buildResult(hdfsStoreData);
    }
    else {
      return ResultBuilder.createInfoResult(CliStrings.LIST_HDFS_STORE__HDFS_STORES_NOT_FOUND_MESSAGE);
    }
  }
  

  @CliCommand(value=CliStrings.DESTROY_HDFS_STORE, help=CliStrings.DESTROY_HDFS_STORE__HELP)
  @CliMetaData(shellOnly=false, relatedTopic={CliStrings.TOPIC_GEMFIRE_HDFSSTORE}, writesToSharedConfiguration=true)
  public Result destroyHdfstore(
      @CliOption  (key=CliStrings.DESTROY_HDFS_STORE__NAME, 
                   optionContext=ConverterHint.HDFSSTORE_ALL,
                   mandatory=true,
                   help=CliStrings.DESTROY_HDFS_STORE__NAME__HELP)
        String hdfsStoreName,
      @CliOption(key=CliStrings.DESTROY_HDFS_STORE__GROUP,
                 help=CliStrings.DESTROY_HDFS_STORE__GROUP__HELP,
                 optionContext=ConverterHint.MEMBERGROUP)
      @CliMetaData (valueSeparator = ",")
        String[] groups) {
    try{      
       return destroyStore(hdfsStoreName,groups);
 
    } catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN,
          CliStrings.DESTROY_HDFS_STORE));
      
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
      
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(
          CliStrings.DESTROY_HDFS_STORE__ERROR_WHILE_DESTROYING_REASON_0, new Object[] { th.getMessage() }));
    }
 }
  
  protected Result destroyStore(String hdfsStoreName , String[] groups){
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers = null;
      try {
        targetMembers = getGroupMembers(groups);
      } catch (CommandResultException cre) {
        return cre.getResult();
      }
      
      ResultCollector<?, ?> rc = getMembersFunctionExecutor(targetMembers)
      .withArgs(hdfsStoreName).execute(new DestroyHDFSStoreFunction());
      
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>)rc.getResult());

      XmlEntity xmlEntity = null;
      for (CliFunctionResult result : results) {
        
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", "ERROR: " + result.getThrowable().getClass().getName() + ": "
              + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Status.ERROR);
        }
        else if (result.getMessage() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", result.getMessage());
          accumulatedData = true;
          
          if (xmlEntity == null) {
            xmlEntity = result.getXmlEntity();
          }
        }
      }
      
      if (!accumulatedData) {
        return ResultBuilder.createInfoResult("No matching hdfs stores found.");
      }
      
      Result result = ResultBuilder.buildResult(tabularData);
      if (xmlEntity != null) {
        result.setCommandPersisted((new SharedConfigurationWriter()).deleteXmlEntity(xmlEntity, groups));
      }
      
      return result;
  }
  @CliCommand(value=CliStrings.ALTER_HDFS_STORE, help=CliStrings.ALTER_HDFS_STORE__HELP)
  @CliMetaData(shellOnly=false, relatedTopic={CliStrings.TOPIC_GEMFIRE_HDFSSTORE}, writesToSharedConfiguration=true)
  public Result alterHdfstore(
      @CliOption (key = CliStrings.ALTER_HDFS_STORE__NAME,                  
          mandatory = true,
          optionContext = ConverterHint.HDFSSTORE_ALL, 
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.ALTER_HDFS_STORE__NAME__HELP)
      String hdfsUniqueName,     
      @CliOption (key = CliStrings.ALTER_HDFS_STORE__BATCHSIZE,
          mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.ALTER_HDFS_STORE__BATCHSIZE__HELP)
      Integer batchSize,
      @CliOption (key = CliStrings.ALTER_HDFS_STORE__BATCHINTERVAL,
          mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.ALTER_HDFS_STORE__BATCHINTERVAL__HELP)
      Integer batchInterval,      
      @CliOption (key = CliStrings.ALTER_HDFS_STORE__MINORCOMPACT,
          mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.ALTER_HDFS_STORE__MINORCOMPACT__HELP)
      Boolean minorCompact,                                                                                                         
      @CliOption (key = CliStrings.ALTER_HDFS_STORE__MINORCOMPACTIONTHREADS,
          mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.ALTER_HDFS_STORE__MINORCOMPACTIONTHREADS__HELP)
      Integer minorCompactionThreads,
      @CliOption (key = CliStrings.ALTER_HDFS_STORE__MAJORCOMPACT,
          mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.ALTER_HDFS_STORE__MAJORCOMPACT__HELP)
      Boolean majorCompact,   
      @CliOption (key = CliStrings.ALTER_HDFS_STORE__MAJORCOMPACTINTERVAL,
          mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.ALTER_HDFS_STORE__MAJORCOMPACTINTERVAL__HELP)
      Integer majorCompactionInterval, 
      @CliOption (key = CliStrings.ALTER_HDFS_STORE__MAJORCOMPACTIONTHREADS,
          mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.ALTER_HDFS_STORE__MAJORCOMPACTIONTHREADS__HELP)
      Integer majorCompactionThreads,  
      @CliOption (key = CliStrings.ALTER_HDFS_STORE__PURGEINTERVAL,
          mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.ALTER_HDFS_STORE__PURGEINTERVAL__HELP)
      Integer purgeInterval,        
      @CliOption (key = CliStrings.ALTER_HDFS_STORE__FILEROLLOVERINTERVAL,
          mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.ALTER_HDFS_STORE__FILEROLLOVERINTERVAL__HELP)
      Integer fileRolloverInterval,
      @CliOption (key = CliStrings.ALTER_HDFS_STORE__WRITEONLYFILESIZE,
          mandatory = false,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.ALTER_HDFS_STORE__WRITEONLYFILESIZE__HELP)
      Integer maxWriteonlyFileSize,  
      @CliOption(key=CliStrings.ALTER_HDFS_STORE__GROUP,
         help=CliStrings.ALTER_HDFS_STORE__GROUP__HELP,
         optionContext=ConverterHint.MEMBERGROUP)
      @CliMetaData (valueSeparator = ",")
      String[] groups){
    try {                         
      
      return getAlteredHDFSStore(groups, hdfsUniqueName, batchSize, batchInterval, minorCompact,
          minorCompactionThreads, majorCompact, majorCompactionInterval, majorCompactionThreads, purgeInterval,
          fileRolloverInterval, maxWriteonlyFileSize);
      
    } catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN,
          CliStrings.ALTER_HDFS_STORE));
      
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
      
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(
          CliStrings.ALTER_HDFS_STORE__ERROR_WHILE_ALTERING_REASON_0, new Object[] { th.getMessage() }));
    }
 }
  
  
  protected Result getAlteredHDFSStore(String[] groups, String hdfsUniqueName, Integer batchSize,
      Integer batchInterval, Boolean minorCompact, Integer minorCompactionThreads, Boolean majorCompact,
      Integer majorCompactionInterval, Integer majorCompactionThreads, Integer purgeInterval,
      Integer fileRolloverInterval, Integer maxWriteonlyFileSize) {
    
    Set<DistributedMember> targetMembers = null;
    try {
      targetMembers = getGroupMembers(groups);
    } catch (CommandResultException cre) {
      return cre.getResult();
    }
    
    TabularResultData tabularData = ResultBuilder.createTabularResultData();
    
	AlterHDFSStoreAttributes alterAttributes = new AlterHDFSStoreAttributes(
				hdfsUniqueName, batchSize, batchInterval, minorCompact,
				majorCompact, minorCompactionThreads, majorCompactionInterval,
				majorCompactionThreads, purgeInterval, fileRolloverInterval,
				maxWriteonlyFileSize);
	
    ResultCollector<?, ?> rc = getMembersFunctionExecutor(targetMembers)
    .withArgs(alterAttributes).execute(new AlterHDFSStoreFunction());
    
    List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>)rc.getResult());

    XmlEntity xmlEntity = null;

    for (CliFunctionResult result : results) {
      if (result.getThrowable() != null) {
        tabularData.accumulate("Member", result.getMemberIdOrName());
        tabularData.accumulate("Result", "ERROR: " + result.getThrowable().getClass().getName() + ": "
            + result.getThrowable().getMessage());
        tabularData.setStatus(Status.ERROR);
      }
      else if (result.getMessage() != null) {
        tabularData.accumulate("Member", result.getMemberIdOrName());
        tabularData.accumulate("Result", result.getMessage());

        if (xmlEntity == null) {
          xmlEntity = result.getXmlEntity();
        }
      }
    }
    
    Result result = ResultBuilder.buildResult(tabularData);
    
    if (xmlEntity != null) {
      result.setCommandPersisted((new SharedConfigurationWriter()).deleteXmlEntity(xmlEntity, groups));
    }
    
    return result;
  }
  @CliAvailabilityIndicator({CliStrings.CREATE_HDFS_STORE, CliStrings.LIST_HDFS_STORE,
    CliStrings.DESCRIBE_HDFS_STORE, CliStrings.ALTER_HDFS_STORE, CliStrings.DESTROY_HDFS_STORE})
  public boolean hdfsStoreCommandsAvailable() {
    // these hdfs store commands are always available in GemFire
    return (!CliUtil.isGfshVM() || (getGfsh() != null && getGfsh().isConnectedAndReady()));
  }  
  
  @Override
  protected Set<DistributedMember> getMembers(final Cache cache) {
    return CliUtil.getAllMembers(cache);
  }
  
  protected Set<DistributedMember> getNormalMembers(final Cache cache) {
    return CliUtil.getAllNormalMembers(cache);
  }
  
  protected Set<DistributedMember> getGroupMembers(String[] groups) throws CommandResultException {    
      return  CliUtil.findAllMatchingMembers(groups, null); 
  }
  
}
