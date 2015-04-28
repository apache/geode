/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
//import com.gemstone.gemfire.distributed.DistributedSystem;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.ManagerLogWriter;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterAppender;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterAppenders;

public final class TailLogResponse extends AdminResponse {
  private static final Logger logger = LogService.getLogger();
  
  private String tail;
  private String childTail;

  public static TailLogResponse create(DistributionManager dm, InternalDistributedMember recipient){
    TailLogResponse m = new TailLogResponse();
    m.setRecipient(recipient);
    try {
      InternalDistributedSystem sys = dm.getSystem();
      LogWriterAppender lwa = LogWriterAppenders.getAppender(LogWriterAppenders.Identifier.MAIN);
      if (lwa != null) {
        m.childTail = tailSystemLog(lwa.getChildLogFile());
        m.tail = tailSystemLog(sys.getConfig());
        if (m.tail == null) {
          m.tail = LocalizedStrings.TailLogResponse_NO_LOG_FILE_WAS_SPECIFIED_IN_THE_CONFIGURATION_MESSAGES_WILL_BE_DIRECTED_TO_STDOUT.toLocalizedString(); 
        }
      } else {
        //Assert.assertTrue(false, "TailLogRequest/Response processed in application vm with shared logging.");
        m.childTail = tailSystemLog((File)null);
        m.tail = tailSystemLog(sys.getConfig());
        if (m.tail == null) {
          m.tail = LocalizedStrings.TailLogResponse_NO_LOG_FILE_WAS_SPECIFIED_IN_THE_CONFIGURATION_MESSAGES_WILL_BE_DIRECTED_TO_STDOUT.toLocalizedString(); 
        }
      }
    } catch (IOException e){
      logger.warn(LocalizedMessage.create(LocalizedStrings.TailLogResponse_ERROR_OCCURRED_WHILE_READING_SYSTEM_LOG__0, e));
      m.tail = "";
    }
    return m;
  }

  public int getDSFID() {
    return TAIL_LOG_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(tail, out);
    DataSerializer.writeString(childTail, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    tail = DataSerializer.readString(in);
    childTail = DataSerializer.readString(in);
  }

  public String getTail(){
    return tail;
  }

  public String getChildTail() {
    return childTail;
  }

  @Override
  public String toString(){
    return "TailLogResponse from " + this.getRecipient() + " <TAIL>" + getTail() + "</TAIL>";
  }

  public static String tailSystemLog(File logFile) throws IOException {
    if (logFile == null || logFile.equals(new File(""))) {
      return null;
    }
    int numLines = 30;
    int maxBuffer = 65500; //DataOutput.writeUTF will only accept 65535 bytes
    long fileLength = logFile.length();
    byte[] buffer = (fileLength > maxBuffer) ? new byte[maxBuffer] : new byte[(int)fileLength];
    int readSize = buffer.length;
    RandomAccessFile f = new RandomAccessFile(logFile, "r");
    f.seek(fileLength - readSize);
    f.read(buffer, 0, readSize);
    f.close();
    
    String messageString = new String( buffer );
    char[] text = messageString.toCharArray();
    for (int i=text.length-1,j=0; i>=0; i--){
      if (text[i] == '[')
        j++;
      if (j == numLines){
        messageString = messageString.substring(i);
        break;
      }
    }    
    return messageString.trim();
  }

//  private static String readSystemLog(File logFile) throws IOException {
//    if (logFile == null || logFile.equals(new File(""))) {
//      return null;
//    }
//    long fileLength = logFile.length();
//    byte[] buffer = new byte[(int)fileLength];
//    BufferedInputStream in = new BufferedInputStream(new FileInputStream(logFile));
//    in.read(buffer, 0, buffer.length);
//    return new String(buffer).trim();
//  }

//  private static String readSystemLog(DistributionConfig sc) throws IOException {
//    File logFile = sc.getLogFile();
//    if (logFile == null || logFile.equals(new File(""))) {
//      return null;
//    }
//    if (!logFile.isAbsolute()) {
//      logFile = new File(logFile.getAbsolutePath());
//    }    
//    return readSystemLog(logFile);
//  }  
  
  private static String tailSystemLog(DistributionConfig sc) throws IOException {
    File logFile = sc.getLogFile();
    if (logFile == null || logFile.equals(new File(""))) {
      return null;
    }
    if (!logFile.isAbsolute()) {
      logFile = new File(logFile.getAbsolutePath());
    }    
    return tailSystemLog(logFile);
  }

}
