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
   
   
package org.apache.geode.internal.admin.remote;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogWriterAppender;
import org.apache.geode.internal.logging.log4j.LogWriterAppenders;
import org.apache.logging.log4j.Logger;

import java.io.*;

//import org.apache.geode.distributed.DistributedSystem;
//import java.util.*;

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
