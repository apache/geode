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
package com.gemstone.gemfire.admin.jmx.internal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * Provides the ways to send emails to all the registered email id It also
 * provides the way to add/remove email ids. Can be used to send email in case
 * of any alerts raised / warning / failure in gemfire.
 * 
 * @since 5.1
 */
public class MailManager {

  private static final Logger logger = LogService.getLogger();
  
  public MailManager() {
  }

  public MailManager(Properties mailProperties) {
    setMailProperties(mailProperties);
  }

  public MailManager(File mailPropertiesFile) throws IOException {
    Properties prop = new Properties();
    FileInputStream fio = new FileInputStream(mailPropertiesFile);
    try {
      prop.load(fio);
    }
    finally {
      fio.close();
    }
    setMailProperties(prop);
  }

  public MailManager(String mailHost, String mailFrom) {
    this.mailHost = mailHost;
    this.mailFrom = mailFrom;
  }

  /**
   * Send email to all the registered email id with given subject and message
   */
  public void sendEmail(String subject, String message) {
    processEmail(new EmailData(subject, message));
  }

  /**
   * Send Emails to all the registered email id
   * 
   * @param emailData
   *                Instance of EmailData
   */
  // Why a separate method & class EmailData needed??? 
  private void processEmail(EmailData emailData) {
    if (logger.isTraceEnabled()) {
      logger.trace("Entered MailManager:processEmail");
    }

    if (mailHost == null || mailHost.length() == 0
        || emailData == null || mailToAddresses.length == 0) {
      logger.error(LocalizedMessage.create(LocalizedStrings.MailManager_REQUIRED_MAILSERVER_CONFIGURATION_NOT_SPECIFIED));
      if (logger.isDebugEnabled()) {
        logger.debug("Exited MailManager:processEmail: Not sending email as conditions not met");
      }
      return;
    }

    Session session = Session.getDefaultInstance(getMailHostConfiguration());
    MimeMessage mimeMessage = new MimeMessage(session);
    String subject = emailData.subject;
    String message = emailData.message;
    String mailToList = getMailToAddressesAsString();

    try {
      for (int i = 0; i < mailToAddresses.length; i++) {
        mimeMessage.addRecipient(Message.RecipientType.TO, new InternetAddress(
            mailToAddresses[i]));
      }

      if (subject == null) {
        subject = LocalizedStrings.MailManager_ALERT_MAIL_SUBJECT.toLocalizedString();
      }
      mimeMessage.setSubject(subject);

      if (message == null) {
        message = "";
      }
      mimeMessage.setText(message);

      Transport.send(mimeMessage);
      logger.info(LocalizedMessage.create(
          LocalizedStrings.MailManager_EMAIL_ALERT_HAS_BEEN_SENT_0_1_2,
          new Object[] { mailToList, subject, message }));
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable ex) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      StringBuilder buf = new StringBuilder();
      buf.append(LocalizedStrings.MailManager_AN_EXCEPTION_OCCURRED_WHILE_SENDING_EMAIL.toLocalizedString());
      buf.append(LocalizedStrings.MailManager_UNABLE_TO_SEND_EMAIL_PLEASE_CHECK_YOUR_EMAIL_SETTINGS_AND_LOG_FILE.toLocalizedString());
      buf.append("\n\n").append(LocalizedStrings.MailManager_EXCEPTION_MESSAGE_0.toLocalizedString(ex.getMessage()));
      buf.append("\n\n").append(LocalizedStrings.MailManager_FOLLOWING_EMAIL_WAS_NOT_DELIVERED.toLocalizedString());
      buf.append("\n\t").append(LocalizedStrings.MailManager_MAIL_HOST_0.toLocalizedString(mailHost));
      buf.append("\n\t").append(LocalizedStrings.MailManager_FROM_0.toLocalizedString(mailFrom));
      buf.append("\n\t").append(LocalizedStrings.MailManager_TO_0.toLocalizedString(mailToList));
      buf.append("\n\t").append(LocalizedStrings.MailManager_SUBJECT_0.toLocalizedString(subject));
      buf.append("\n\t").append(LocalizedStrings.MailManager_CONTENT_0.toLocalizedString(message));

      logger.error(buf.toString(), ex);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Exited MailManager:processEmail");
    }
  }

  /**
   * Not yet implemented
   */
  public void close() {
  }

  /**
   * @return All the registered email id as string
   */
  private String getMailToAddressesAsString() {
    StringBuffer mailToList = new StringBuffer();
    for (int i = 0; i < mailToAddresses.length; i++) {
      mailToList.append(mailToAddresses[i]);
      mailToList.append(", ");
    }
    return mailToList.toString();
  }

  /**
   * 
   * @return Properties consisting mailHost and mailFrom property
   */
  private Properties getMailHostConfiguration() {
    Properties result = new Properties();
    if (mailHost == null) {
      mailHost = "";
    }
    if (mailFrom == null) {
      mailFrom = "";
    }
    result.setProperty("mail.host", mailHost);
    result.put("mail.from", mailFrom);
    return result;
  }

  /**
   * 
   * @param host
   *                mail host server name
   */
  public void setMailHost(String host) {
    this.mailHost = host;
  }

  /**
   * 
   * @return mail host server name
   */
  public String getMailHost() {
    return this.mailHost;
  }

  /**
   * 
   * @param fromAddress
   *                mailFrom email id
   */
  public void setMailFromAddress(String fromAddress) {
    mailFrom = fromAddress;
  }

  /**
   * 
   * @return mailFrom email id
   */
  public String getMailFromAddress() {
    return mailFrom;
  }

  /**
   * add new mail id to ToList
   */
  public void addMailToAddress(String toAddress) {
    mailToSet.add(toAddress);
    mailToAddresses = getAllToAddresses();
  }

  /**
   * remove given mail id from ToList
   */
  public void removeMailToAddress(String toAddress) {
    mailToSet.remove(toAddress);
    mailToAddresses = getAllToAddresses();
  }

  /**
   * @return list all the registered email id
   */
  public String[] getAllToAddresses() {
    return (String[])mailToSet.toArray(new String[0]);
  }

  /**
   * remove all the registered email ids from ToList
   */
  public void removeAllMailToAddresses() {
    mailToSet.clear();
    mailToAddresses = new String[0];
  }

  /**
   * Set the mail properties, e.g mail host, mailFrom, MailTo etc
   */
  public void setMailProperties(Properties mailProperties) {
    mailHost = mailProperties.getProperty(PROPERTY_MAIL_HOST);
    mailFrom = mailProperties.getProperty(PROPERTY_MAIL_FROM);
    String mailList = mailProperties.getProperty(PROPERTY_MAIL_TO_LIST, "");
    String split[] = mailList.split(",");
    removeAllMailToAddresses();
    for (int i = 0; i < split.length; i++) {
      addMailToAddress(split[i].trim());
    }
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer(200);
    buffer.append("[Mail Host: ");
    buffer.append(getMailHost());
    buffer.append("]");
    buffer.append(" [Mail From: ");
    buffer.append(getMailFromAddress());
    buffer.append("]");
    buffer.append(" [Mail To: ");
    if (mailToAddresses.length > 0) {

      for (int i = 0; i < mailToAddresses.length; i++) {
        buffer.append(mailToAddresses[i]);
        buffer.append(", ");
      }
      buffer.replace(buffer.length() - 2, buffer.length(), "");
    }
    else {
      buffer.append(" Undefined");
    }
    buffer.append("]");
    return buffer.toString();
  }

  private HashSet mailToSet = new HashSet();

  private String mailToAddresses[] = new String[0];

  protected String mailHost;

  protected String mailFrom;

  public final static String PROPERTY_MAIL_HOST = "mail.host";

  public final static String PROPERTY_MAIL_FROM = "mail.from";

  public final static String PROPERTY_MAIL_TO_LIST = "mail.toList";

  /**
   * Incorporating subject and message of email
   * 
   * 
   */
  static private class EmailData {
    String subject;

    String message;

    EmailData(String subject, String message) {
      this.subject = subject;
      this.message = message;
    }
  }

  public static void main(String args[]) {
    MailManager mailManager = new MailManager("mailsrv1.gemstone.com",
        "hkhanna@gemstone.com");
    mailManager.sendEmail("Alert!", "Test");
  }
}
