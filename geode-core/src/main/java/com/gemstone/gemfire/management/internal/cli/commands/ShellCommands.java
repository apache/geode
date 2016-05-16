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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyStore;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.DSFIDFactory;
import com.gemstone.gemfire.internal.lang.Initializer;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.internal.util.PasswordUtil;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.JmxManagerLocatorRequest;
import com.gemstone.gemfire.management.internal.JmxManagerLocatorResponse;
import com.gemstone.gemfire.management.internal.SSLUtil;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.annotation.CliArgument;
import com.gemstone.gemfire.management.internal.cli.converters.ConnectionEndpointConverter;
import com.gemstone.gemfire.management.internal.cli.domain.ConnectToLocatorResult;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.ErrorResultData;
import com.gemstone.gemfire.management.internal.cli.result.InfoResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.cli.shell.JMXConnectionException;
import com.gemstone.gemfire.management.internal.cli.shell.JmxOperationInvoker;
import com.gemstone.gemfire.management.internal.cli.shell.OperationInvoker;
import com.gemstone.gemfire.management.internal.cli.shell.jline.GfshHistory;
import com.gemstone.gemfire.management.internal.cli.util.CauseFinder;
import com.gemstone.gemfire.management.internal.cli.util.ConnectionEndpoint;
import com.gemstone.gemfire.management.internal.web.domain.LinkIndex;
import com.gemstone.gemfire.management.internal.web.http.support.SimpleHttpRequester;
import com.gemstone.gemfire.management.internal.web.shell.HttpOperationInvoker;
import com.gemstone.gemfire.management.internal.web.shell.RestHttpOperationInvoker;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.ExitShellRequest;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

/**
 *
 * @since 7.0
 */
public class ShellCommands implements CommandMarker {

  private Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  @CliCommand(value = { CliStrings.EXIT, "quit" }, help = CliStrings.EXIT__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public ExitShellRequest exit() throws IOException {
    Gfsh gfshInstance = getGfsh();

    gfshInstance.stop();

    ExitShellRequest exitShellRequest = gfshInstance.getExitShellRequest();
    if (exitShellRequest == null) {
      // shouldn't really happen, but we'll fallback to this anyway
      exitShellRequest = ExitShellRequest.NORMAL_EXIT;
    }

    return exitShellRequest;
  }

  // millis that connect --locator will wait for a response from the locator.
  private final static int CONNECT_LOCATOR_TIMEOUT_MS = 60000; // see bug 45971

  public static int getConnectLocatorTimeoutInMS() {
    return ShellCommands.CONNECT_LOCATOR_TIMEOUT_MS;
  }

  @CliCommand(value = { CliStrings.CONNECT }, help = CliStrings.CONNECT__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEMFIRE_JMX, CliStrings.TOPIC_GEMFIRE_MANAGER})
  public Result connect(
    @CliOption(key = { CliStrings.CONNECT__LOCATOR },
               unspecifiedDefaultValue = ConnectionEndpointConverter.DEFAULT_LOCATOR_ENDPOINTS,
               optionContext = ConnectionEndpoint.LOCATOR_OPTION_CONTEXT,
               help = CliStrings.CONNECT__LOCATOR__HELP) ConnectionEndpoint locatorTcpHostPort,
    @CliOption(key = { CliStrings.CONNECT__JMX_MANAGER },
               unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
               optionContext = ConnectionEndpoint.JMXMANAGER_OPTION_CONTEXT,
               help = CliStrings.CONNECT__JMX_MANAGER__HELP) ConnectionEndpoint memberRmiHostPort,
    @CliOption(key = { CliStrings.CONNECT__USE_HTTP },
               mandatory = false,
               specifiedDefaultValue = "true",
               unspecifiedDefaultValue = "false",
               help = CliStrings.CONNECT__USE_HTTP__HELP) boolean useHttp,
    @CliOption(key = { CliStrings.CONNECT__URL },
               mandatory = false,
               unspecifiedDefaultValue = CliStrings.CONNECT__DEFAULT_BASE_URL,
               help = CliStrings.CONNECT__URL__HELP) String url,
    @CliOption(key = { CliStrings.CONNECT__USERNAME },
               unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
               help = CliStrings.CONNECT__USERNAME__HELP) String userName,
    @CliOption(key = { CliStrings.CONNECT__PASSWORD },
               unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
               help = CliStrings.CONNECT__PASSWORD__HELP) String password,
    @CliOption(key = { CliStrings.CONNECT__KEY_STORE },
               unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
               help = CliStrings.CONNECT__KEY_STORE__HELP) String keystore,
    @CliOption(key = { CliStrings.CONNECT__KEY_STORE_PASSWORD },
               unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
               help = CliStrings.CONNECT__KEY_STORE_PASSWORD__HELP) String keystorePassword,
    @CliOption(key = { CliStrings.CONNECT__TRUST_STORE },
               unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
               help = CliStrings.CONNECT__TRUST_STORE__HELP) String truststore,
    @CliOption(key = { CliStrings.CONNECT__TRUST_STORE_PASSWORD },
               unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
               help = CliStrings.CONNECT__TRUST_STORE_PASSWORD__HELP) String truststorePassword,
    @CliOption(key = { CliStrings.CONNECT__SSL_CIPHERS },
               unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
               help = CliStrings.CONNECT__SSL_CIPHERS__HELP) String sslCiphers,
    @CliOption(key = { CliStrings.CONNECT__SSL_PROTOCOLS },
               unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
               help = CliStrings.CONNECT__SSL_PROTOCOLS__HELP) String sslProtocols,
    @CliOption(key = CliStrings.CONNECT__SECURITY_PROPERTIES,
               optionContext = ConverterHint.FILE_PATHSTRING,
               unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
               help = CliStrings.CONNECT__SECURITY_PROPERTIES__HELP) final String gfSecurityPropertiesPath,
    @CliOption(key = { CliStrings.CONNECT__USE_SSL },
               specifiedDefaultValue = "true",
               unspecifiedDefaultValue = "false",
               help = CliStrings.CONNECT__USE_SSL__HELP) final boolean useSsl)
  {
    Result result;
    
    String passwordToUse           = decrypt(password);
    String keystoreToUse           = keystore;
    String keystorePasswordToUse   = keystorePassword;
    String truststoreToUse         = truststore;
    String truststorePasswordToUse = truststorePassword;
    String sslCiphersToUse         = sslCiphers;
    String sslProtocolsToUse       = sslProtocols;

    // TODO shouldn't the condition be (getGfsh() != null && getGfsh().isConnectedAndReady())?
    // otherwise, we have potential NullPointerException on the line with getGfsh().getOperationInvoker()
    //if (getGfsh() == null || getGfsh().isConnectedAndReady()) {
    if (getGfsh() != null && getGfsh().isConnectedAndReady()) {
      try {
        result = ResultBuilder.createInfoResult("Already connected to: " + getGfsh().getOperationInvoker().toString());
      } catch (Exception e) {
        result = ResultBuilder.buildResult(ResultBuilder.createErrorResultData().setErrorCode(
          ResultBuilder.ERRORCODE_DEFAULT).addLine(e.getMessage()));
      }
    } else if (useHttp) {      
      Gfsh gemfireShell = getGfsh();
      try{

        Map<String,String> securityProperties = new HashMap<String, String>();

        if (userName != null && userName.length() > 0) {
          if (passwordToUse == null || passwordToUse.length() == 0) {
            passwordToUse = gemfireShell.readWithMask("password: ", '*');
          }
          if (passwordToUse == null || passwordToUse.length() == 0) {
            throw new IllegalArgumentException(CliStrings.CONNECT__MSG__JMX_PASSWORD_MUST_BE_SPECIFIED);
          }
          securityProperties.put("security-username", userName);
          securityProperties.put("security-password", passwordToUse);
        }

        final Map<String, String> sslConfigProps = this.readSSLConfiguration(useSsl, keystoreToUse,keystorePasswordToUse,
            truststoreToUse, truststorePasswordToUse, sslCiphersToUse, sslProtocolsToUse, gfSecurityPropertiesPath);
     
        if (useSsl) {
          configureHttpsURLConnection(sslConfigProps);
          if (url.startsWith("http:")) {
            url = url.replace("http:", "https:");
          }
        }

        Iterator<String> it = sslConfigProps.keySet().iterator();
        while(it.hasNext()){
          String secKey = it.next();
          securityProperties.put(secKey, sslConfigProps.get(secKey));
        }

        // This is so that SSL termination results in https URLs being returned
        String query = (url.startsWith("https")) ? "?scheme=https" : "";

        LogWrapper.getInstance().warning(String.format("Sending HTTP request for Link Index at (%1$s)...", url.concat("/index").concat(query)));

        LinkIndex linkIndex = new SimpleHttpRequester(gemfireShell, CONNECT_LOCATOR_TIMEOUT_MS, securityProperties).exchange(url.concat("/index").concat(query), LinkIndex.class);

        LogWrapper.getInstance().warning(String.format("Received Link Index (%1$s)", linkIndex.toString()));

        HttpOperationInvoker operationInvoker = new RestHttpOperationInvoker(linkIndex, gemfireShell, url, securityProperties);

        Initializer.init(operationInvoker);
        gemfireShell.setOperationInvoker(operationInvoker);

        LogWrapper.getInstance().info(CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, operationInvoker.toString()));

        Gfsh.redirectInternalJavaLoggers();
        result = ResultBuilder.createInfoResult(CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, operationInvoker.toString()));
        
      } catch (IOException ioe) {
        String errorMessage = ioe.getMessage();
        result = ResultBuilder.createConnectionErrorResult(errorMessage);
        if (gemfireShell.getDebug()) {ioe.printStackTrace();}
      } catch (Exception e) {
        String errorMessage = e.getMessage();
        result = ResultBuilder.createConnectionErrorResult(errorMessage);
        if (gemfireShell.getDebug()) {e.printStackTrace();}
      }
    } else {

      boolean isConnectingViaLocator = false;

      InfoResultData infoResultData = ResultBuilder.createInfoResultData();
      ConnectionEndpoint hostPortToConnect = null;
      
      try {
        Gfsh gfshInstance = getGfsh();

        // JMX Authentication Config
        if (userName != null && userName.length() > 0) {
          if (passwordToUse == null || passwordToUse.length() == 0) {
            passwordToUse = gfshInstance.readWithMask("password: ", '*');
          }
          if (passwordToUse == null || passwordToUse.length() == 0) {
              throw new IllegalArgumentException(CliStrings.CONNECT__MSG__JMX_PASSWORD_MUST_BE_SPECIFIED);
          }
        }

        final Map<String, String> sslConfigProps = this.readSSLConfiguration(useSsl, keystoreToUse,keystorePasswordToUse, 
            truststoreToUse, truststorePasswordToUse, sslCiphersToUse, sslProtocolsToUse, gfSecurityPropertiesPath);

        if (memberRmiHostPort != null) {
          hostPortToConnect = memberRmiHostPort;
          Gfsh.println(CliStrings.format(CliStrings.CONNECT__MSG__CONNECTING_TO_MANAGER_AT_0, new Object[] {memberRmiHostPort.toString(false)}));
        } else {
          isConnectingViaLocator = true;
          hostPortToConnect = locatorTcpHostPort;
          Gfsh.println(CliStrings.format(CliStrings.CONNECT__MSG__CONNECTING_TO_LOCATOR_AT_0, new Object[] {locatorTcpHostPort.toString(false)}));

          // Props required to configure a SocketCreator with SSL.
          // Used for gfsh->locator connection & not needed for gfsh->manager connection
          if (useSsl || !sslConfigProps.isEmpty()) {
            //Fix for 51266 : Added an check for cluster-ssl-enabled proeprty
            if(!sslConfigProps.containsKey(DistributionConfig.CLUSTER_SSL_ENABLED_NAME))
              sslConfigProps.put(DistributionConfig.SSL_ENABLED_NAME, String.valueOf(true));
            sslConfigProps.put(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
            sslConfigProps.put(DistributionConfig.LOCATORS_NAME, "");

            String sslInfoLogMsg = "Connecting to Locator via SSL.";
            if (useSsl) {
              sslInfoLogMsg = CliStrings.CONNECT__USE_SSL + " is set to true. " + sslInfoLogMsg;
            }
            gfshInstance.logToFile(sslInfoLogMsg, null);
          }

          ConnectToLocatorResult connectToLocatorResult = connectToLocator(locatorTcpHostPort.getHost(), locatorTcpHostPort.getPort(), CONNECT_LOCATOR_TIMEOUT_MS, sslConfigProps);
          memberRmiHostPort = connectToLocatorResult.getMemberEndpoint();
          hostPortToConnect = memberRmiHostPort;
          Gfsh.printlnErr(connectToLocatorResult.getResultMessage());

          // when locator is configured to use SSL (ssl-enabled=true) but manager is not (jmx-manager-ssl=false)
          if ((useSsl || !sslConfigProps.isEmpty()) && !connectToLocatorResult.isJmxManagerSslEnabled()) {
            gfshInstance.logInfo(CliStrings.CONNECT__USE_SSL + " is set to true. But JMX Manager doesn't support SSL, connecting without SSL.", null);
            sslConfigProps.clear();
          }
        }

        if (!sslConfigProps.isEmpty()) {
          gfshInstance.logToFile("Connecting to manager via SSL.", null);
        }

        JmxOperationInvoker operationInvoker = new JmxOperationInvoker(memberRmiHostPort.getHost(), memberRmiHostPort.getPort(), userName, passwordToUse, sslConfigProps, gfSecurityPropertiesPath);
        gfshInstance.setOperationInvoker(operationInvoker);
        infoResultData.addLine(CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, memberRmiHostPort.toString(false)));
        LogWrapper.getInstance().info(CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, memberRmiHostPort.toString(false)));
        result = ResultBuilder.buildResult(infoResultData);
      } catch (Exception e) {
        // TODO - Abhishek: Refactor to use catch blocks for instanceof checks
        Gfsh gfshInstance = Gfsh.getCurrentInstance();
        String errorMessage = e.getMessage();
        boolean logAsFine = false;
        if (CauseFinder.indexOfCause(e, javax.naming.ServiceUnavailableException.class, false) != -1) {
          errorMessage = CliStrings.format(CliStrings.CONNECT__MSG__SERVICE_UNAVAILABLE_ERROR, hostPortToConnect.toString(false));
        } else if (e instanceof JMXConnectionException) {
          JMXConnectionException jce = (JMXConnectionException)e;
          if (jce.getExceptionType() == JMXConnectionException.MANAGER_NOT_FOUND_EXCEPTION) {
            errorMessage = CliStrings.format(CliStrings.CONNECT__MSG__SERVICE_UNAVAILABLE_ERROR, hostPortToConnect.toString(false));
          }
        } else if ((e instanceof ConnectException) && isConnectingViaLocator) {
          errorMessage = CliStrings.format(CliStrings.CONNECT__MSG__COULD_NOT_CONNECT_TO_LOCATOR_0, hostPortToConnect.toString(false));
        } else if ( (e instanceof IllegalStateException) && isConnectingViaLocator) {
          Throwable causeByType = CauseFinder.causeByType(e, ClassCastException.class, false);
          if (causeByType != null) {
            errorMessage = CliStrings.format(CliStrings.CONNECT__MSG__COULD_NOT_CONNECT_TO_LOCATOR_0_POSSIBLY_SSL_CONFIG_ERROR,
                                             new Object[] { hostPortToConnect.toString(false)});
            if (gfshInstance.isLoggingEnabled()) {
              errorMessage += " "+ getGfshLogsCheckMessage(gfshInstance.getLogFilePath());
            }
          } else if (errorMessage == null) {
            errorMessage = CliStrings.format(CliStrings.CONNECT__MSG__COULD_NOT_CONNECT_TO_LOCATOR_0, locatorTcpHostPort.toString(false));
            if (gfshInstance.isLoggingEnabled()) {
              errorMessage += " "+ getGfshLogsCheckMessage(gfshInstance.getLogFilePath());
            }
          }
        } else if (e instanceof IOException) {
          Throwable causeByType = CauseFinder.causeByType(e, java.rmi.ConnectIOException.class, false);
          if (causeByType != null) {
            // TODO - Abhishek : Is there a better way to know about a specific cause?
            if (String.valueOf(causeByType.getMessage()).contains("non-JRMP server")) {
              errorMessage = CliStrings.format(CliStrings.CONNECT__MSG__COULD_NOT_CONNECT_TO_MANAGER_0_POSSIBLY_SSL_CONFIG_ERROR,
                                               new Object[] { memberRmiHostPort.toString(false)});
              logAsFine = true;
            } else {
              errorMessage = CliStrings.format(CliStrings.CONNECT__MSG__ERROR, new Object[] {memberRmiHostPort.toString(false), ""});
            }
            if (gfshInstance.isLoggingEnabled()) {
              errorMessage += " "+ getGfshLogsCheckMessage(gfshInstance.getLogFilePath());
            }
          }
        } else if (e instanceof SecurityException) {
          // the default exception message is clear enough
          String msgPart = StringUtils.isBlank(userName) && StringUtils.isBlank(passwordToUse) ? "" : "appropriate ";
          errorMessage += ". Please specify "+msgPart+"values for --"+CliStrings.CONNECT__USERNAME+" and --"+CliStrings.CONNECT__PASSWORD;
        } else{
          errorMessage = CliStrings.format(CliStrings.CONNECT__MSG__ERROR, hostPortToConnect.toString(false), errorMessage);
        }
        result = ResultBuilder.createConnectionErrorResult(errorMessage);
        if (logAsFine) {
          LogWrapper.getInstance().fine(e.getMessage(), e);
        } else {
          LogWrapper.getInstance().severe(e.getMessage(), e);
        }
      }

      Gfsh.redirectInternalJavaLoggers();
    }

    return result;
  }

  private String decrypt(String password) {
    if (password != null) {
      return PasswordUtil.decrypt(password);
    }
    return null;
  }

private void configureHttpsURLConnection(Map<String, String> sslConfigProps) throws Exception {
    String keystoreToUse = sslConfigProps.get(Gfsh.SSL_KEYSTORE);
    String keystorePasswordToUse = sslConfigProps.get(Gfsh.SSL_KEYSTORE_PASSWORD);
    String truststoreToUse = sslConfigProps.get(Gfsh.SSL_TRUSTSTORE);
    String truststorePasswordToUse = sslConfigProps.get(Gfsh.SSL_TRUSTSTORE_PASSWORD);
    // Ciphers are not passed to HttpsURLConnection. Could not find a clean way
    // to pass this attribute to socket layer (see #51645)
    String sslCiphersToUse = sslConfigProps.get(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME);
    String sslProtocolsToUse = sslConfigProps.get(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME);

    //Commenting the code to set cipher suites in GFSH rest connect (see #51645)
    /*
    if(sslCiphersToUse != null){
      System.setProperty("https.cipherSuites", sslCiphersToUse);
    }
    */
    FileInputStream keyStoreStream = null;
    FileInputStream trustStoreStream = null;
    try{
      
      KeyManagerFactory keyManagerFactory = null;
      if (!StringUtils.isBlank(keystoreToUse)) {
        KeyStore clientKeys = KeyStore.getInstance("JKS");
        keyStoreStream = new FileInputStream(keystoreToUse);
        clientKeys.load(keyStoreStream, keystorePasswordToUse.toCharArray());

        keyManagerFactory = KeyManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(clientKeys, keystorePasswordToUse.toCharArray());
      }

      // load server public key
      TrustManagerFactory trustManagerFactory = null;
      if (!StringUtils.isBlank(truststoreToUse)) {
        KeyStore serverPub = KeyStore.getInstance("JKS");
        trustStoreStream = new FileInputStream(truststoreToUse);
        serverPub.load(trustStoreStream, truststorePasswordToUse.toCharArray());
        trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(serverPub);
      }

      SSLContext ssl = SSLContext.getInstance(SSLUtil.getSSLAlgo(SSLUtil.readArray(sslProtocolsToUse)));

      ssl.init(keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null,
          trustManagerFactory != null ? trustManagerFactory.getTrustManagers() : null, new java.security.SecureRandom());
   
      HttpsURLConnection.setDefaultSSLSocketFactory(ssl.getSocketFactory());
    }finally{
      if(keyStoreStream != null){
        keyStoreStream.close();
      }
      if(trustStoreStream != null ){
        trustStoreStream.close();
      }
      
    }
   
    
  }
  
  /**
   * Common code to read SSL information. Used by JMX, Locator & HTTP mode connect
   */
  private Map<String, String> readSSLConfiguration(boolean useSsl, String keystoreToUse, String keystorePasswordToUse,
      String truststoreToUse, String truststorePasswordToUse, String sslCiphersToUse, String sslProtocolsToUse,
      String gfSecurityPropertiesPath) throws IOException {

    Gfsh gfshInstance = getGfsh();    
    final Map<String, String> sslConfigProps = new LinkedHashMap<String, String>();
    
    // JMX SSL Config 1:
    // First from gfsecurity properties file if it's specified OR
    // if the default gfsecurity.properties exists useSsl==true
    if (useSsl || gfSecurityPropertiesPath != null) {
      // reference to hold resolved gfSecurityPropertiesPath
      String gfSecurityPropertiesPathToUse = CliUtil.resolvePathname(gfSecurityPropertiesPath);
      URL gfSecurityPropertiesUrl = null;

      // Case 1: User has specified gfSecurity properties file
      if (!StringUtils.isBlank(gfSecurityPropertiesPathToUse)) {
        // User specified gfSecurity properties doesn't exist
        if (!IOUtils.isExistingPathname(gfSecurityPropertiesPathToUse)) {
          gfshInstance.printAsSevere(CliStrings.format(CliStrings.GEMFIRE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, "Security ", gfSecurityPropertiesPathToUse));
        } else {
          gfSecurityPropertiesUrl = new File(gfSecurityPropertiesPathToUse).toURI().toURL();
        }
      } else if (useSsl && gfSecurityPropertiesPath == null) {
        // Case 2: User has specified to useSsl but hasn't specified
        // gfSecurity properties file. Use default "gfsecurity.properties"
        // in current dir, user's home or classpath
        gfSecurityPropertiesUrl = getFileUrl("gfsecurity.properties");
      }
      // if 'gfSecurityPropertiesPath' OR gfsecurity.properties has resolvable path
      if (gfSecurityPropertiesUrl != null) {
        gfshInstance.logToFile("Using security properties file : "
                + CliUtil.decodeWithDefaultCharSet(gfSecurityPropertiesUrl.getPath()), null);
        Map<String, String> gfsecurityProps = loadPropertiesFromURL(gfSecurityPropertiesUrl);
        // command line options (if any) would override props in gfsecurity.properties
        sslConfigProps.putAll(gfsecurityProps);
      }
    }

    int numTimesPrompted = 0;
    /*
     * Using do-while here for a case when --use-ssl=true is specified but
     * no SSL options were specified & there was no gfsecurity properties
     * specified or readable in default gfsh directory.
     *
     * NOTE: 2nd round of prompting is done only when sslConfigProps map is
     * empty & useSsl is true - so we won't over-write any previous values.
     */
    do {
      // JMX SSL Config 2: Now read the options
      if (numTimesPrompted > 0) {
        Gfsh.println("Please specify these SSL Configuration properties: ");
      }

      if (numTimesPrompted > 0) {
        //NOTE: sslConfigProps map was empty
        keystoreToUse = readText(gfshInstance, CliStrings.CONNECT__KEY_STORE + ": ");
      }
      if (keystoreToUse != null && keystoreToUse.length() > 0) {
        if (keystorePasswordToUse == null || keystorePasswordToUse.length() == 0) {
          // Check whether specified in gfsecurity props earlier
          keystorePasswordToUse = sslConfigProps.get(Gfsh.SSL_KEYSTORE_PASSWORD);
          if (keystorePasswordToUse == null || keystorePasswordToUse.length() == 0) {
            // not even in properties file, prompt user for it
            keystorePasswordToUse = readPassword(gfshInstance, CliStrings.CONNECT__KEY_STORE_PASSWORD + ": ");
            sslConfigProps.put(Gfsh.SSL_KEYSTORE_PASSWORD, keystorePasswordToUse);
          }
        }else{//For cases where password is already part of command option
          sslConfigProps.put(Gfsh.SSL_KEYSTORE_PASSWORD, keystorePasswordToUse);
        }
        sslConfigProps.put(Gfsh.SSL_KEYSTORE, keystoreToUse);
      }

      if (numTimesPrompted > 0) {
        truststoreToUse = readText(gfshInstance, CliStrings.CONNECT__TRUST_STORE + ": ");
      }
      if (truststoreToUse != null && truststoreToUse.length() > 0) {
        if (truststorePasswordToUse == null || truststorePasswordToUse.length() == 0) {
          // Check whether specified in gfsecurity props earlier?
          truststorePasswordToUse = sslConfigProps.get(Gfsh.SSL_TRUSTSTORE_PASSWORD);
          if (truststorePasswordToUse == null || truststorePasswordToUse.length() == 0) {
            // not even in properties file, prompt user for it
            truststorePasswordToUse = readPassword(gfshInstance, CliStrings.CONNECT__TRUST_STORE_PASSWORD + ": ");
            sslConfigProps.put(Gfsh.SSL_TRUSTSTORE_PASSWORD, truststorePasswordToUse);
          }
        }else{//For cases where password is already part of command option
          sslConfigProps.put(Gfsh.SSL_TRUSTSTORE_PASSWORD, truststorePasswordToUse);
        }
        sslConfigProps.put(Gfsh.SSL_TRUSTSTORE, truststoreToUse);
      }

      if (numTimesPrompted > 0) {
        sslCiphersToUse = readText(gfshInstance, CliStrings.CONNECT__SSL_CIPHERS + ": ");
      }
      if (sslCiphersToUse != null && sslCiphersToUse.length() > 0) {            
        //sslConfigProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslCiphersToUse);
        sslConfigProps.put(Gfsh.SSL_ENABLED_CIPHERS, sslCiphersToUse);
      }

      if (numTimesPrompted > 0) {
        sslProtocolsToUse = readText(gfshInstance, CliStrings.CONNECT__SSL_PROTOCOLS + ": ");
      }
      if (sslProtocolsToUse != null && sslProtocolsToUse.length() > 0) {
        //sslConfigProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslProtocolsToUse);
        sslConfigProps.put(Gfsh.SSL_ENABLED_PROTOCOLS, sslProtocolsToUse);
      }

      // SSL is required to be used but no SSL config found
    } while(useSsl && sslConfigProps.isEmpty() && (0 == numTimesPrompted++) && !gfshInstance.isQuietMode());    
    return sslConfigProps;
  }

  private static String getGfshLogsCheckMessage(String logFilePath) {
    return CliStrings.format(CliStrings.GFSH__PLEASE_CHECK_LOGS_AT_0, logFilePath);
  }

  private String readText(Gfsh gfsh, String textToPrompt) throws IOException {
    if (!gfsh.isHeadlessMode() || !gfsh.isQuietMode()) {
      return gfsh.interact(textToPrompt);
    } else {
      return null;
    }
  }

  private String readPassword(Gfsh gfsh, String textToPrompt) throws IOException {
    if (!gfsh.isHeadlessMode() || !gfsh.isQuietMode()) {
      return gfsh.readWithMask(textToPrompt, '*');
    } else {
      return null;
    }
  }

  /* package-private */ static Map<String, String> loadPropertiesFromURL(URL gfSecurityPropertiesUrl) {
    Map<String, String> propsMap = Collections.emptyMap();

    if (gfSecurityPropertiesUrl != null) {
      InputStream inputStream = null;
      try {
        Properties props = new Properties();
        inputStream = gfSecurityPropertiesUrl.openStream();
        props.load(inputStream);
        if (!props.isEmpty()) {
          Set<String> jmxSpecificProps = new HashSet<String>();
          propsMap = new LinkedHashMap<String, String>();
          Set<Entry<Object, Object>> entrySet = props.entrySet();
          for (Entry<Object, Object> entry : entrySet) {

            String key = (String)entry.getKey();
            if (key.endsWith(DistributionConfig.JMX_SSL_PROPS_SUFFIX)) {
              key = key.substring(0, key.length() - DistributionConfig.JMX_SSL_PROPS_SUFFIX.length());
              jmxSpecificProps.add(key);

              propsMap.put(key, (String)entry.getValue());
            } else if (!jmxSpecificProps.contains(key)) {// Prefer properties ending with "-jmx" over default SSL props.
              propsMap.put(key, (String)entry.getValue());
            }
          }
          props.clear();
          jmxSpecificProps.clear();
        }
      } catch (IOException io) {
        throw new RuntimeException(CliStrings.format(
            CliStrings.CONNECT__MSG__COULD_NOT_READ_CONFIG_FROM_0,
                CliUtil.decodeWithDefaultCharSet(gfSecurityPropertiesUrl.getPath())), io);
      } finally {
        IOUtils.close(inputStream);
      }
    }
    return propsMap;
  }

  // Copied from DistributedSystem.java
  public static URL getFileUrl(String fileName) {
    File file = new File(fileName);

    if (file.exists()) {
      try {
        return IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(file).toURI().toURL();
      } catch (MalformedURLException ignore) {
      }
    }

    file = new File(System.getProperty("user.home"), fileName);

    if (file.exists()) {
      try {
        return IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(file).toURI().toURL();
      } catch (MalformedURLException ignore) {
      }
    }

    return ClassPathLoader.getLatest().getResource(ShellCommands.class, fileName);
  }

  public static ConnectToLocatorResult connectToLocator(String host, int port, int timeout, Map<String, String> props)
    throws IOException
  {
    // register DSFID types first; invoked explicitly so that all message type
    // initializations do not happen in first deserialization on a possibly
    // "precious" thread
    DSFIDFactory.registerTypes();
    
    JmxManagerLocatorResponse locatorResponse = JmxManagerLocatorRequest.send(host, port, timeout, props);

    if (StringUtils.isBlank(locatorResponse.getHost()) || locatorResponse.getPort() == 0) {
      Throwable locatorResponseException = locatorResponse.getException();
      String exceptionMessage = CliStrings.CONNECT__MSG__LOCATOR_COULD_NOT_FIND_MANAGER;

      if (locatorResponseException != null) {
        String locatorResponseExceptionMessage = locatorResponseException.getMessage();
        locatorResponseExceptionMessage = (!StringUtils.isBlank(locatorResponseExceptionMessage)
          ? locatorResponseExceptionMessage : locatorResponseException.toString());
        exceptionMessage = "Exception caused JMX Manager startup to fail because: '"
          .concat(locatorResponseExceptionMessage).concat("'");
      }

      throw new IllegalStateException(exceptionMessage, locatorResponseException);
    }

    ConnectionEndpoint memberEndpoint = new ConnectionEndpoint(locatorResponse.getHost(), locatorResponse.getPort());

    String resultMessage = CliStrings.format(CliStrings.CONNECT__MSG__CONNECTING_TO_MANAGER_AT_0,
      memberEndpoint.toString(false));

    return new ConnectToLocatorResult(memberEndpoint, resultMessage, locatorResponse.isJmxManagerSslEnabled());
  }


  @CliCommand(value = { CliStrings.DISCONNECT }, help = CliStrings.DISCONNECT__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEMFIRE_JMX, CliStrings.TOPIC_GEMFIRE_MANAGER})
  public Result disconnect() {
    Result result = null;

    if (getGfsh() != null && !getGfsh().isConnectedAndReady()) {
      result = ResultBuilder.createInfoResult("Not connected.");
    } else {
      InfoResultData infoResultData = ResultBuilder.createInfoResultData();
      try {
        Gfsh gfshInstance = getGfsh();
        if (gfshInstance.isConnectedAndReady()) {
          OperationInvoker operationInvoker = gfshInstance.getOperationInvoker();
          Gfsh.println("Disconnecting from: " + operationInvoker);
          operationInvoker.stop();
          infoResultData.addLine(CliStrings
            .format(CliStrings.DISCONNECT__MSG__DISCONNECTED, operationInvoker.toString()));
          LogWrapper.getInstance().info(CliStrings.format(CliStrings.DISCONNECT__MSG__DISCONNECTED, operationInvoker.toString()));
          gfshInstance.setPromptPath(com.gemstone.gemfire.management.internal.cli.converters.RegionPathConverter.DEFAULT_APP_CONTEXT_PATH);
        } else {
          infoResultData.addLine(CliStrings.DISCONNECT__MSG__NOTCONNECTED);
        }
        result = ResultBuilder.buildResult(infoResultData);
      } catch (Exception e) {
        result = ResultBuilder.createConnectionErrorResult(CliStrings.format(CliStrings.DISCONNECT__MSG__ERROR, e.getMessage()));
      }
    }

    return result;
  }


  @CliCommand(value = {CliStrings.DESCRIBE_CONNECTION}, help = CliStrings.DESCRIBE_CONNECTION__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEMFIRE_JMX})
  public Result describeConnection() {
    Result result = null;
    try {
      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      Gfsh gfshInstance = getGfsh();
      if (gfshInstance.isConnectedAndReady()) {
        OperationInvoker operationInvoker = gfshInstance.getOperationInvoker();
//        tabularResultData.accumulate("Monitored GemFire DS", operationInvoker.toString());
        tabularResultData.accumulate("Connection Endpoints", operationInvoker.toString());
      } else {
        tabularResultData.accumulate("Connection Endpoints", "Not connected");
      }
      result = ResultBuilder.buildResult(tabularResultData);
    } catch (Exception e) {
      ErrorResultData errorResultData =
        ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
          .addLine(e.getMessage());
      result = ResultBuilder.buildResult(errorResultData);
    }

    return result;
  }


  @CliCommand(value = { CliStrings.ECHO }, help = CliStrings.ECHO__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result echo(
      @CliOption(key = {CliStrings.ECHO__STR, ""},
                 unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                 specifiedDefaultValue = "",
                 mandatory = true,
                 help = CliStrings.ECHO__STR__HELP) String stringToEcho) {
    Result result = null;

    if(stringToEcho.equals("$*")){
      Gfsh gfshInstance = getGfsh();
      Map<String, String> envMap = gfshInstance.getEnv();
      Set< Entry<String, String> > setEnvMap = envMap.entrySet();
      TabularResultData  resultData = buildResultForEcho(setEnvMap);

      result = ResultBuilder.buildResult(resultData);
    } else {
      result = ResultBuilder.createInfoResult(stringToEcho);
    }

    return result;
  }
  TabularResultData buildResultForEcho(Set< Entry<String, String> > propertyMap){
    TabularResultData resultData = ResultBuilder.createTabularResultData();
    Iterator <Entry<String, String>> it = propertyMap.iterator();

    while(it.hasNext()){
      Entry<String, String> setEntry = it.next();
      resultData.accumulate("Property", setEntry.getKey());
      resultData.accumulate("Value", String.valueOf(setEntry.getValue()));
    }
    return resultData;
  }



  @CliCommand(value = { CliStrings.SET_VARIABLE }, help = CliStrings.SET_VARIABLE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result setVariable(
      @CliOption(key = CliStrings.SET_VARIABLE__VAR,
                 mandatory=true,
                 help = CliStrings.SET_VARIABLE__VAR__HELP)
                 String var,
      @CliOption(key = CliStrings.SET_VARIABLE__VALUE,
                 mandatory=true,
                 help = CliStrings.SET_VARIABLE__VALUE__HELP)
                 String value) {
    Result result = null;
    try {
      getGfsh().setEnvProperty(var, String.valueOf(value));
      result = ResultBuilder.createInfoResult("Value for variable "+var+" is now: "+value+".");
    } catch (IllegalArgumentException e) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData();
      errorResultData.addLine(e.getMessage());
      result = ResultBuilder.buildResult(errorResultData);
    }

    return result;
  }

//Enable when "use region" command is required. See #46110
//  @CliCommand(value = { CliStrings.USE_REGION }, help = CliStrings.USE_REGION__HELP)
//  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEMFIRE_REGION})
//  public Result useRegion(
//      @CliArgument(name = CliStrings.USE_REGION__REGION,
//                   unspecifiedDefaultValue = "/",
//                   argumentContext = CliStrings.PARAM_CONTEXT_REGIONPATH,
//                   help = CliStrings.USE_REGION__REGION__HELP)
//                    String toRegion) {
//    Gfsh gfsh = Gfsh.getCurrentInstance();
//
//    gfsh.setPromptPath(toRegion);
//    return ResultBuilder.createInfoResult("");
//  }

  @CliCommand(value = { CliStrings.DEBUG }, help = CliStrings.DEBUG__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEMFIRE_DEBUG_UTIL })
  public Result debug(
      @CliOption(key = CliStrings.DEBUG__STATE,
                   unspecifiedDefaultValue = "OFF",
                   mandatory = true,
                   optionContext = "debug",
                   help = CliStrings.DEBUG__STATE__HELP)
                   String state) {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();
    if (gfshInstance != null) {
      // Handle state
      if (state.equalsIgnoreCase("ON")) {
        gfshInstance.setDebug(true);
      } else if(state.equalsIgnoreCase("OFF")){
        gfshInstance.setDebug(false);
      }else{
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.DEBUG__MSG_0_INVALID_STATE_VALUE,state)) ;
      }

    } else {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
              CliStrings.ECHO__MSG__NO_GFSH_INSTANCE);
      return ResultBuilder.buildResult(errorResultData);
    }
    return ResultBuilder.createInfoResult(CliStrings.DEBUG__MSG_DEBUG_STATE_IS + state );
  }

  @CliCommand(value = CliStrings.HISTORY, help = CliStrings.HISTORY__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GFSH })
  public Result history(
      @CliOption(key = { CliStrings.HISTORY__FILE }, unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, help = CliStrings.HISTORY__FILE__HELP)
      String saveHistoryTo,
      @CliOption(key = { CliStrings.HISTORY__CLEAR }, specifiedDefaultValue = "true",
                      unspecifiedDefaultValue = "false", help = CliStrings.HISTORY__CLEAR__HELP) Boolean clearHistory) {

    //process clear history
    if (clearHistory ) {
      return executeClearHistory();
    }else {
      //Process file option
      Gfsh gfsh = Gfsh.getCurrentInstance();
      ErrorResultData errorResultData = null;
      StringBuilder contents = new StringBuilder();
      Writer output = null;

      int historySize = gfsh.getHistorySize();
      String historySizeString = String.valueOf(historySize);
      int historySizeWordLength = historySizeString.length();

      GfshHistory gfshHistory = gfsh.getGfshHistory();
      Iterator<?> it = gfshHistory.entries();
      boolean flagForLineNumbers = (saveHistoryTo != null && saveHistoryTo
          .length() > 0) ? false : true;
      long lineNumber = 0;

      while (it.hasNext()) {
        String line = it.next().toString();
        if (line.isEmpty() == false) {
          if (flagForLineNumbers) {
            lineNumber++;
            contents.append(String.format("%" + historySizeWordLength + "s  ",
                lineNumber));
          }
          contents.append(line);
          contents.append(GfshParser.LINE_SEPARATOR);
        }
      }

      try {
        // write to a user file
        if (saveHistoryTo != null && saveHistoryTo.length() > 0) {
          File saveHistoryToFile = new File(saveHistoryTo);
          output = new BufferedWriter(new FileWriter(saveHistoryToFile));

          if (!saveHistoryToFile.exists()) {
            errorResultData = ResultBuilder.createErrorResultData()
                .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                .addLine(CliStrings.HISTORY__MSG__FILE_DOES_NOT_EXISTS);
            return ResultBuilder.buildResult(errorResultData);
          }
          if (!saveHistoryToFile.isFile()) {
            errorResultData = ResultBuilder.createErrorResultData()
                .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                .addLine(CliStrings.HISTORY__MSG__FILE_SHOULD_NOT_BE_DIRECTORY);
            return ResultBuilder.buildResult(errorResultData);
          }
          if (!saveHistoryToFile.canWrite()) {
            errorResultData = ResultBuilder.createErrorResultData()
                .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                .addLine(CliStrings.HISTORY__MSG__FILE_CANNOT_BE_WRITTEN);
            return ResultBuilder.buildResult(errorResultData);
          }

          output.write(contents.toString());
        }

      } catch (IOException ex) {
        return ResultBuilder.createInfoResult("File error " + ex.getMessage()
            + " for file " + saveHistoryTo);
      } finally {
        try {
          if (output != null) {
            output.close();
          }
        } catch (IOException e) {
          errorResultData = ResultBuilder.createErrorResultData()
              .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
              .addLine("exception in closing file");
          return ResultBuilder.buildResult(errorResultData);
        }
      }
      if (saveHistoryTo != null && saveHistoryTo.length() > 0) {
        // since written to file no need to display the content
        return ResultBuilder.createInfoResult("Wrote successfully to file "
            + saveHistoryTo);
      } else {
        return ResultBuilder.createInfoResult(contents.toString());
      }
    }

  }

  Result executeClearHistory(){
    try{
      Gfsh gfsh = Gfsh.getCurrentInstance();
      gfsh.clearHistory();
    }catch(Exception e){
      LogWrapper.getInstance().info(CliUtil.stackTraceAsString(e) );
      return ResultBuilder.createGemFireErrorResult("Exception occured while clearing history " + e.getMessage());
    }
    return ResultBuilder.createInfoResult(CliStrings.HISTORY__MSG__CLEARED_HISTORY);

  }



  @CliCommand(value = { CliStrings.RUN }, help = CliStrings.RUN__HELP)
  @CliMetaData(shellOnly=true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result executeScript(
      @CliOption(key = CliStrings.RUN__FILE,
                 optionContext = ConverterHint.FILE,
                 mandatory = true,
                 help = CliStrings.RUN__FILE__HELP)
                 File file,
      @CliOption(key = { CliStrings.RUN__QUIET },
                 specifiedDefaultValue = "true",
                 unspecifiedDefaultValue = "false",
                 help = CliStrings.RUN__QUIET__HELP)
                  boolean quiet,
      @CliOption(key = { CliStrings.RUN__CONTINUEONERROR },
                 specifiedDefaultValue = "true",
                 unspecifiedDefaultValue = "false",
                 help = CliStrings.RUN__CONTINUEONERROR__HELP)
                  boolean continueOnError) {
    Result result = null;

    Gfsh gfsh = Gfsh.getCurrentInstance();
    try {
      result = gfsh.executeScript(file, quiet, continueOnError);
    } catch (IllegalArgumentException e) {
      result = ResultBuilder.createShellClientErrorResult(e.getMessage());
    } // let CommandProcessingException go to the caller

    return result;
  }


  @CliCommand(value = CliStrings.ENCRYPT, help = CliStrings.ENCRYPT__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GEMFIRE_DEBUG_UTIL})
  public Result encryptPassword(
      @CliOption(key = CliStrings.ENCRYPT_STRING,
                 help = CliStrings.ENCRYPT_STRING__HELP,
                 mandatory = true)
                 String stringToEncrypt) {
    return ResultBuilder.createInfoResult(PasswordUtil.encrypt(stringToEncrypt, false/*echo*/));
  }

  @CliCommand(value = { CliStrings.VERSION }, help = CliStrings.VERSION__HELP)
  @CliMetaData(shellOnly=true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result version(
      @CliOption(key = { CliStrings.VERSION__FULL },
                 specifiedDefaultValue = "true",
                 unspecifiedDefaultValue = "false",
                 help = CliStrings.VERSION__FULL__HELP)
                  boolean full) {
    Gfsh gfsh = Gfsh.getCurrentInstance();

    return ResultBuilder.createInfoResult(gfsh.getVersion(full));
  }

  @CliCommand(value = { CliStrings.SLEEP }, help = CliStrings.SLEEP__HELP)
  @CliMetaData(shellOnly=true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result sleep(
      @CliOption(key = { CliStrings.SLEEP__TIME },
                 unspecifiedDefaultValue = "3",
                 help = CliStrings.SLEEP__TIME__HELP)
                  double time) {
    try {
      LogWrapper.getInstance().fine("Sleeping for " + time + "seconds.");
      Thread.sleep( Math.round(time * 1000) );
    } catch (InterruptedException ignorable) {}
    return ResultBuilder.createInfoResult("");
  }

  @CliCommand(value = { CliStrings.SH }, help = CliStrings.SH__HELP)
  @CliMetaData(shellOnly=true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result sh(
      @CliArgument(name = CliStrings.SH__COMMAND,
                   mandatory = true,
                   help = CliStrings.SH__COMMAND__HELP)
                    String command,
      @CliOption(key = CliStrings.SH__USE_CONSOLE,
                 specifiedDefaultValue = "true",
                 unspecifiedDefaultValue = "false",
                 help = CliStrings.SH__USE_CONSOLE__HELP)
                  boolean useConsole) {
    Result result = null;
    try {
      result = ResultBuilder.buildResult(executeCommand(Gfsh.getCurrentInstance(), command, useConsole));
    } catch (IllegalStateException e) {
      result = ResultBuilder.createUserErrorResult(e.getMessage());
      LogWrapper.getInstance().warning("Unable to execute command \"" + command + "\". Reason:" + e.getMessage() + ".");
    } catch (IOException e) {
      result = ResultBuilder.createUserErrorResult(e.getMessage());
      LogWrapper.getInstance().warning("Unable to execute command \"" + command + "\". Reason:" + e.getMessage() + ".");
    }
    return result;
  }

  private static InfoResultData executeCommand(Gfsh gfsh, String userCommand, boolean useConsole) throws IOException {
    InfoResultData infoResultData = ResultBuilder.createInfoResultData();

    String cmdToExecute   = userCommand;
    String cmdExecutor    = "/bin/sh";
    String cmdExecutorOpt = "-c";
    if (SystemUtils.isWindows()) {
      cmdExecutor    = "cmd";
      cmdExecutorOpt = "/c";
    } else if (useConsole) {
      cmdToExecute = cmdToExecute + " </dev/tty >/dev/tty";
    }
    String[] commandArray = { cmdExecutor, cmdExecutorOpt, cmdToExecute };

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(commandArray);
    builder.directory();
    builder.redirectErrorStream();
    Process proc = builder.start();

    BufferedReader input = new BufferedReader(new InputStreamReader(proc.getInputStream()));

    String lineRead = "";
    while ((lineRead = input.readLine()) != null) {
      infoResultData.addLine(lineRead);
    }

    proc.getOutputStream().close();

    try {
      if (proc.waitFor() != 0) {
        gfsh.logWarning("The command '" + userCommand + "' did not complete successfully", null);
      }
    } catch (final InterruptedException e) {
      throw new IllegalStateException(e);
    }
    return infoResultData;
  }


  @CliAvailabilityIndicator({CliStrings.CONNECT, CliStrings.DISCONNECT, CliStrings.DESCRIBE_CONNECTION})
  public boolean isAvailable() {
    return true;
  }
}
