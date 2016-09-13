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
package org.apache.geode.management.internal;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.net.ssl.SSLContext;

import org.apache.geode.internal.lang.StringUtils;

/**
 * 
 * @since GemFire  8.1
 */
public class SSLUtil {
  
  public static String getSSLAlgo(String[] protocols) {
    String c = null;

    if (protocols != null && protocols.length > 0) {
      for (String protocol : protocols) {
        if (!protocol.equals("any")) {
          try {
            SSLContext.getInstance(protocol);
            c = protocol;
            break;
          } catch (NoSuchAlgorithmException e) {
            // continue
          }
        }
      }
    }
    if (c != null) {
      return c;
    }
    // lookup known algorithms
    String[] knownAlgorithms = { "SSL", "SSLv2", "SSLv3", "TLS", "TLSv1", "TLSv1.1", "TLSv1.2" };
    for (String algo : knownAlgorithms) {
      try {
        SSLContext.getInstance(algo);
        c = algo;
        break;
      } catch (NoSuchAlgorithmException e) {
        // continue
      }
    }
    return c;
  }
  
  /** Read an array of values from a string, whitespace separated. */
  public static String[] readArray( String text ) {
    if (StringUtils.isBlank(text)) {
      return null;
    }
    
    StringTokenizer st = new StringTokenizer( text );
    List<String> v = new ArrayList<String>( );
    while( st.hasMoreTokens() ) {
      v.add( st.nextToken() );
    }
    return v.toArray( new String[ v.size() ] );
  }

}
