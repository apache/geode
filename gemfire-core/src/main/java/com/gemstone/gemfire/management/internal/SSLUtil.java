package com.gemstone.gemfire.management.internal;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.net.ssl.SSLContext;

import com.gemstone.gemfire.internal.lang.StringUtils;

/**
 * 
 * @author rishim
 * @since  8.1
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
