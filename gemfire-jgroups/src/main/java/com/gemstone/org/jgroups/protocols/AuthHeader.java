/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.protocols;

import java.io.*;
import java.util.Properties;

import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Streamable;

/**
 * 
 * AuthHeader stores credentials of a joiner vm to pass it to the coordinator
 * 
 * @author Yogesh Mahajan
 * @since 5.5
 * 
 */
public class AuthHeader extends Header implements Streamable {

  private static final long serialVersionUID = 1L;

  private byte[] credentials = null;

  public static final String USER_NAME = "security-username";

  public static final String PASSWORD = "security-password";
  
//  public static int MAX_CREDENTIAL_SIZE = 50000;

  public void readFrom(DataInputStream in) throws IOException,
      IllegalAccessException, InstantiationException {
    try {
      this.credentials = JChannel.getGfFunctions().readByteArray(in);
    }
    catch (Exception ex) {
      this.credentials = new byte[1];
    }
  }

  public void writeTo(DataOutputStream out) throws IOException {
    if (this.credentials == null) {
      this.credentials = new byte[1];
    }
    JChannel.getGfFunctions().writeByteArray(this.credentials, out);
  }

  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    this.credentials = JChannel.getGfFunctions().readByteArray(in);
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    JChannel.getGfFunctions().writeByteArray(this.credentials, out);
  }

  public void setCredentials(Properties credentials) {
    try {
      ByteArrayOutputStream bas = new ByteArrayOutputStream(10000);
      ObjectOutputStream oos = new ObjectOutputStream(bas);
      JChannel.getGfFunctions().writeProperties(credentials, oos);
//      if( bas.size() > MAX_CREDENTIAL_SIZE) {
//          throw new IllegalArgumentException(
//            JGroupsStrings.AuthHeader_SERIALIZED_CREDENTIAL_SIZE_0_EXCEEDS_MAXIMUM_OF_1.toLocalizedString(new Object[] {Integer.valueOf(bas.size()), Integer.valueOf(MAX_CREDENTIAL_SIZE) }));
//      }
      oos.flush();
      this.credentials = bas.toByteArray();
      
    }
    catch (IOException e) {
        // ignore - this will happen again when the view is serialized
        // for transmission
    }
  }

  public Properties getCredentials() {
    Properties retProp = new Properties();
    try {
      ByteArrayInputStream bas = new ByteArrayInputStream(credentials);
      ObjectInputStream ois = new ObjectInputStream(bas);
      retProp = JChannel.getGfFunctions().readProperties(ois);
    }
    catch (ClassNotFoundException e) {
    	
    }
    catch (IOException e) {
      // ignore - instead return properties with zero size
      // security properties are optional, if expected in
      // the other end anyway connectivity will fail.
    }
    return retProp;
  }

  @Override // GemStoneAddition
  public long size(short version) {
      return this.credentials==null? 0 : this.credentials.length;
  }

  @Override // GemStoneAddition
  public String toString() {
    return "[AuthHeader : " + getCredentials() + "]";
  }

}
