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
package com.gemstone.gemfire.internal.util;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * Generates an encrypted password, used by the gemfire encrypt-password
 * command. Makes use of Blowfish algorithm to encrypt/decrypt password string
 * 
 * <p>
 * This shows a sample command invocation and output (assuming password is the
 * actual password for the datasource): <br>
 * <br>
 * bash-2.05$ $GEMFIRE/bin/gemfire encrypt-password password<br>
 * Using system directory "/home/users/jpearson/gemfire/defaultSystem".<br>
 * Encrypted to 83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a <br>
 * <br>
 * Copy the output from the gemfire command to the cache.xml file as the value
 * of the password attribute of the jndi-binding tag embedded in encrypted(),
 * just like a method parameter.<br>
 * Enter it as encrypted, in this format:
 * password="encrypted(83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a)"<br>
 * To use a non-encrypted password, put the actual password as the value of the
 * password attribute of the jndi-binding tag, like this: password="password"
 * <br>
 * 
 */
public class PasswordUtil {

  private static byte[] init = "string".getBytes();

  /**
   * Encrypts a password string
   * 
   * @param password
   *          String to be encrypted.
   * @return String encrypted String
   */
  public static String encrypt(String password) {
    return encrypt(password, true);
  }

  /**
   * 
   * @param password String to be encrypted
   * @param echo if true prints result to system.out
   * @return String encrypted String
   */
  public static String encrypt(String password, boolean echo) {
    String encryptedString = null;
    try {
      SecretKeySpec key = new SecretKeySpec(init, "Blowfish");
      Cipher cipher = Cipher.getInstance("Blowfish");
      cipher.init(Cipher.ENCRYPT_MODE, key);
      byte[] encrypted = cipher.doFinal(password.getBytes());
      encryptedString = byteArrayToHexString(encrypted);
      if (echo) {
        System.out.println(LocalizedStrings.PasswordUtil_ENCRYPTED_TO_0
            .toLocalizedString(encryptedString));
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    return encryptedString;
  }
  /**
   * Decrypts an encrypted password string.
   * 
   * @param password
   *          String to be decrypted
   * @return String decrypted String
   */
  public static String decrypt(String password) {
    if (password.startsWith("encrypted(") && password.endsWith(")")) {
      byte[] decrypted = null;
      try {
        String toDecrypt = password.substring(10, password.length() - 1);
        SecretKeySpec key = new SecretKeySpec(init, "Blowfish");
        Cipher cipher = Cipher.getInstance("Blowfish");
        cipher.init(Cipher.DECRYPT_MODE, key);
        decrypted = cipher.doFinal(hexStringToByteArray(toDecrypt));
        return new String(decrypted);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }
    return password;
  }

  private static String byteArrayToHexString(byte[] b) {
    StringBuilder sb = new StringBuilder(b.length * 2);
    for (int i = 0; i < b.length; i++) {
      int v = b[i] & 0xff;
      if (v < 16) {
        sb.append('0');
      }
      sb.append(Integer.toHexString(v));
    }
    return sb.toString().toUpperCase();
  }

  private static byte[] hexStringToByteArray(String s) {
    byte[] b = new byte[s.length() / 2];
    for (int i = 0; i < b.length; i++) {
      int index = i * 2;
      int v = Integer.parseInt(s.substring(index, index + 2), 16);
      b[i] = (byte)v;
    }
    return b;
  }
}
