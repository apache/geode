/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.util;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.geode.annotations.Immutable;

/**
 * Makes use of Blowfish algorithm to decrypt a pre-encrypted password string. As of June 2017, no
 * longer supports encrypting a password. However, decrypting still works.
 */
public class PasswordUtil {

  @Immutable
  private static final byte[] init = "string".getBytes();

  /**
   * Decrypts an encrypted password string.
   *
   * @param password String to be decrypted (format: `encrypted(password_to_decrypt)`)
   * @return String decrypted String
   */
  @Deprecated
  public static String decrypt(String password) {
    if (!isEncryptedPassword(password)) {
      return password;
    }

    String toDecrypt = password.substring(10, password.length() - 1);
    SecretKeySpec key = new SecretKeySpec(init, "Blowfish");
    try {
      Cipher cipher = Cipher.getInstance("Blowfish");
      cipher.init(Cipher.DECRYPT_MODE, key);
      byte[] decrypted = cipher.doFinal(hexStringToByteArray(toDecrypt));

      return new String(decrypted);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isEncryptedPassword(String password) {
    if (password == null) {
      return false;
    }
    return password.startsWith("encrypted(") && password.endsWith(")");
  }

  private static byte[] hexStringToByteArray(String s) {
    byte[] b = new byte[s.length() / 2];
    for (int i = 0; i < b.length; i++) {
      int index = i * 2;
      int v = Integer.parseInt(s.substring(index, index + 2), 16);
      b[i] = (byte) (v & 0xff);
    }
    return b;
  }
}
