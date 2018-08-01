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

package org.apache.geode.cache.client.internal.provider;

import java.security.Provider;
import java.util.logging.Logger;


public final class CustomProvider extends Provider {
  private static final long serialVersionUID = -2667509590306131953L;

  private final Logger logger = Logger.getLogger(this.getClass().getName());

  public CustomProvider() {
    super("Custom Provider", 1.0,
        "KeyManagerFactory and TrustManagerFactory based on a custom factory implementation");

    this.logger.fine("KeyManager enabled");

    put("KeyManagerFactory.SunX509",
        "org.apache.geode.cache.client.internal.provider.CustomKeyManagerFactory$SimpleFactory");
    put("KeyManagerFactory.PKIX",
        "org.apache.geode.cache.client.internal.provider.CustomKeyManagerFactory$PKIXFactory");
    put("Alg.Alias.KeyManagerFactory.SunPKIX", "PKIX");
    put("Alg.Alias.KeyManagerFactory.X509", "PKIX");
    put("Alg.Alias.KeyManagerFactory.X.509", "PKIX");

    this.logger.fine("TrustManager enabled");

    put("TrustManagerFactory.SunX509",
        "org.apache.geode.cache.client.internal.provider.CustomTrustManagerFactory$SimpleFactory");
    put("TrustManagerFactory.PKIX",
        "org.apache.geode.cache.client.internal.provider.CustomTrustManagerFactory$PKIXFactory");
    put("Alg.Alias.TrustManagerFactory.SunPKIX", "PKIX");
    put("Alg.Alias.TrustManagerFactory.X509", "PKIX");
    put("Alg.Alias.TrustManagerFactory.X.509", "PKIX");

    this.logger.fine("Provider loaded");
  }
}
