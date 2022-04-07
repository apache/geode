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
package org.apache.geode.internal;

import static java.util.Collections.emptySet;
import static org.apache.geode.distributed.internal.DistributionConfig.VALIDATE_SERIALIZABLE_OBJECTS_NAME;
import static org.apache.geode.internal.lang.ClassUtils.isClassAvailable;
import static org.apache.geode.internal.serialization.Version.CURRENT;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.shiro.ShiroException;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.codec.CodecException;
import org.apache.shiro.config.ConfigurationException;
import org.apache.shiro.crypto.UnknownAlgorithmException;
import org.apache.shiro.dao.InvalidResourceUsageException;
import org.apache.shiro.env.RequiredTypeException;
import org.apache.shiro.io.SerializationException;
import org.apache.shiro.ldap.UnsupportedAuthenticationMechanismException;
import org.apache.shiro.session.SessionException;
import org.apache.shiro.session.StoppedSessionException;
import org.apache.shiro.subject.ExecutionException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SecurityTest.class, SerializationTest.class})
public class InternalDataSerializerShiroAcceptListTest {

  @BeforeClass
  public static void hasObjectInputFilter() {
    assumeTrue("ObjectInputFilter is present in this JVM",
        isClassAvailable("sun.misc.ObjectInputFilter") ||
            isClassAvailable("java.io.ObjectInputFilter"));
  }

  @After
  public void clearSerializationFilter() {
    InternalDataSerializer.clearSerializationFilter();
  }

  @Test
  public void acceptsAuthenticationException() throws IOException, ClassNotFoundException {
    trySerializingObject(new AuthenticationException("testing"), propertiesWithoutFilter());
  }

  @Test
  public void acceptsAuthorizationException() throws IOException, ClassNotFoundException {
    trySerializingObject(new AuthorizationException("testing"), propertiesWithoutFilter());
  }

  @Test
  public void acceptsCodecException() throws IOException, ClassNotFoundException {
    trySerializingObject(new CodecException("testing"), propertiesWithoutFilter());
  }

  @Test
  public void acceptsConfigurationException() throws IOException, ClassNotFoundException {
    trySerializingObject(new ConfigurationException("testing"), propertiesWithoutFilter());
  }

  @Test
  public void acceptsExecutionException() throws IOException, ClassNotFoundException {
    trySerializingObject(new ExecutionException("testing", new Exception("testing")),
        propertiesWithoutFilter());
  }

  @Test
  public void acceptsInstantiationException() throws IOException, ClassNotFoundException {
    trySerializingObject(new org.apache.shiro.util.InstantiationException("testing"),
        propertiesWithoutFilter());
  }

  @Test
  public void acceptsInvalidResourceUsageException() throws IOException, ClassNotFoundException {
    trySerializingObject(new InvalidResourceUsageException("testing"), propertiesWithoutFilter());
  }

  @Test
  public void acceptsRequiredTypeException() throws IOException, ClassNotFoundException {
    trySerializingObject(new RequiredTypeException("testing"), propertiesWithoutFilter());
  }

  @Test
  public void acceptsSerializationException() throws IOException, ClassNotFoundException {
    trySerializingObject(new SerializationException("testing"), propertiesWithoutFilter());
  }

  @Test
  public void acceptsSessionException() throws IOException, ClassNotFoundException {
    trySerializingObject(new SessionException("testing"), propertiesWithoutFilter());
  }

  @Test
  public void acceptsShiroException() throws IOException, ClassNotFoundException {
    trySerializingObject(new ShiroException("testing"), propertiesWithoutFilter());
  }

  @Test
  public void acceptsStoppedSessionException() throws IOException, ClassNotFoundException {
    trySerializingObject(new StoppedSessionException("testing"), propertiesWithoutFilter());
  }

  @Test
  public void acceptsUnknownAlgorithmException() throws IOException, ClassNotFoundException {
    trySerializingObject(new UnknownAlgorithmException("testing"), propertiesWithoutFilter());
  }

  @Test
  public void acceptsUnsupportedAuthenticationMechanismException()
      throws IOException, ClassNotFoundException {
    trySerializingObject(new UnsupportedAuthenticationMechanismException("testing"),
        propertiesWithoutFilter());
  }

  private static Properties propertiesWithoutFilter() {
    Properties properties = new Properties();
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");

    return properties;
  }

  private static void trySerializingObject(Object object, Properties properties)
      throws IOException, ClassNotFoundException {
    DistributionConfig distributionConfig = new DistributionConfigImpl(properties);
    InternalDataSerializer.initializeSerializationFilter(distributionConfig, emptySet());
    HeapDataOutputStream outputStream = new HeapDataOutputStream(CURRENT);

    DataSerializer.writeObject(object, outputStream);

    try (ByteArrayInputStream bais = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dis = new DataInputStream(bais)) {
      DataSerializer.readObject(dis);
    }
  }
}
