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
package org.apache.geode.internal.serialization.filter;

import java.util.StringJoiner;

/**
 * This list contains classes that Geode's classes subclass, such as antlr AST classes which are
 * used by our Object Query Language. It also contains certain classes that are DataSerializable
 * but end up being serialized as part of other serializable objects. VersionedObjectList, for
 * instance, is serialized as part of a partial putAll exception object.
 *
 * <p>
 * Do not java-serialize objects that Geode does not have complete control over. This leaves us
 * open to security attacks such as Gadget Chains and compromises the ability to do a rolling
 * upgrade from one version of Geode to the next.
 *
 * <p>
 * In general, you shouldn't use java serialization, and you should implement
 * DataSerializableFixedID for internal Geode objects. This gives you better control over
 * backward-compatibility.
 *
 * <p>
 * Do not add to this list unless absolutely necessary. Instead, put your classes either in the
 * sanctionedSerializables file for your module or in its excludedClasses file. Run
 * AnalyzeSerializables to generate the content for the file.
 */
public class SanctionedSerializablesFilterPattern implements FilterPattern {

  private final StringJoiner stringJoiner = dependenciesPattern();

  @Override
  public String pattern() {
    return new StringJoiner(";")
        .merge(stringJoiner)

        // reject all others
        .add("!*")

        .toString();
  }

  /**
   * Used to append the user specified serializable-object-filter
   */
  public SanctionedSerializablesFilterPattern append(CharSequence chars) {
    stringJoiner.add(chars);
    return this;
  }

  private static StringJoiner dependenciesPattern() {
    return new StringJoiner(";")

        // accept all open MBean data types
        .add("java.**")
        .add("javax.management.**")

        // used for some old enums
        .add("javax.print.attribute.EnumSyntax")

        // query AST objects
        .add("antlr.**")

        // old Admin API
        .add("org.apache.commons.modeler.AttributeInfo")
        .add("org.apache.commons.modeler.FeatureInfo")
        .add("org.apache.commons.modeler.ManagedBean")
        .add("org.apache.geode.distributed.internal.DistributionConfigSnapshot")
        .add("org.apache.geode.distributed.internal.RuntimeDistributionConfigImpl")
        .add("org.apache.geode.distributed.internal.DistributionConfigImpl")

        // WindowedExportFunction, RegionSnapshotService
        .add("org.apache.geode.distributed.internal.membership.InternalDistributedMember")

        // putAll
        .add("org.apache.geode.internal.cache.persistence.PersistentMemberID")
        .add("org.apache.geode.internal.cache.persistence.DiskStoreID")
        .add("org.apache.geode.internal.cache.tier.sockets.VersionedObjectList")

        // security services
        .add("org.apache.shiro.**")

        // export logs
        .add("org.apache.logging.log4j.Level")
        .add("org.apache.logging.log4j.spi.StandardLevel")

        // jar deployment
        .add("com.sun.proxy.$Proxy*")
        .add("com.healthmarketscience.rmiio.RemoteInputStream")
        .add("javax.rmi.ssl.SslRMIClientSocketFactory")
        .add("javax.net.ssl.SSLHandshakeException")
        .add("javax.net.ssl.SSLException")
        .add("sun.security.validator.ValidatorException")
        .add("sun.security.provider.certpath.SunCertPathBuilderException")

        // geode-modules
        .add("org.apache.geode.modules.util.SessionCustomExpiry");
  }
}
