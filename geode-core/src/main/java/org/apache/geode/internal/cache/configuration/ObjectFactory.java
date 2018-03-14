
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.internal.cache.configuration;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each
 * Java content interface and Java element interface
 * generated in the org.apache.geode.cache.domain.internal package.
 * <p>
 * An ObjectFactory allows you to programatically
 * construct new instances of the Java representation
 * for XML content. The Java representation of XML
 * content can consist of schema derived interfaces
 * and classes representing the binding of schema
 * type definitions, element declarations and model
 * groups. Factory methods for each of these are
 * provided in this class.
 *
 */
@XmlRegistry
public class ObjectFactory {

  private static final QName _CacheJndiBindings_QNAME =
      new QName("http://geode.apache.org/schema/cache", "jndi-bindings");
  private static final QName _CacheVmRootRegion_QNAME =
      new QName("http://geode.apache.org/schema/cache", "vm-root-region");
  private static final QName _CacheRegion_QNAME =
      new QName("http://geode.apache.org/schema/cache", "region");

  /**
   * Create a new ObjectFactory that can be used to create new instances of schema derived classes
   * for package: org.apache.geode.cache.domain.internal
   *
   */
  public ObjectFactory() {}

  /**
   * Create an instance of {@link CacheConfig }
   *
   */
  public CacheConfig createCache() {
    return new CacheConfig();
  }

  /**
   * Create an instance of {@link ExpirationAttributesType }
   *
   */
  public ExpirationAttributesType createExpirationAttributesType() {
    return new ExpirationAttributesType();
  }

  /**
   * Create an instance of {@link ServerType }
   *
   */
  public ServerType createServerType() {
    return new ServerType();
  }

  /**
   * Create an instance of {@link SerializationRegistrationType }
   *
   */
  public SerializationRegistrationType createSerializationRegistrationType() {
    return new SerializationRegistrationType();
  }

  /**
   * Create an instance of {@link FunctionServiceType }
   *
   */
  public FunctionServiceType createFunctionServiceType() {
    return new FunctionServiceType();
  }

  /**
   * Create an instance of {@link RegionConfig }
   *
   */
  public RegionConfig createRegionType() {
    return new RegionConfig();
  }

  /**
   * Create an instance of {@link RegionConfig.Entry }
   *
   */
  public RegionConfig.Entry createRegionTypeEntry() {
    return new RegionConfig.Entry();
  }

  /**
   * Create an instance of {@link RegionConfig.Index }
   *
   */
  public RegionConfig.Index createRegionTypeIndex() {
    return new RegionConfig.Index();
  }

  /**
   * Create an instance of {@link JndiBindingsType }
   *
   */
  public JndiBindingsType createJndiBindingsType() {
    return new JndiBindingsType();
  }

  /**
   * Create an instance of {@link JndiBindingsType.JndiBinding }
   *
   */
  public JndiBindingsType.JndiBinding createJndiBindingsTypeJndiBinding() {
    return new JndiBindingsType.JndiBinding();
  }

  /**
   * Create an instance of {@link RegionAttributesType }
   *
   */
  public RegionAttributesType createRegionAttributesType() {
    return new RegionAttributesType();
  }

  /**
   * Create an instance of {@link RegionAttributesType.EvictionAttributes }
   *
   */
  public RegionAttributesType.EvictionAttributes createRegionAttributesTypeEvictionAttributes() {
    return new RegionAttributesType.EvictionAttributes();
  }

  /**
   * Create an instance of {@link RegionAttributesType.MembershipAttributes }
   *
   */
  public RegionAttributesType.MembershipAttributes createRegionAttributesTypeMembershipAttributes() {
    return new RegionAttributesType.MembershipAttributes();
  }

  /**
   * Create an instance of {@link RegionAttributesType.PartitionAttributes }
   *
   */
  public RegionAttributesType.PartitionAttributes createRegionAttributesTypePartitionAttributes() {
    return new RegionAttributesType.PartitionAttributes();
  }

  /**
   * Create an instance of {@link RegionAttributesType.DiskWriteAttributes }
   *
   */
  public RegionAttributesType.DiskWriteAttributes createRegionAttributesTypeDiskWriteAttributes() {
    return new RegionAttributesType.DiskWriteAttributes();
  }

  /**
   * Create an instance of {@link PdxType }
   *
   */
  public PdxType createPdxType() {
    return new PdxType();
  }

  /**
   * Create an instance of {@link PoolType }
   *
   */
  public PoolType createPoolType() {
    return new PoolType();
  }

  /**
   * Create an instance of {@link CacheConfig.GatewayHub }
   *
   */
  public CacheConfig.GatewayHub createCacheGatewayHub() {
    return new CacheConfig.GatewayHub();
  }

  /**
   * Create an instance of {@link CacheConfig.GatewayHub.Gateway }
   *
   */
  public CacheConfig.GatewayHub.Gateway createCacheGatewayHubGateway() {
    return new CacheConfig.GatewayHub.Gateway();
  }

  /**
   * Create an instance of {@link CacheTransactionManagerType }
   *
   */
  public CacheTransactionManagerType createCacheTransactionManagerType() {
    return new CacheTransactionManagerType();
  }

  /**
   * Create an instance of {@link DynamicRegionFactoryType }
   *
   */
  public DynamicRegionFactoryType createDynamicRegionFactoryType() {
    return new DynamicRegionFactoryType();
  }

  /**
   * Create an instance of {@link CacheConfig.GatewaySender }
   *
   */
  public CacheConfig.GatewaySender createCacheGatewaySender() {
    return new CacheConfig.GatewaySender();
  }

  /**
   * Create an instance of {@link CacheConfig.GatewayReceiver }
   *
   */
  public CacheConfig.GatewayReceiver createCacheGatewayReceiver() {
    return new CacheConfig.GatewayReceiver();
  }

  /**
   * Create an instance of {@link CacheConfig.GatewayConflictResolver }
   *
   */
  public CacheConfig.GatewayConflictResolver createCacheGatewayConflictResolver() {
    return new CacheConfig.GatewayConflictResolver();
  }

  /**
   * Create an instance of {@link CacheConfig.AsyncEventQueue }
   *
   */
  public CacheConfig.AsyncEventQueue createCacheAsyncEventQueue() {
    return new CacheConfig.AsyncEventQueue();
  }

  /**
   * Create an instance of {@link CacheConfig.CacheServer }
   *
   */
  public CacheConfig.CacheServer createCacheCacheServer() {
    return new CacheConfig.CacheServer();
  }

  /**
   * Create an instance of {@link DiskStoreType }
   *
   */
  public DiskStoreType createDiskStoreType() {
    return new DiskStoreType();
  }

  /**
   * Create an instance of {@link ResourceManagerType }
   *
   */
  public ResourceManagerType createResourceManagerType() {
    return new ResourceManagerType();
  }

  /**
   * Create an instance of {@link InitializerType }
   *
   */
  public InitializerType createInitializerType() {
    return new InitializerType();
  }

  /**
   * Create an instance of {@link CacheWriterType }
   *
   */
  public CacheWriterType createCacheWriterType() {
    return new CacheWriterType();
  }

  /**
   * Create an instance of {@link DiskDirsType }
   *
   */
  public DiskDirsType createDiskDirsType() {
    return new DiskDirsType();
  }

  /**
   * Create an instance of {@link StringType }
   *
   */
  public StringType createStringType() {
    return new StringType();
  }

  /**
   * Create an instance of {@link ParameterType }
   *
   */
  public ParameterType createParameterType() {
    return new ParameterType();
  }

  /**
   * Create an instance of {@link ClassWithParametersType }
   *
   */
  public ClassWithParametersType createClassWithParametersType() {
    return new ClassWithParametersType();
  }

  /**
   * Create an instance of {@link DeclarableType }
   *
   */
  public DeclarableType createDeclarableType() {
    return new DeclarableType();
  }

  /**
   * Create an instance of {@link CacheLoaderType }
   *
   */
  public CacheLoaderType createCacheLoaderType() {
    return new CacheLoaderType();
  }

  /**
   * Create an instance of {@link DiskDirType }
   *
   */
  public DiskDirType createDiskDirType() {
    return new DiskDirType();
  }

  /**
   * Create an instance of {@link ExpirationAttributesType.CustomExpiry }
   *
   */
  public ExpirationAttributesType.CustomExpiry createExpirationAttributesTypeCustomExpiry() {
    return new ExpirationAttributesType.CustomExpiry();
  }

  /**
   * Create an instance of {@link ServerType.ClientSubscription }
   *
   */
  public ServerType.ClientSubscription createServerTypeClientSubscription() {
    return new ServerType.ClientSubscription();
  }

  /**
   * Create an instance of {@link ServerType.CustomLoadProbe }
   *
   */
  public ServerType.CustomLoadProbe createServerTypeCustomLoadProbe() {
    return new ServerType.CustomLoadProbe();
  }

  /**
   * Create an instance of {@link SerializationRegistrationType.Serializer }
   *
   */
  public SerializationRegistrationType.Serializer createSerializationRegistrationTypeSerializer() {
    return new SerializationRegistrationType.Serializer();
  }

  /**
   * Create an instance of {@link SerializationRegistrationType.Instantiator }
   *
   */
  public SerializationRegistrationType.Instantiator createSerializationRegistrationTypeInstantiator() {
    return new SerializationRegistrationType.Instantiator();
  }

  /**
   * Create an instance of {@link FunctionServiceType.Function }
   *
   */
  public FunctionServiceType.Function createFunctionServiceTypeFunction() {
    return new FunctionServiceType.Function();
  }

  /**
   * Create an instance of {@link RegionConfig.Entry.Key }
   *
   */
  public RegionConfig.Entry.Key createRegionTypeEntryKey() {
    return new RegionConfig.Entry.Key();
  }

  /**
   * Create an instance of {@link RegionConfig.Entry.Value }
   *
   */
  public RegionConfig.Entry.Value createRegionTypeEntryValue() {
    return new RegionConfig.Entry.Value();
  }

  /**
   * Create an instance of {@link RegionConfig.Index.Functional }
   *
   */
  public RegionConfig.Index.Functional createRegionTypeIndexFunctional() {
    return new RegionConfig.Index.Functional();
  }

  /**
   * Create an instance of {@link RegionConfig.Index.PrimaryKey }
   *
   */
  public RegionConfig.Index.PrimaryKey createRegionTypeIndexPrimaryKey() {
    return new RegionConfig.Index.PrimaryKey();
  }

  /**
   * Create an instance of {@link JndiBindingsType.JndiBinding.ConfigProperty }
   *
   */
  public JndiBindingsType.JndiBinding.ConfigProperty createJndiBindingsTypeJndiBindingConfigProperty() {
    return new JndiBindingsType.JndiBinding.ConfigProperty();
  }

  /**
   * Create an instance of {@link RegionAttributesType.RegionTimeToLive }
   *
   */
  public RegionAttributesType.RegionTimeToLive createRegionAttributesTypeRegionTimeToLive() {
    return new RegionAttributesType.RegionTimeToLive();
  }

  /**
   * Create an instance of {@link RegionAttributesType.RegionIdleTime }
   *
   */
  public RegionAttributesType.RegionIdleTime createRegionAttributesTypeRegionIdleTime() {
    return new RegionAttributesType.RegionIdleTime();
  }

  /**
   * Create an instance of {@link RegionAttributesType.EntryTimeToLive }
   *
   */
  public RegionAttributesType.EntryTimeToLive createRegionAttributesTypeEntryTimeToLive() {
    return new RegionAttributesType.EntryTimeToLive();
  }

  /**
   * Create an instance of {@link RegionAttributesType.EntryIdleTime }
   *
   */
  public RegionAttributesType.EntryIdleTime createRegionAttributesTypeEntryIdleTime() {
    return new RegionAttributesType.EntryIdleTime();
  }

  /**
   * Create an instance of {@link RegionAttributesType.SubscriptionAttributes }
   *
   */
  public RegionAttributesType.SubscriptionAttributes createRegionAttributesTypeSubscriptionAttributes() {
    return new RegionAttributesType.SubscriptionAttributes();
  }

  /**
   * Create an instance of {@link RegionAttributesType.CacheListener }
   *
   */
  public RegionAttributesType.CacheListener createRegionAttributesTypeCacheListener() {
    return new RegionAttributesType.CacheListener();
  }

  /**
   * Create an instance of {@link RegionAttributesType.Compressor }
   *
   */
  public RegionAttributesType.Compressor createRegionAttributesTypeCompressor() {
    return new RegionAttributesType.Compressor();
  }

  /**
   * Create an instance of {@link RegionAttributesType.EvictionAttributes.LruEntryCount }
   *
   */
  public RegionAttributesType.EvictionAttributes.LruEntryCount createRegionAttributesTypeEvictionAttributesLruEntryCount() {
    return new RegionAttributesType.EvictionAttributes.LruEntryCount();
  }

  /**
   * Create an instance of {@link RegionAttributesType.EvictionAttributes.LruHeapPercentage }
   *
   */
  public RegionAttributesType.EvictionAttributes.LruHeapPercentage createRegionAttributesTypeEvictionAttributesLruHeapPercentage() {
    return new RegionAttributesType.EvictionAttributes.LruHeapPercentage();
  }

  /**
   * Create an instance of {@link RegionAttributesType.EvictionAttributes.LruMemorySize }
   *
   */
  public RegionAttributesType.EvictionAttributes.LruMemorySize createRegionAttributesTypeEvictionAttributesLruMemorySize() {
    return new RegionAttributesType.EvictionAttributes.LruMemorySize();
  }

  /**
   * Create an instance of {@link RegionAttributesType.MembershipAttributes.RequiredRole }
   *
   */
  public RegionAttributesType.MembershipAttributes.RequiredRole createRegionAttributesTypeMembershipAttributesRequiredRole() {
    return new RegionAttributesType.MembershipAttributes.RequiredRole();
  }

  /**
   * Create an instance of {@link RegionAttributesType.PartitionAttributes.PartitionResolver }
   *
   */
  public RegionAttributesType.PartitionAttributes.PartitionResolver createRegionAttributesTypePartitionAttributesPartitionResolver() {
    return new RegionAttributesType.PartitionAttributes.PartitionResolver();
  }

  /**
   * Create an instance of {@link RegionAttributesType.PartitionAttributes.PartitionListener }
   *
   */
  public RegionAttributesType.PartitionAttributes.PartitionListener createRegionAttributesTypePartitionAttributesPartitionListener() {
    return new RegionAttributesType.PartitionAttributes.PartitionListener();
  }

  /**
   * Create an instance of
   * {@link RegionAttributesType.PartitionAttributes.FixedPartitionAttributes }
   *
   */
  public RegionAttributesType.PartitionAttributes.FixedPartitionAttributes createRegionAttributesTypePartitionAttributesFixedPartitionAttributes() {
    return new RegionAttributesType.PartitionAttributes.FixedPartitionAttributes();
  }

  /**
   * Create an instance of {@link RegionAttributesType.DiskWriteAttributes.AsynchronousWrites }
   *
   */
  public RegionAttributesType.DiskWriteAttributes.AsynchronousWrites createRegionAttributesTypeDiskWriteAttributesAsynchronousWrites() {
    return new RegionAttributesType.DiskWriteAttributes.AsynchronousWrites();
  }

  /**
   * Create an instance of {@link PdxType.PdxSerializer }
   *
   */
  public PdxType.PdxSerializer createPdxTypePdxSerializer() {
    return new PdxType.PdxSerializer();
  }

  /**
   * Create an instance of {@link PoolType.Locator }
   *
   */
  public PoolType.Locator createPoolTypeLocator() {
    return new PoolType.Locator();
  }

  /**
   * Create an instance of {@link PoolType.Server }
   *
   */
  public PoolType.Server createPoolTypeServer() {
    return new PoolType.Server();
  }

  /**
   * Create an instance of {@link CacheConfig.GatewayHub.Gateway.GatewayEndpoint }
   *
   */
  public CacheConfig.GatewayHub.Gateway.GatewayEndpoint createCacheGatewayHubGatewayGatewayEndpoint() {
    return new CacheConfig.GatewayHub.Gateway.GatewayEndpoint();
  }

  /**
   * Create an instance of {@link CacheConfig.GatewayHub.Gateway.GatewayListener }
   *
   */
  public CacheConfig.GatewayHub.Gateway.GatewayListener createCacheGatewayHubGatewayGatewayListener() {
    return new CacheConfig.GatewayHub.Gateway.GatewayListener();
  }

  /**
   * Create an instance of {@link CacheConfig.GatewayHub.Gateway.GatewayQueue }
   *
   */
  public CacheConfig.GatewayHub.Gateway.GatewayQueue createCacheGatewayHubGatewayGatewayQueue() {
    return new CacheConfig.GatewayHub.Gateway.GatewayQueue();
  }

  /**
   * Create an instance of {@link CacheTransactionManagerType.TransactionListener }
   *
   */
  public CacheTransactionManagerType.TransactionListener createCacheTransactionManagerTypeTransactionListener() {
    return new CacheTransactionManagerType.TransactionListener();
  }

  /**
   * Create an instance of {@link CacheTransactionManagerType.TransactionWriter }
   *
   */
  public CacheTransactionManagerType.TransactionWriter createCacheTransactionManagerTypeTransactionWriter() {
    return new CacheTransactionManagerType.TransactionWriter();
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link JndiBindingsType }{@code >}}
   *
   */
  @XmlElementDecl(namespace = "http://geode.apache.org/schema/cache", name = "jndi-bindings",
      scope = CacheConfig.class)
  public JAXBElement<JndiBindingsType> createCacheJndiBindings(JndiBindingsType value) {
    return new JAXBElement<JndiBindingsType>(_CacheJndiBindings_QNAME, JndiBindingsType.class,
        CacheConfig.class, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link RegionConfig }{@code >}}
   *
   */
  @XmlElementDecl(namespace = "http://geode.apache.org/schema/cache", name = "vm-root-region",
      scope = CacheConfig.class)
  public JAXBElement<RegionConfig> createCacheVmRootRegion(RegionConfig value) {
    return new JAXBElement<RegionConfig>(_CacheVmRootRegion_QNAME, RegionConfig.class,
        CacheConfig.class, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link RegionConfig }{@code >}}
   *
   */
  @XmlElementDecl(namespace = "http://geode.apache.org/schema/cache", name = "region",
      scope = CacheConfig.class)
  public JAXBElement<RegionConfig> createCacheRegion(RegionConfig value) {
    return new JAXBElement<RegionConfig>(_CacheRegion_QNAME, RegionConfig.class, CacheConfig.class,
        value);
  }

}
