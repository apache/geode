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

package org.apache.geode.internal.cache.xmlcache;

import static javax.xml.XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI;
import static org.apache.geode.internal.cache.xmlcache.XmlGeneratorUtils.addAttribute;
import static org.apache.geode.management.internal.configuration.utils.XmlConstants.W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION;
import static org.apache.geode.management.internal.configuration.utils.XmlConstants.W3C_XML_SCHEMA_INSTANCE_PREFIX;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.internal.index.HashIndex;
import org.apache.geode.cache.query.internal.index.PrimaryKeyIndex;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ServerLoadProbe;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.Role;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.ClientSubscriptionConfigImpl;
import org.apache.geode.internal.cache.ColocationHelper;
import org.apache.geode.internal.cache.DiskWriteAttributesImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.MemoryThresholds;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.persistence.DefaultDiskDirs;
import org.apache.geode.internal.size.SizeClassOnceObjectSizer;
import org.apache.geode.management.internal.configuration.utils.XmlConstants;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

/**
 * Generates a declarative XML file that describes a given {@link Cache} instance. This class was
 * developed for testing purposes, but it is conceivable that it could be used in the product as
 * well.
 *
 * @since GemFire 3.0
 */
@SuppressWarnings("deprecation")
public class CacheXmlGenerator extends CacheXml implements XMLReader {

  /** An empty <code>Attributes</code> */
  @Immutable
  private static final Attributes EMPTY = new AttributesImpl();

  /** The content handler to which SAX events are generated */
  private ContentHandler handler;

  /** The Cache that we're generating XML for */
  private final Cache cache;

  /**
   * Will the generated XML file reference an XML schema instead of the DTD?
   */
  private boolean useSchema;
  private boolean includeKeysValues = true;
  private final boolean generateDefaults;

  /** The <code>CacheCreation</code> from which XML is generated */
  private final CacheCreation creation;

  /**
   * Examines the given <code>Cache</code> and from it generates XML data that is written to the
   * given <code>PrintWriter</code>. The schema/dtd for the current version of GemFire is used.
   *
   * @param useSchema Should the generated XML reference a schema (as opposed to a DTD)? For
   *        versions 8.1 or newer this should be true, otherwise false.
   * @param version The version of GemFire whose DTD/schema should be used in the generated XML. See
   *        {@link #VERSION_4_0}.
   *
   * @since GemFire 4.0
   */
  public static void generate(Cache cache, PrintWriter pw, boolean useSchema, String version) {
    new CacheXmlGenerator(cache, useSchema, version, true).generate(pw);
  }

  /**
   * Examines the given <code>Cache</code> and from it generates XML data that is written to the
   * given <code>PrintWriter</code>. The schema/dtd for the current version of GemFire is used.
   *
   * @param includeKeysValues true if the xml should include keys and values false otherwise
   */
  public static void generate(Cache cache, PrintWriter pw, boolean includeKeysValues) {
    new CacheXmlGenerator(cache, true, VERSION_LATEST, includeKeysValues).generate(pw);
  }

  /**
   * @param useSchema Should the generated XML reference a schema (as opposed to a DTD)? As of 8.1
   *        this value is ignored and always true.
   * @param includeDefaults set to false to cause generated xml to not have defaults values.
   * @deprecated Only used for rolling upgrades
   */
  @Deprecated
  public static void generate(Cache cache, PrintWriter pw, boolean useSchema,
      boolean includeKeysValues, boolean includeDefaults) {
    new CacheXmlGenerator(cache, true, VERSION_LATEST, includeKeysValues, includeDefaults)
        .generate(pw);
  }

  /**
   * @param includeDefaults set to false to cause generated xml to not have defaults values.
   */
  public static void generate(Cache cache, PrintWriter pw, boolean includeKeysValues,
      boolean includeDefaults) {
    new CacheXmlGenerator(cache, true, VERSION_LATEST, includeKeysValues, includeDefaults)
        .generate(pw);
  }

  /**
   * Examines the given <code>Cache</code> and from it generates XML data that is written to the
   * given <code>PrintWriter</code>.
   */
  public static void generate(Cache cache, PrintWriter pw) {
    new CacheXmlGenerator(cache, true, VERSION_LATEST, true).generate(pw);
  }

  /**
   * Examines the given <code>ClientCache</code> and from it generates XML data that is written to
   * the given <code>PrintWriter</code>. The schema/dtd for the current version of GemFire is used.
   *
   * @param useSchema Should the generated XML reference a schema (as opposed to a DTD)? For
   *        versions 8.1 or newer this should be true, otherwise false.
   * @param version The version of GemFire whose DTD/schema should be used in the generated XML. See
   *        {@link #VERSION_4_0}.
   *
   * @since GemFire 6.5
   */
  public static void generate(ClientCache cache, PrintWriter pw, boolean useSchema,
      String version) {
    new CacheXmlGenerator(cache, useSchema, version, true).generate(pw);
  }

  /**
   * Examines the given <code>ClientCache</code> and from it generates XML data that is written to
   * the given <code>PrintWriter</code>. The schema/dtd for the current version of GemFire is used.
   *
   * @param includeKeysValues true if the xml should include keys and values false otherwise
   */
  public static void generate(ClientCache cache, PrintWriter pw, boolean includeKeysValues) {
    new CacheXmlGenerator(cache, true, VERSION_LATEST, includeKeysValues).generate(pw);
  }

  /**
   * Examines the given <code>Cache</code> and from it generates XML data that is written to the
   * given <code>PrintWriter</code>.
   */
  public static void generate(ClientCache cache, PrintWriter pw) {
    new CacheXmlGenerator(cache, true, VERSION_LATEST, true).generate(pw);
  }

  /**
   * Writes a default cache.xml to pw.
   */
  public static void generateDefault(PrintWriter pw) {
    (new CacheXmlGenerator()).generate(pw);
  }

  /**
   * Creates a new <code>CacheXmlGenerator</code> that generates XML for a given <code>Cache</code>.
   */
  private CacheXmlGenerator(Cache cache, boolean useSchema, String version,
      boolean includeKeysValues) {
    this(cache, useSchema, version, includeKeysValues, true);
  }

  private CacheXmlGenerator(Cache cache, boolean useSchema, String version,
      boolean includeKeysValues, boolean generateDefaults) {
    this.cache = cache;
    this.useSchema = useSchema;
    this.version = CacheXmlVersion.valueForVersion(version);
    this.includeKeysValues = includeKeysValues;
    this.generateDefaults = generateDefaults;

    if (cache instanceof CacheCreation) {
      creation = (CacheCreation) cache;
      creation.startingGenerate();

    } else if (cache instanceof GemFireCacheImpl) {
      if (((InternalCache) cache).isClient()) {
        creation = new ClientCacheCreation();
        if (generateDefaults() || cache.getCopyOnRead()) {
          creation.setCopyOnRead(cache.getCopyOnRead());
        }
      } else {
        // if we are not generating defaults then create the CacheCreation for parsing
        // so that we can fetch the actual PoolManager and not a fake.
        creation = new CacheCreation(!generateDefaults);
        if (generateDefaults() || cache.getLockLease() != GemFireCacheImpl.DEFAULT_LOCK_LEASE) {
          creation.setLockLease(cache.getLockLease());
        }
        if (generateDefaults() || cache.getLockTimeout() != GemFireCacheImpl.DEFAULT_LOCK_TIMEOUT) {
          creation.setLockTimeout(cache.getLockTimeout());
        }
        if (generateDefaults()
            || cache.getSearchTimeout() != GemFireCacheImpl.DEFAULT_SEARCH_TIMEOUT) {
          creation.setSearchTimeout(cache.getSearchTimeout());
        }
        if (generateDefaults() || cache.isServer()) {
          creation.setIsServer(cache.isServer());
        }
        if (generateDefaults() || cache.getCopyOnRead()) {
          creation.setCopyOnRead(cache.getCopyOnRead());
        }
      }
    } else {
      // if we are not generating defaults then create the CacheCreation for parsing
      // so that we can fetch the actual PoolManager and not a fake.
      creation = new CacheCreation(!generateDefaults);
      if (generateDefaults() || cache.getLockLease() != GemFireCacheImpl.DEFAULT_LOCK_LEASE) {
        creation.setLockLease(cache.getLockLease());
      }
      if (generateDefaults() || cache.getLockTimeout() != GemFireCacheImpl.DEFAULT_LOCK_TIMEOUT) {
        creation.setLockTimeout(cache.getLockTimeout());
      }
      if (generateDefaults()
          || cache.getSearchTimeout() != GemFireCacheImpl.DEFAULT_SEARCH_TIMEOUT) {
        creation.setSearchTimeout(cache.getSearchTimeout());
      }
      if (generateDefaults() || cache.isServer()) {
        creation.setIsServer(cache.isServer());
      }
      if (generateDefaults() || cache.getCopyOnRead()) {
        creation.setCopyOnRead(cache.getCopyOnRead());
      }
    }
  }

  /**
   * Creates a new <code>CacheXmlGenerator</code> that generates XML for a given
   * <code>ClientCache</code>.
   */
  private CacheXmlGenerator(ClientCache cache, boolean useSchema, String version,
      boolean includeKeysValues) {
    this.cache = (Cache) cache;
    this.useSchema = useSchema;
    this.version = CacheXmlVersion.valueForVersion(version);
    this.includeKeysValues = includeKeysValues;

    generateDefaults = true;

    if (cache instanceof ClientCacheCreation) {
      creation = (ClientCacheCreation) cache;
      creation.startingGenerate();
    } else {
      creation = new ClientCacheCreation();
      if (cache.getCopyOnRead()) {
        creation.setCopyOnRead(cache.getCopyOnRead());
      }
    }
  }

  /**
   * return true if default values should be generated.
   */
  private boolean generateDefaults() {
    return generateDefaults;
  }

  /**
   * Creates a generator for a default cache.
   */
  private CacheXmlGenerator() {
    cache = null;
    useSchema = true;
    version = CacheXmlVersion.valueForVersion(VERSION_LATEST);
    generateDefaults = true;

    creation = new CacheCreation();
    creation.setLockLease(GemFireCacheImpl.DEFAULT_LOCK_LEASE);
    creation.setLockTimeout(GemFireCacheImpl.DEFAULT_LOCK_TIMEOUT);
    creation.setSearchTimeout(GemFireCacheImpl.DEFAULT_SEARCH_TIMEOUT);
    // No cache proxy
    creation.setIsServer(false);
    creation.setCopyOnRead(GemFireCacheImpl.DEFAULT_COPY_ON_READ);
  }

  /**
   * Writes the generator's state to pw
   */
  private void generate(PrintWriter pw) {
    // Use JAXP's transformation API to turn SAX events into pretty
    // XML text
    try {
      Source src = new SAXSource(this, new InputSource());
      Result res = new StreamResult(pw);

      TransformerFactory xFactory = TransformerFactory.newInstance();
      Transformer xform = xFactory.newTransformer();
      xform.setOutputProperty(OutputKeys.METHOD, "xml");
      xform.setOutputProperty(OutputKeys.INDENT, "yes");
      if (!useSchema) {
        // set the doctype system and public ids from version for older DTDs.
        xform.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, version.getSystemId());
        xform.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, version.getPublicId());
      }
      xform.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
      xform.transform(src, res);
      pw.flush();

    } catch (Exception ex) {
      if (ExceptionUtils.getRootCause(ex) instanceof CancelException) {
        throw (CancelException) ExceptionUtils.getRootCause(ex);
      }
      throw new RuntimeException("An Exception was thrown while generating XML.", ex);
    }
  }

  /**
   * Called by the transformer to parse the "input source". We ignore the input source and, instead,
   * generate SAX events to the {@link #setContentHandler ContentHandler}.
   */
  @Override
  public void parse(InputSource input) throws SAXException {
    Assert.assertTrue(handler != null);

    boolean isClientCache = creation instanceof ClientCacheCreation;

    handler.startDocument();

    AttributesImpl atts = new AttributesImpl();
    if (useSchema) {
      if (null == version.getSchemaLocation()) {
        throw new IllegalStateException("No schema for version " + version.getVersion());
      }
      // add schema location for cache schema.
      handler.startPrefixMapping(W3C_XML_SCHEMA_INSTANCE_PREFIX, W3C_XML_SCHEMA_INSTANCE_NS_URI);
      addAttribute(atts, W3C_XML_SCHEMA_INSTANCE_PREFIX,
          W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION,
          version.getNamespace() + " " + version.getSchemaLocation());

      // add cache schema to default prefix.
      handler.startPrefixMapping(XmlConstants.DEFAULT_PREFIX, version.getNamespace());
      addAttribute(atts, VERSION, version.getVersion());
    }

    // Don't generate XML for attributes that are not set.

    if (creation.hasLockLease()) {
      atts.addAttribute("", "", LOCK_LEASE, "", String.valueOf(creation.getLockLease()));
    }
    if (creation.hasLockTimeout()) {
      atts.addAttribute("", "", LOCK_TIMEOUT, "", String.valueOf(creation.getLockTimeout()));
    }
    if (creation.hasSearchTimeout()) {
      atts.addAttribute("", "", SEARCH_TIMEOUT, "", String.valueOf(creation.getSearchTimeout()));
    }
    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_1) >= 0) {
      if (creation.hasMessageSyncInterval()) {
        atts.addAttribute("", "", MESSAGE_SYNC_INTERVAL, "",
            String.valueOf(creation.getMessageSyncInterval()));
      }
    }
    if (version.compareTo(CacheXmlVersion.GEMFIRE_4_0) >= 0) {
      if (creation.hasServer()) {
        atts.addAttribute("", "", IS_SERVER, "", String.valueOf(creation.isServer()));
      }
      if (creation.hasCopyOnRead()) {
        atts.addAttribute("", "", COPY_ON_READ, "", String.valueOf(creation.getCopyOnRead()));
      }
    }
    if (isClientCache) {
      handler.startElement("", CLIENT_CACHE, CLIENT_CACHE, atts);
    } else {
      handler.startElement("", CACHE, CACHE, atts);
    }
    if (cache != null) {
      if (!isClientCache) {
        generate(cache.getCacheTransactionManager());
      } else if (version.compareTo(CacheXmlVersion.GEMFIRE_6_6) >= 0) {
        generate(cache.getCacheTransactionManager());
      }

      generateDynamicRegionFactory(cache);

      if (!isClientCache) {
        if (version.compareTo(CacheXmlVersion.GEMFIRE_7_0) >= 0) {
          Set<GatewaySender> senderSet = cache.getGatewaySenders();
          for (GatewaySender sender : senderSet) {
            generateGatewaySender(sender);
          }
          generateGatewayReceiver(cache);
          generateAsyncEventQueue(cache);
        }
      }

      if (!isClientCache && version.compareTo(CacheXmlVersion.GEMFIRE_7_0) >= 0) {
        if (cache.getGatewayConflictResolver() != null) {
          generate(GATEWAY_CONFLICT_RESOLVER, cache.getGatewayConflictResolver());
        }
      }

      if (!isClientCache) {
        for (CacheServer bridge : cache.getCacheServers()) {
          generate(bridge);
        }
      }

      if (version.compareTo(CacheXmlVersion.GEMFIRE_5_7) >= 0) {
        Iterator pools;
        if (cache instanceof GemFireCacheImpl) {
          pools = PoolManager.getAll().values().iterator();
        } else {
          pools = creation.getPools().values().iterator();
        }
        while (pools.hasNext()) {
          Pool cp = (Pool) pools.next();
          generate(cp);
        }
      }

      if (version.compareTo(CacheXmlVersion.GEMFIRE_6_5) >= 0) {
        if (cache instanceof GemFireCacheImpl) {
          InternalCache gfc = (InternalCache) cache;
          for (DiskStore ds : gfc.listDiskStores()) {
            generate(ds);
          }
        } else {
          for (DiskStore ds : creation.listDiskStores()) {
            generate(ds);
          }
        }
      }
      if (version.compareTo(CacheXmlVersion.GEMFIRE_6_6) >= 0) {
        generatePdx();
      }

      if (version.compareTo(CacheXmlVersion.GEMFIRE_4_1) >= 0) {
        Map namedAttributes = cache.listRegionAttributes();
        for (Object o : namedAttributes.entrySet()) {
          Map.Entry entry = (Map.Entry) o;
          String id = (String) entry.getKey();
          RegionAttributes attrs = (RegionAttributes) entry.getValue();
          // Since CacheCreation predefines these even in later versions
          // we need to exclude them in all versions.
          // It would be better if CacheCreation could only predefine them
          // for versions 6.5 and later but that is not easy to do
          {
            if (creation instanceof ClientCacheCreation) {
              try {
                ClientRegionShortcut.valueOf(id);
                // skip this client region since id mapped to one of the enum types
                continue;
              } catch (IllegalArgumentException ignore) {
                // id is not a shortcut so go ahead and call generate
              }
            } else {
              try {
                RegionShortcut.valueOf(id);
                // skip this region since id mapped to one of the enum types
                continue;
              } catch (IllegalArgumentException ignore) {
                // id is not a shortcut so go ahead and call generate
              }
            }
          }
          generate(id, attrs);
        }
      }

      if (cache instanceof GemFireCacheImpl) {
        generateRegions();
      } else {
        TreeSet<Region<?, ?>> rSet = new TreeSet<>(new RegionComparator());
        rSet.addAll(cache.rootRegions());
        for (Region<?, ?> region : rSet) {
          generateRegion(region);
        }
      }

      if (version.compareTo(CacheXmlVersion.GEMFIRE_5_8) >= 0) {
        generateFunctionService();
      }
      if (version.compareTo(CacheXmlVersion.GEMFIRE_6_0) >= 0) {
        generateResourceManager();
        generateSerializerRegistration();
      }
      if (!isClientCache) {
        if (version.compareTo(CacheXmlVersion.GEMFIRE_6_5) >= 0) {
          if (cache instanceof GemFireCacheImpl) {
            InternalCache internalCache = (InternalCache) cache;
            for (File file : internalCache.getBackupFiles()) {
              generateBackupFile(file);
            }
          } else {
            for (File file : creation.getBackupFiles()) {
              generateBackupFile(file);
            }
          }
        }
      }
      if (version.compareTo(CacheXmlVersion.GEMFIRE_6_6) >= 0) {
        generateInitializer();
      }
    }

    if (cache instanceof Extensible) {
      @SuppressWarnings("unchecked")
      final Extensible<Cache> extensible = (Extensible<Cache>) cache;
      generate(extensible);
    }

    if (isClientCache) {
      handler.endElement("", CLIENT_CACHE, CLIENT_CACHE);
    } else {
      handler.endElement("", CACHE, CACHE);
    }
    handler.endDocument();
  }

  private void generatePdx() throws SAXException {
    AttributesImpl atts = new AttributesImpl();
    CacheConfig config = ((InternalCache) cache).getCacheConfig();
    if (config.pdxReadSerializedUserSet) {
      if (generateDefaults() || cache.getPdxReadSerialized())
        atts.addAttribute("", "", READ_SERIALIZED, "",
            Boolean.toString(cache.getPdxReadSerialized()));
    }
    if (config.pdxIgnoreUnreadFieldsUserSet) {
      if (generateDefaults() || cache.getPdxIgnoreUnreadFields())
        atts.addAttribute("", "", IGNORE_UNREAD_FIELDS, "",
            Boolean.toString(cache.getPdxIgnoreUnreadFields()));
    }
    if (config.pdxPersistentUserSet) {
      if (generateDefaults() || cache.getPdxPersistent())
        atts.addAttribute("", "", PERSISTENT, "", Boolean.toString(cache.getPdxPersistent()));
    }
    if (config.pdxDiskStoreUserSet) {
      if (generateDefaults()
          || cache.getPdxDiskStore() != null && !cache.getPdxDiskStore().equals(""))
        atts.addAttribute("", "", DISK_STORE_NAME, "", cache.getPdxDiskStore());
    }
    if (!generateDefaults() && cache.getPdxSerializer() == null && atts.getLength() == 0) {
      return;
    }
    handler.startElement("", PDX, PDX, atts);

    if (cache.getPdxSerializer() != null) {
      generate(PDX_SERIALIZER, cache.getPdxSerializer());
    }
    handler.endElement("", PDX, PDX);
  }

  private void generateInitializer() throws SAXException {
    if (cache.getInitializer() != null) {
      generate(cache.getInitializer(), cache.getInitializerProps());
    }
  }

  private void generateRegion(Region root) throws SAXException {
    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_0) >= 0) {
      generate(root, REGION);
    } else {
      generate(root, VM_ROOT_REGION);
    }
  }

  private void generateRegions() throws SAXException {
    Set<Region<?, ?>> colocatedChildRegions = new HashSet<>();
    Set<Region> generatedRegions = new HashSet<>();

    // Merge from persist_Nov10 - iterate the regions in order for persistent recovery.
    TreeSet<Region<?, ?>> rSet = new TreeSet<>(new RegionComparator());
    rSet.addAll(cache.rootRegions());
    for (Region<?, ?> root : rSet) {
      Assert.assertTrue(root instanceof LocalRegion);
      if (root instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion) root;
        if (pr.getColocatedWith() != null) {
          colocatedChildRegions.add(root);
        } else {
          generateRegion(root); // normal PR or root in colocated chain
          generatedRegions.add(root);
        }
      } else {
        // normal non pr regions, but they can have PR as subregions
        boolean found = false;
        for (Region<?, ?> subregion : root.subregions(false)) {
          Assert.assertTrue(subregion instanceof LocalRegion);
          if (subregion instanceof PartitionedRegion) {
            PartitionedRegion pr = (PartitionedRegion) subregion;
            if (pr.getColocatedWith() != null) {
              colocatedChildRegions.add(root);
              found = true;
              break;
            }
          }
        }
        if (!found) {
          generateRegion(root); // normal non pr regions
          generatedRegions.add(root);
        }
      }
    }
    TreeSet<Region<?, ?>> rColSet = new TreeSet<>(new RegionComparator());
    rColSet.addAll(colocatedChildRegions);
    for (Region<?, ?> root : rColSet) {
      Assert.assertTrue(root instanceof LocalRegion);
      if (root instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion) root;
        PartitionedRegion colocatedWithPr = ColocationHelper.getColocatedRegion(pr);
        if (colocatedWithPr != null && !generatedRegions.contains(colocatedWithPr)) {
          generateRegion(colocatedWithPr);
          generatedRegions.add(colocatedWithPr);
        }
        if (!generatedRegions.contains(root)) {
          generateRegion(root);
          generatedRegions.add(root);
        }
      } else {
        generateRegion(root);
        generatedRegions.add(root);
      }
    }
  }

  /**
   * Generate a resource-manager element
   */
  private void generateResourceManager() throws SAXException {
    AttributesImpl atts = new AttributesImpl();
    if (cache instanceof CacheCreation && creation.hasResourceManager()) {
      boolean generateIt = false;
      if (creation.getResourceManager().hasCriticalHeap()) {
        float chp = creation.getResourceManager().getCriticalHeapPercentage();
        if (generateDefaults() || chp != MemoryThresholds.DEFAULT_CRITICAL_PERCENTAGE) {
          atts.addAttribute("", "", CRITICAL_HEAP_PERCENTAGE, "", String.valueOf(chp));
          generateIt = true;
        }
      }
      if (creation.getResourceManager().hasEvictionHeap()) {
        float ehp = creation.getResourceManager().getEvictionHeapPercentage();
        if (generateDefaults() || ehp != MemoryThresholds.DEFAULT_EVICTION_PERCENTAGE) {
          atts.addAttribute("", "", EVICTION_HEAP_PERCENTAGE, "", String.valueOf(ehp));
          generateIt = true;
        }
      }

      if (version.compareTo(CacheXmlVersion.GEODE_1_0) >= 0) {
        if (creation.getResourceManager().hasCriticalOffHeap()) {
          float chp = creation.getResourceManager().getCriticalOffHeapPercentage();
          if (generateDefaults() || chp != MemoryThresholds.DEFAULT_CRITICAL_PERCENTAGE) {
            atts.addAttribute("", "", CRITICAL_OFF_HEAP_PERCENTAGE, "", String.valueOf(chp));
            generateIt = true;
          }
        }
        if (creation.getResourceManager().hasEvictionOffHeap()) {
          float ehp = creation.getResourceManager().getEvictionOffHeapPercentage();
          if (generateDefaults() || ehp != MemoryThresholds.DEFAULT_EVICTION_PERCENTAGE) {
            atts.addAttribute("", "", EVICTION_OFF_HEAP_PERCENTAGE, "", String.valueOf(ehp));
            generateIt = true;
          }
        }
      }
      if (generateIt) {
        generateResourceManagerElement(atts);
      }
    } else if (cache instanceof GemFireCacheImpl) {
      {
        int chp = (int) cache.getResourceManager().getCriticalHeapPercentage();
        if (generateDefaults() || chp != MemoryThresholds.DEFAULT_CRITICAL_PERCENTAGE)

          atts.addAttribute("", "", CRITICAL_HEAP_PERCENTAGE, "", String.valueOf(chp));
      }
      {
        int ehp = (int) cache.getResourceManager().getEvictionHeapPercentage();
        if (generateDefaults() || ehp != MemoryThresholds.DEFAULT_EVICTION_PERCENTAGE)
          atts.addAttribute("", "", EVICTION_HEAP_PERCENTAGE, "", String.valueOf(ehp));
      }

      if (version.compareTo(CacheXmlVersion.GEODE_1_0) >= 0) {
        {
          int chp = (int) cache.getResourceManager().getCriticalOffHeapPercentage();
          if (generateDefaults() || chp != MemoryThresholds.DEFAULT_CRITICAL_PERCENTAGE)

            atts.addAttribute("", "", CRITICAL_OFF_HEAP_PERCENTAGE, "", String.valueOf(chp));
        }
        {
          int ehp = (int) cache.getResourceManager().getEvictionOffHeapPercentage();
          if (generateDefaults() || ehp != MemoryThresholds.DEFAULT_EVICTION_PERCENTAGE)
            atts.addAttribute("", "", EVICTION_OFF_HEAP_PERCENTAGE, "", String.valueOf(ehp));
        }
      }
      if (generateDefaults() || atts.getLength() > 0)
        generateResourceManagerElement(atts);
    }
  }

  private void generateResourceManagerElement(AttributesImpl atts) throws SAXException {
    handler.startElement("", RESOURCE_MANAGER, RESOURCE_MANAGER, atts);
    handler.endElement("", RESOURCE_MANAGER, RESOURCE_MANAGER);
  }

  private void generateBackupFile(File file) throws SAXException {
    handler.startElement("", BACKUP, BACKUP, EMPTY);
    handler.characters(file.getPath().toCharArray(), 0, file.getPath().length());
    handler.endElement("", BACKUP, BACKUP);
  }

  /**
   * Generates the <code>serializer-registration</code> element.
   *
   */
  private void generateSerializerRegistration() throws SAXException {
    final SerializerCreation sc = creation.getSerializerCreation();
    if (sc == null) {
      return;
    }

    handler.startElement("", TOP_SERIALIZER_REGISTRATION, TOP_SERIALIZER_REGISTRATION, EMPTY);
    for (Class c : sc.getSerializerRegistrations()) {
      handler.startElement("", SERIALIZER_REGISTRATION, SERIALIZER_REGISTRATION, EMPTY);
      handler.startElement("", CLASS_NAME, CLASS_NAME, EMPTY);
      handler.characters(c.getName().toCharArray(), 0, c.getName().length());
      handler.endElement("", CLASS_NAME, CLASS_NAME);
      handler.endElement("", SERIALIZER_REGISTRATION, SERIALIZER_REGISTRATION);
    }

    for (Map.Entry<Class, Integer> e : sc.getInstantiatorRegistrations().entrySet()) {
      Class c = e.getKey();
      Integer i = e.getValue();

      AttributesImpl atts = new AttributesImpl();
      atts.addAttribute("", "", ID, "", i.toString());
      handler.startElement("", INSTANTIATOR_REGISTRATION, INSTANTIATOR_REGISTRATION, atts);
      handler.startElement("", CLASS_NAME, CLASS_NAME, EMPTY);
      handler.characters(c.getName().toCharArray(), 0, c.getName().length());
      handler.endElement("", CLASS_NAME, CLASS_NAME);
      handler.endElement("", INSTANTIATOR_REGISTRATION, INSTANTIATOR_REGISTRATION);
    }

    handler.endElement("", TOP_SERIALIZER_REGISTRATION, TOP_SERIALIZER_REGISTRATION);
  }

  private void generateFunctionService() throws SAXException {
    Collection<Function> functions = Collections.emptyList();
    if (cache instanceof CacheCreation) {
      if (creation.hasFunctionService()) {
        functions = creation.getFunctionServiceCreation().getFunctionList();
      }
    } else {
      functions = FunctionService.getRegisteredFunctions().values();
    }
    if (!generateDefaults() && functions.isEmpty()) {
      return;
    }
    handler.startElement("", FUNCTION_SERVICE, FUNCTION_SERVICE, EMPTY);
    for (Function function : functions) {
      if (function instanceof Declarable) {
        handler.startElement("", FUNCTION, FUNCTION, EMPTY);
        generate((Declarable) function, false);
        handler.endElement("", FUNCTION, FUNCTION);
      }
    }
    handler.endElement("", FUNCTION_SERVICE, FUNCTION_SERVICE);
  }

  /**
   * Generates XML for the client-subscription tag
   *
   * @param bridge instance of <code>CacheServer</code>
   *
   * @since GemFire 5.7
   */
  private void generateClientHaQueue(CacheServer bridge) {
    AttributesImpl atts = new AttributesImpl();
    ClientSubscriptionConfigImpl csc =
        (ClientSubscriptionConfigImpl) bridge.getClientSubscriptionConfig();
    try {
      atts.addAttribute("", "", CLIENT_SUBSCRIPTION_EVICTION_POLICY, "", csc.getEvictionPolicy());
      atts.addAttribute("", "", CLIENT_SUBSCRIPTION_CAPACITY, "",
          String.valueOf(csc.getCapacity()));
      if (version.compareTo(CacheXmlVersion.GEMFIRE_6_5) >= 0) {
        String dsVal = csc.getDiskStoreName();
        if (dsVal != null) {
          atts.addAttribute("", "", DISK_STORE_NAME, "", dsVal);
        }
      }
      if (csc.getDiskStoreName() == null && csc.hasOverflowDirectory()) {
        atts.addAttribute("", "", OVERFLOW_DIRECTORY, "", csc.getOverflowDirectory());
      }
      handler.startElement("", CLIENT_SUBSCRIPTION, CLIENT_SUBSCRIPTION, atts);
      handler.endElement("", CLIENT_SUBSCRIPTION, CLIENT_SUBSCRIPTION);

    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Generates XML for the given cache server
   *
   * @since GemFire 4.0
   */
  private void generate(CacheServer bridge) throws SAXException {
    if (version.compareTo(CacheXmlVersion.GEMFIRE_4_0) < 0) {
      return;
    }
    AttributesImpl atts = new AttributesImpl();
    try {
      if (generateDefaults() || bridge.getPort() != CacheServer.DEFAULT_PORT)
        atts.addAttribute("", "", PORT, "", String.valueOf(bridge.getPort()));

      if (version.compareTo(CacheXmlVersion.GEMFIRE_4_1) < 0) {
        return;
      }
      if (generateDefaults()
          || bridge.getMaximumTimeBetweenPings() != CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS)
        atts.addAttribute("", "", MAXIMUM_TIME_BETWEEN_PINGS, "",
            String.valueOf(bridge.getMaximumTimeBetweenPings()));

      if (generateDefaults()
          || bridge.getNotifyBySubscription() != CacheServer.DEFAULT_NOTIFY_BY_SUBSCRIPTION)
        atts.addAttribute("", "", NOTIFY_BY_SUBSCRIPTION, "",
            String.valueOf(bridge.getNotifyBySubscription()));

      if (generateDefaults()
          || bridge.getSocketBufferSize() != CacheServer.DEFAULT_SOCKET_BUFFER_SIZE)
        atts.addAttribute("", "", SOCKET_BUFFER_SIZE, "",
            String.valueOf(bridge.getSocketBufferSize()));

      if (version.compareTo(CacheXmlVersion.GEMFIRE_5_0) < 0) {
        return;
      }

      if (generateDefaults() || bridge.getMaxConnections() != CacheServer.DEFAULT_MAX_CONNECTIONS)
        atts.addAttribute("", "", MAX_CONNECTIONS, "", String.valueOf(bridge.getMaxConnections()));

      if (version.compareTo(CacheXmlVersion.GEMFIRE_5_1) < 0) {
        return;
      }

      if (generateDefaults() || bridge.getMaxThreads() != CacheServer.DEFAULT_MAX_THREADS)
        atts.addAttribute("", "", MAX_THREADS, "", String.valueOf(bridge.getMaxThreads()));
      if (generateDefaults()
          || bridge.getMaximumMessageCount() != CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT)
        atts.addAttribute("", "", MAXIMUM_MESSAGE_COUNT, "",
            String.valueOf(bridge.getMaximumMessageCount()));

      if (generateDefaults()
          || bridge.getMessageTimeToLive() != CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE) {
        atts.addAttribute("", "", MESSAGE_TIME_TO_LIVE, "",
            String.valueOf(bridge.getMessageTimeToLive()));
      }

      if (version.compareTo(CacheXmlVersion.GEMFIRE_5_7) < 0) {
        return;
      }

      if (bridge.getBindAddress() != null) {
        if (generateDefaults() || !CacheServer.DEFAULT_BIND_ADDRESS.equals(bridge.getBindAddress()))
          atts.addAttribute("", "", BIND_ADDRESS, "", bridge.getBindAddress());
      }

      if (bridge.getHostnameForClients() != null && !bridge.getHostnameForClients().equals("")) {
        atts.addAttribute("", "", HOSTNAME_FOR_CLIENTS, "", bridge.getHostnameForClients());
      }
      if (generateDefaults()
          || bridge.getLoadPollInterval() != CacheServer.DEFAULT_LOAD_POLL_INTERVAL)
        atts.addAttribute("", "", LOAD_POLL_INTERVAL, "",
            String.valueOf(bridge.getLoadPollInterval()));

      if (version.compareTo(CacheXmlVersion.GEMFIRE_8_0) < 0) {
        return;
      }

      if (generateDefaults() || bridge.getTcpNoDelay() != CacheServer.DEFAULT_TCP_NO_DELAY) {
        atts.addAttribute("", "", TCP_NO_DELAY, "", "" + bridge.getTcpNoDelay());
      }

    } finally {
      if (version.compareTo(CacheXmlVersion.GEMFIRE_5_7) >= 0) {
        handler.startElement("", CACHE_SERVER, CACHE_SERVER, atts);
      } else {
        handler.startElement("", BRIDGE_SERVER, BRIDGE_SERVER, atts);
      }

      if (version.compareTo(CacheXmlVersion.GEMFIRE_5_7) >= 0) {
        String[] groups = bridge.getGroups();
        if (groups.length > 0) {
          for (String group : groups) {
            handler.startElement("", GROUP, GROUP, EMPTY);
            handler.characters(group.toCharArray(), 0, group.length());
            handler.endElement("", GROUP, GROUP);
          }
        }

        if (!bridge.getClientSubscriptionConfig().getEvictionPolicy().equals("none")) {
          generateClientHaQueue(bridge);
        }

        ServerLoadProbe probe = bridge.getLoadProbe();
        if (generateDefaults() || !probe.equals(CacheServer.DEFAULT_LOAD_PROBE)) {
          generate(LOAD_PROBE, probe);
        }
      }
      if (version.compareTo(CacheXmlVersion.GEMFIRE_5_7) >= 0) {
        handler.endElement("", "", CACHE_SERVER);
      } else {
        handler.endElement("", "", BRIDGE_SERVER);
      }
    }
  }

  /**
   * Generates XML for the given disk store
   *
   * @since GemFire prPersistSprint2
   */
  private void generate(DiskStore ds) throws SAXException {
    if (version.compareTo(CacheXmlVersion.GEMFIRE_6_5) < 0) {
      return;
    }
    AttributesImpl atts = new AttributesImpl();
    try {
      atts.addAttribute("", "", NAME, "", ds.getName());

      if ((!(ds instanceof DiskStoreAttributesCreation)
          || ((DiskStoreAttributesCreation) ds).hasAutoCompact())) {
        if (generateDefaults() || ds.getAutoCompact() != DiskStoreFactory.DEFAULT_AUTO_COMPACT)
          atts.addAttribute("", "", AUTO_COMPACT, "", String.valueOf(ds.getAutoCompact()));
      }

      if ((!(ds instanceof DiskStoreAttributesCreation)
          || ((DiskStoreAttributesCreation) ds).hasAllowForceCompaction())) {
        if (generateDefaults()
            || ds.getAllowForceCompaction() != DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION)
          atts.addAttribute("", "", ALLOW_FORCE_COMPACTION, "",
              String.valueOf(ds.getAllowForceCompaction()));
      }

      if ((!(ds instanceof DiskStoreAttributesCreation)
          || ((DiskStoreAttributesCreation) ds).hasCompactionThreshold())) {
        if (generateDefaults()
            || ds.getCompactionThreshold() != DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD)
          atts.addAttribute("", "", COMPACTION_THRESHOLD, "",
              String.valueOf(ds.getCompactionThreshold()));
      }

      if ((!(ds instanceof DiskStoreAttributesCreation)
          || ((DiskStoreAttributesCreation) ds).hasMaxOplogSize())) {
        if (generateDefaults() || ds.getMaxOplogSize() != DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE)
          atts.addAttribute("", "", MAX_OPLOG_SIZE, "", String.valueOf(ds.getMaxOplogSize()));
      }

      if ((!(ds instanceof DiskStoreAttributesCreation)
          || ((DiskStoreAttributesCreation) ds).hasTimeInterval())) {
        if (generateDefaults() || ds.getTimeInterval() != DiskStoreFactory.DEFAULT_TIME_INTERVAL)
          atts.addAttribute("", "", TIME_INTERVAL, "", String.valueOf(ds.getTimeInterval()));
      }

      if ((!(ds instanceof DiskStoreAttributesCreation)
          || ((DiskStoreAttributesCreation) ds).hasWriteBufferSize())) {
        if (generateDefaults()
            || ds.getWriteBufferSize() != DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE)
          atts.addAttribute("", "", WRITE_BUFFER_SIZE, "", String.valueOf(ds.getWriteBufferSize()));
      }

      if ((!(ds instanceof DiskStoreAttributesCreation)
          || ((DiskStoreAttributesCreation) ds).hasQueueSize())) {
        if (generateDefaults() || ds.getQueueSize() != DiskStoreFactory.DEFAULT_QUEUE_SIZE)
          atts.addAttribute("", "", QUEUE_SIZE, "", String.valueOf(ds.getQueueSize()));
      }

      if (version.compareTo(CacheXmlVersion.GEMFIRE_8_0) >= 0) {
        if ((!(ds instanceof DiskStoreAttributesCreation)
            || ((DiskStoreAttributesCreation) ds).hasDiskUsageWarningPercentage())) {
          if (generateDefaults() || ds
              .getDiskUsageWarningPercentage() != DiskStoreFactory.DEFAULT_DISK_USAGE_WARNING_PERCENTAGE)
            atts.addAttribute("", "", DISK_USAGE_WARNING_PERCENTAGE, "",
                String.valueOf(ds.getDiskUsageWarningPercentage()));
        }

        if ((!(ds instanceof DiskStoreAttributesCreation)
            || ((DiskStoreAttributesCreation) ds).hasDiskUsageCriticalPercentage())) {
          if (generateDefaults() || ds
              .getDiskUsageCriticalPercentage() != DiskStoreFactory.DEFAULT_DISK_USAGE_CRITICAL_PERCENTAGE)
            atts.addAttribute("", "", DISK_USAGE_CRITICAL_PERCENTAGE, "",
                String.valueOf(ds.getDiskUsageCriticalPercentage()));
        }
      }
    } finally {
      handler.startElement("", DISK_STORE, DISK_STORE, atts);

      if ((!(ds instanceof DiskStoreAttributesCreation)
          || ((DiskStoreAttributesCreation) ds).hasDiskDirs())) {
        File[] diskDirs = ds.getDiskDirs();
        int[] diskSizes = ds.getDiskDirSizes();
        if (diskDirs != null && diskDirs.length > 0) {
          if (generateDefaults() || !Arrays.equals(diskDirs, DefaultDiskDirs.getDefaultDiskDirs())
              || !Arrays.equals(diskSizes, DiskStoreFactory.DEFAULT_DISK_DIR_SIZES)) {
            handler.startElement("", DISK_DIRS, DISK_DIRS, EMPTY);
            for (int i = 0; i < diskDirs.length; i++) {
              AttributesImpl diskAtts = new AttributesImpl();
              if (diskSizes[i] != DiskStoreFactory.DEFAULT_DISK_DIR_SIZE) {
                diskAtts.addAttribute("", "", DIR_SIZE, "", String.valueOf(diskSizes[i]));
              }
              handler.startElement("", DISK_DIR, DISK_DIR, diskAtts);
              File dir = diskDirs[i];
              String name = generateDefaults() ? dir.getAbsolutePath() : dir.getPath();
              handler.characters(name.toCharArray(), 0, name.length());
              handler.endElement("", DISK_DIR, DISK_DIR);
            }
            handler.endElement("", DISK_DIRS, DISK_DIRS);
          }
        }
      }

      handler.endElement("", "", DISK_STORE);
    }
  }

  /**
   * Compare regions by name
   */
  static class RegionComparator implements Comparator<Region> {
    @Override
    public int compare(Region o1, Region o2) {
      return o1.getFullPath().compareTo(o2.getFullPath());
    }
  }

  /**
   * Generates XML for the given connection pool
   *
   * @since GemFire 5.7
   */
  private void generate(Pool cp) throws SAXException {
    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_7) < 0) {
      return;
    }
    if (((PoolImpl) cp).isUsedByGateway()) {
      // no need to generate xml for gateway pools
      return;
    }
    AttributesImpl atts = new AttributesImpl();
    try {
      atts.addAttribute("", "", NAME, "", cp.getName());
      if (version.compareTo(CacheXmlVersion.GEODE_1_0) >= 0) {
        if (generateDefaults() || cp
            .getSubscriptionTimeoutMultiplier() != PoolFactory.DEFAULT_SUBSCRIPTION_TIMEOUT_MULTIPLIER) {
          atts.addAttribute("", "", SUBSCRIPTION_TIMEOUT_MULTIPLIER, "",
              String.valueOf(cp.getSubscriptionTimeoutMultiplier()));
        }
        if (generateDefaults()
            || cp.getSocketConnectTimeout() != PoolFactory.DEFAULT_SOCKET_CONNECT_TIMEOUT) {
          atts.addAttribute("", "", SOCKET_CONNECT_TIMEOUT, "",
              String.valueOf(cp.getSocketConnectTimeout()));
        }
      }
      if (generateDefaults()
          || cp.getFreeConnectionTimeout() != PoolFactory.DEFAULT_FREE_CONNECTION_TIMEOUT)
        atts.addAttribute("", "", FREE_CONNECTION_TIMEOUT, "",
            String.valueOf(cp.getFreeConnectionTimeout()));
      if (generateDefaults()
          || cp.getServerConnectionTimeout() != PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT)
        atts.addAttribute("", "", SERVER_CONNECTION_TIMEOUT, "",
            String.valueOf(cp.getServerConnectionTimeout()));
      if (generateDefaults()
          || cp.getLoadConditioningInterval() != PoolFactory.DEFAULT_LOAD_CONDITIONING_INTERVAL)
        atts.addAttribute("", "", LOAD_CONDITIONING_INTERVAL, "",
            String.valueOf(cp.getLoadConditioningInterval()));
      if (generateDefaults() || cp.getMinConnections() != PoolFactory.DEFAULT_MIN_CONNECTIONS)
        atts.addAttribute("", "", MIN_CONNECTIONS, "", String.valueOf(cp.getMinConnections()));
      if (generateDefaults() || cp.getMaxConnections() != PoolFactory.DEFAULT_MAX_CONNECTIONS)
        atts.addAttribute("", "", MAX_CONNECTIONS, "", String.valueOf(cp.getMaxConnections()));
      if (generateDefaults() || cp.getRetryAttempts() != PoolFactory.DEFAULT_RETRY_ATTEMPTS)
        atts.addAttribute("", "", RETRY_ATTEMPTS, "", String.valueOf(cp.getRetryAttempts()));
      if (generateDefaults() || cp.getIdleTimeout() != PoolFactory.DEFAULT_IDLE_TIMEOUT)
        atts.addAttribute("", "", IDLE_TIMEOUT, "", String.valueOf(cp.getIdleTimeout()));
      if (generateDefaults() || cp.getPingInterval() != PoolFactory.DEFAULT_PING_INTERVAL)
        atts.addAttribute("", "", PING_INTERVAL, "", String.valueOf(cp.getPingInterval()));
      if (generateDefaults() || cp.getStatisticInterval() != PoolFactory.DEFAULT_STATISTIC_INTERVAL)
        atts.addAttribute("", "", STATISTIC_INTERVAL, "",
            String.valueOf(cp.getStatisticInterval()));
      if (generateDefaults()
          || cp.getSubscriptionAckInterval() != PoolFactory.DEFAULT_SUBSCRIPTION_ACK_INTERVAL)
        atts.addAttribute("", "", SUBSCRIPTION_ACK_INTERVAL, "",
            String.valueOf(cp.getSubscriptionAckInterval()));
      if (generateDefaults()
          || cp.getSubscriptionEnabled() != PoolFactory.DEFAULT_SUBSCRIPTION_ENABLED)
        atts.addAttribute("", "", SUBSCRIPTION_ENABLED, "",
            String.valueOf(cp.getSubscriptionEnabled()));
      if (generateDefaults() || cp
          .getSubscriptionMessageTrackingTimeout() != PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT)
        atts.addAttribute("", "", SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT, "",
            String.valueOf(cp.getSubscriptionMessageTrackingTimeout()));
      if (generateDefaults()
          || cp.getSubscriptionRedundancy() != PoolFactory.DEFAULT_SUBSCRIPTION_REDUNDANCY)
        atts.addAttribute("", "", SUBSCRIPTION_REDUNDANCY, "",
            String.valueOf(cp.getSubscriptionRedundancy()));
      if (generateDefaults() || cp.getReadTimeout() != PoolFactory.DEFAULT_READ_TIMEOUT)
        atts.addAttribute("", "", READ_TIMEOUT, "", String.valueOf(cp.getReadTimeout()));
      if (cp.getServerGroup() != null && !cp.getServerGroup().equals("")) {
        atts.addAttribute("", "", SERVER_GROUP, "", cp.getServerGroup());
      }
      if (generateDefaults() || cp.getSocketBufferSize() != PoolFactory.DEFAULT_SOCKET_BUFFER_SIZE)
        atts.addAttribute("", "", SOCKET_BUFFER_SIZE, "", String.valueOf(cp.getSocketBufferSize()));
      if (generateDefaults()
          || cp.getThreadLocalConnections() != PoolFactory.DEFAULT_THREAD_LOCAL_CONNECTIONS)
        atts.addAttribute("", "", THREAD_LOCAL_CONNECTIONS, "",
            String.valueOf(cp.getThreadLocalConnections()));

      if (version.compareTo(CacheXmlVersion.GEMFIRE_6_1) > 0) {
        if (generateDefaults()
            || cp.getPRSingleHopEnabled() != PoolFactory.DEFAULT_PR_SINGLE_HOP_ENABLED)
          atts.addAttribute("", "", PR_SINGLE_HOP_ENABLED, "",
              String.valueOf(cp.getPRSingleHopEnabled()));
      }

      if (version.compareTo(CacheXmlVersion.GEMFIRE_6_1) > 0) {
        if (generateDefaults()
            || cp.getMultiuserAuthentication() != PoolFactory.DEFAULT_MULTIUSER_AUTHENTICATION)
          atts.addAttribute("", "", MULTIUSER_SECURE_MODE_ENABLED, "",
              String.valueOf(cp.getMultiuserAuthentication()));
      }
    } finally {
      handler.startElement("", CONNECTION_POOL, CONNECTION_POOL, atts);
      {
        for (InetSocketAddress addr : cp.getLocators()) {
          AttributesImpl sAtts = new AttributesImpl();
          sAtts.addAttribute("", "", HOST, "", addr.getHostString());
          sAtts.addAttribute("", "", PORT, "", String.valueOf(addr.getPort()));
          handler.startElement("", LOCATOR, LOCATOR, sAtts);
          handler.endElement("", LOCATOR, LOCATOR);
        }
      }
      {
        for (InetSocketAddress addr : cp.getServers()) {
          AttributesImpl sAtts = new AttributesImpl();
          sAtts.addAttribute("", "", HOST, "", addr.getHostString());
          sAtts.addAttribute("", "", PORT, "", String.valueOf(addr.getPort()));
          handler.startElement("", SERVER, SERVER, sAtts);
          handler.endElement("", SERVER, SERVER);
        }
        if (version.compareTo(CacheXmlVersion.GEODE_1_0) >= 0) {
          if (cp.getSocketFactory() != PoolFactory.DEFAULT_SOCKET_FACTORY) {
            generate(SOCKET_FACTORY, cp.getSocketFactory());
          }
        }
      }
      handler.endElement("", "", CONNECTION_POOL);
    }
  }

  /**
   * Generates XML for a CacheTransactionManager
   *
   * @since GemFire 4.0
   */
  private void generate(CacheTransactionManager txMgr) throws SAXException {
    if (version.compareTo(CacheXmlVersion.GEMFIRE_4_0) < 0) {
      return;
    }

    if (txMgr == null) {
      return;
    }
    if (!generateDefaults() && txMgr.getWriter() == null && txMgr.getListeners().length == 0) {
      return;
    }

    handler.startElement("", TRANSACTION_MANAGER, TRANSACTION_MANAGER, EMPTY);
    {
      TransactionListener[] listeners = txMgr.getListeners();
      for (TransactionListener listener : listeners) {
        generate(TRANSACTION_LISTENER, listener);
      }
      if (txMgr.getWriter() != null) {
        generate(TRANSACTION_WRITER, txMgr.getWriter());
      }
    }
    handler.endElement("", TRANSACTION_MANAGER, TRANSACTION_MANAGER);
  }

  /**
   * Generates XML for a DynamicRegionFactory.Config
   *
   * @since GemFire 4.3
   */
  private void generateDynamicRegionFactory(Cache c) throws SAXException {
    if (version.compareTo(CacheXmlVersion.GEMFIRE_4_1) < 0) {
      return;
    }
    DynamicRegionFactory.Config cfg;
    if (c instanceof CacheCreation) {
      cfg = ((CacheCreation) c).getDynamicRegionFactoryConfig();
    } else {
      DynamicRegionFactory drf = DynamicRegionFactory.get();
      if (drf == null || drf.isClosed()) {
        return;
      }
      cfg = drf.getConfig();
    }
    if (cfg == null) {
      return;
    }
    AttributesImpl atts = new AttributesImpl();
    if (!cfg.getPersistBackup())
      atts.addAttribute("", "", DISABLE_PERSIST_BACKUP, "", "true");
    if (!cfg.getRegisterInterest())
      atts.addAttribute("", "", DISABLE_REGISTER_INTEREST, "", "true");
    if (cfg.getPoolName() != null) {
      atts.addAttribute("", "", POOL_NAME, "", cfg.getPoolName());
    }
    handler.startElement("", DYNAMIC_REGION_FACTORY, DYNAMIC_REGION_FACTORY, atts);
    {
      File dir = cfg.getDiskDir();
      if (dir != null) {
        handler.startElement("", DISK_DIR, DISK_DIR, EMPTY);
        String name = generateDefaults() ? dir.getAbsolutePath() : dir.getPath();
        handler.characters(name.toCharArray(), 0, name.length());
        handler.endElement("", DISK_DIR, DISK_DIR);
      }
    }
    handler.endElement("", DYNAMIC_REGION_FACTORY, DYNAMIC_REGION_FACTORY);
  }

  private void generateGatewaySender(GatewaySender sender) throws SAXException {
    AttributesImpl atts = new AttributesImpl();
    // id
    atts.addAttribute("", "", ID, "", sender.getId());
    // remote-distributed-system
    atts.addAttribute("", "", REMOTE_DISTRIBUTED_SYSTEM_ID, "",
        String.valueOf(sender.getRemoteDSId()));
    // parallel
    if (generateDefaults() || sender.isParallel() != GatewaySender.DEFAULT_IS_PARALLEL)
      atts.addAttribute("", "", PARALLEL, "", String.valueOf(sender.isParallel()));
    // manual-start
    if (generateDefaults() || sender.isManualStart() != GatewaySender.DEFAULT_MANUAL_START)
      atts.addAttribute("", "", MANUAL_START, "", String.valueOf(sender.isManualStart()));
    // socket-buffer-size
    if (generateDefaults()
        || sender.getSocketBufferSize() != GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE)
      atts.addAttribute("", "", SOCKET_BUFFER_SIZE, "",
          String.valueOf(sender.getSocketBufferSize()));
    // socket-read-timeout
    if (generateDefaults()
        || sender.getSocketReadTimeout() != GatewaySender.DEFAULT_SOCKET_READ_TIMEOUT)
      atts.addAttribute("", "", SOCKET_READ_TIMEOUT, "",
          String.valueOf(sender.getSocketReadTimeout()));
    // enable-batch-conflation
    if (generateDefaults()
        || sender.isBatchConflationEnabled() != GatewaySender.DEFAULT_BATCH_CONFLATION)
      atts.addAttribute("", "", ENABLE_BATCH_CONFLATION, "",
          String.valueOf(sender.isBatchConflationEnabled())); // Should we use ENABLE-CONFLATION
    // batch-size
    if (generateDefaults() || sender.getBatchSize() != GatewaySender.DEFAULT_BATCH_SIZE)
      atts.addAttribute("", "", BATCH_SIZE, "", String.valueOf(sender.getBatchSize()));
    // batch-time-interval
    if (generateDefaults()
        || sender.getBatchTimeInterval() != GatewaySender.DEFAULT_BATCH_TIME_INTERVAL)
      atts.addAttribute("", "", BATCH_TIME_INTERVAL, "",
          String.valueOf(sender.getBatchTimeInterval()));
    // enable-persistence
    if (generateDefaults()
        || sender.isPersistenceEnabled() != GatewaySender.DEFAULT_PERSISTENCE_ENABLED)
      atts.addAttribute("", "", ENABLE_PERSISTENCE, "",
          String.valueOf(sender.isPersistenceEnabled()));
    // disk-store-name
    if (generateDefaults()
        || sender.getDiskStoreName() != null && !sender.getDiskStoreName().equals(""))
      atts.addAttribute("", "", DISK_STORE_NAME, "", String.valueOf(sender.getDiskStoreName()));
    // disk-synchronous
    if (generateDefaults() || sender.isDiskSynchronous() != GatewaySender.DEFAULT_DISK_SYNCHRONOUS)
      atts.addAttribute("", "", DISK_SYNCHRONOUS, "", String.valueOf(sender.isDiskSynchronous()));
    // maximum-queue-memory
    if (generateDefaults()
        || sender.getMaximumQueueMemory() != GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY)
      atts.addAttribute("", "", MAXIMUM_QUEUE_MEMORY, "",
          String.valueOf(sender.getMaximumQueueMemory()));
    // alert-threshold
    if (generateDefaults() || sender.getAlertThreshold() != GatewaySender.DEFAULT_ALERT_THRESHOLD)
      atts.addAttribute("", "", ALERT_THRESHOLD, "", String.valueOf(sender.getAlertThreshold()));

    // dispatcher-threads
    if (generateDefaults()
        || sender.getDispatcherThreads() != GatewaySender.DEFAULT_DISPATCHER_THREADS)
      atts.addAttribute("", "", DISPATCHER_THREADS, "",
          String.valueOf(sender.getDispatcherThreads()));
    // order-policy
    if (sender.getOrderPolicy() != null) {
      if (generateDefaults() || !sender.getOrderPolicy().equals(GatewaySender.DEFAULT_ORDER_POLICY))
        atts.addAttribute("", "", ORDER_POLICY, "", String.valueOf(sender.getOrderPolicy()));
    }

    handler.startElement("", GATEWAY_SENDER, GATEWAY_SENDER, atts);

    for (GatewayEventFilter gef : sender.getGatewayEventFilters()) {
      generateGatewayEventFilter(gef);
    }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_8_0) >= 0) {
      if (sender.getGatewayEventSubstitutionFilter() != null) {
        generateGatewayEventSubstitutionFilter(sender.getGatewayEventSubstitutionFilter());
      }
    }

    for (GatewayTransportFilter gsf : sender.getGatewayTransportFilters()) {
      generateGatewayTransportFilter(gsf);
    }

    handler.endElement("", GATEWAY_SENDER, GATEWAY_SENDER);
  }

  private void generateAsyncEventQueue(Cache cache) throws SAXException {
    Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncEventQueue : asyncEventQueues) {
      AttributesImpl atts = new AttributesImpl();
      // id
      atts.addAttribute("", "", ID, "", asyncEventQueue.getId());
      // parallel
      if (generateDefaults() || asyncEventQueue.isParallel() != GatewaySender.DEFAULT_IS_PARALLEL)
        atts.addAttribute("", "", PARALLEL, "", String.valueOf(asyncEventQueue.isParallel()));
      // batch-size
      if (generateDefaults() || asyncEventQueue.getBatchSize() != GatewaySender.DEFAULT_BATCH_SIZE)
        atts.addAttribute("", "", BATCH_SIZE, "", String.valueOf(asyncEventQueue.getBatchSize()));
      // batch-time-interval
      if (generateDefaults()
          || asyncEventQueue.getBatchTimeInterval() != GatewaySender.DEFAULT_BATCH_TIME_INTERVAL)
        atts.addAttribute("", "", BATCH_TIME_INTERVAL, "",
            String.valueOf(asyncEventQueue.getBatchTimeInterval()));
      // enable-batch-conflation
      if (generateDefaults()
          || asyncEventQueue.isBatchConflationEnabled() != GatewaySender.DEFAULT_BATCH_CONFLATION)
        atts.addAttribute("", "", ENABLE_BATCH_CONFLATION, "",
            String.valueOf(asyncEventQueue.isBatchConflationEnabled()));
      // maximum-queue-memory
      if (generateDefaults()
          || asyncEventQueue.getMaximumQueueMemory() != GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY)
        atts.addAttribute("", "", MAXIMUM_QUEUE_MEMORY, "",
            String.valueOf(asyncEventQueue.getMaximumQueueMemory()));
      // enable-persistence
      if (generateDefaults()
          || asyncEventQueue.isPersistent() != GatewaySender.DEFAULT_PERSISTENCE_ENABLED)
        atts.addAttribute("", "", PERSISTENT, "", String.valueOf(asyncEventQueue.isPersistent()));
      if (asyncEventQueue.isPersistent()) {
        // disk-store-name
        if (generateDefaults() || (asyncEventQueue.getDiskStoreName() != null
            && !asyncEventQueue.getDiskStoreName().equals("")))
          atts.addAttribute("", "", DISK_STORE_NAME, "",
              String.valueOf(asyncEventQueue.getDiskStoreName()));
      }
      // dispatcher-threads
      if (generateDefaults()
          || asyncEventQueue.getDispatcherThreads() != GatewaySender.DEFAULT_DISPATCHER_THREADS)
        atts.addAttribute("", "", DISPATCHER_THREADS, "",
            String.valueOf(asyncEventQueue.getDispatcherThreads()));
      // order-policy
      if (asyncEventQueue.getOrderPolicy() != null) {
        if (generateDefaults()
            || !asyncEventQueue.getOrderPolicy().equals(GatewaySender.DEFAULT_ORDER_POLICY))
          atts.addAttribute("", "", ORDER_POLICY, "",
              String.valueOf(asyncEventQueue.getOrderPolicy()));
      }
      // eviction and expiration events
      if (version.compareTo(CacheXmlVersion.GEODE_1_0) >= 0) {
        if (generateDefaults() || asyncEventQueue
            .isForwardExpirationDestroy() != (GatewaySender.DEFAULT_FORWARD_EXPIRATION_DESTROY))
          atts.addAttribute("", "", FORWARD_EXPIRATION_DESTROY, "",
              String.valueOf(asyncEventQueue.isForwardExpirationDestroy()));
      }
      // disk-synchronous
      if (generateDefaults()
          || asyncEventQueue.isDiskSynchronous() != GatewaySender.DEFAULT_DISK_SYNCHRONOUS)
        atts.addAttribute("", "", DISK_SYNCHRONOUS, "",
            String.valueOf(asyncEventQueue.isDiskSynchronous()));

      // AsyncEventQueue element start
      handler.startElement("", ASYNC_EVENT_QUEUE, ASYNC_EVENT_QUEUE, atts);

      List<GatewayEventFilter> eventFilters = asyncEventQueue.getGatewayEventFilters();
      if (eventFilters != null) {
        for (GatewayEventFilter eventFilter : eventFilters) {
          generateGatewayEventFilter(eventFilter);
        }
      }

      if (version.compareTo(CacheXmlVersion.GEMFIRE_8_0) >= 0) {
        if (asyncEventQueue.getGatewayEventSubstitutionFilter() != null) {
          generateGatewayEventSubstitutionFilter(
              asyncEventQueue.getGatewayEventSubstitutionFilter());
        }
      }

      AsyncEventListener asyncListener = asyncEventQueue.getAsyncEventListener();
      if (asyncListener != null) {
        generate(ASYNC_EVENT_LISTENER, asyncListener);
      }

      handler.endElement("", ASYNC_EVENT_QUEUE, ASYNC_EVENT_QUEUE);
    }
  }


  private void generateGatewayReceiver(Cache cache) throws SAXException {
    Set<GatewayReceiver> receiverList = cache.getGatewayReceivers();
    for (GatewayReceiver receiver : receiverList) {
      AttributesImpl atts = new AttributesImpl();
      try {
        // hostnameForSenders
        if (generateDefaults() || receiver.getHostnameForSenders() != null)
          atts.addAttribute("", "", HOSTNAME_FOR_SENDERS, "", receiver.getHostnameForSenders());
        // start port
        if (generateDefaults() || receiver.getStartPort() != GatewayReceiver.DEFAULT_START_PORT)
          atts.addAttribute("", "", START_PORT, "", String.valueOf(receiver.getStartPort()));
        // end port
        if (generateDefaults() || receiver.getEndPort() != GatewayReceiver.DEFAULT_END_PORT)
          atts.addAttribute("", "", END_PORT, "", String.valueOf(receiver.getEndPort()));
        // bind-address
        if (generateDefaults() || (receiver.getBindAddress() != null
            && !receiver.getBindAddress().equals(GatewayReceiver.DEFAULT_BIND_ADDRESS)))
          atts.addAttribute("", "", BIND_ADDRESS, "", receiver.getBindAddress());
        // maximum-time-between-pings
        if (generateDefaults() || receiver
            .getMaximumTimeBetweenPings() != GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS)
          atts.addAttribute("", "", MAXIMUM_TIME_BETWEEN_PINGS, "",
              String.valueOf(receiver.getMaximumTimeBetweenPings()));
        // socket-buffer-size
        if (generateDefaults()
            || receiver.getSocketBufferSize() != GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE)
          atts.addAttribute("", "", SOCKET_BUFFER_SIZE, "",
              String.valueOf(receiver.getSocketBufferSize()));

        if (version.compareTo(CacheXmlVersion.GEMFIRE_8_0) < 0) {
          return;
        }
        // manual-start
        if (generateDefaults() || receiver.isManualStart() != GatewayReceiver.DEFAULT_MANUAL_START)
          atts.addAttribute("", "", MANUAL_START, "", String.valueOf(receiver.isManualStart()));

      } finally {
        handler.startElement("", GATEWAY_RECEIVER, GATEWAY_RECEIVER, atts);
        for (GatewayTransportFilter gsf : receiver.getGatewayTransportFilters()) {
          generateGatewayTransportFilter(gsf);
        }
        handler.endElement("", GATEWAY_RECEIVER, GATEWAY_RECEIVER);
      }
    }
  }

  private void generateGatewayEventFilter(GatewayEventFilter gef) throws SAXException {
    handler.startElement("", GATEWAY_EVENT_FILTER, GATEWAY_EVENT_FILTER, EMPTY);
    String className = gef.getClass().getName();

    handler.startElement("", CLASS_NAME, CLASS_NAME, EMPTY);
    handler.characters(className.toCharArray(), 0, className.length());
    handler.endElement("", CLASS_NAME, CLASS_NAME);
    Properties props;
    if (gef instanceof Declarable2) {
      props = ((Declarable2) gef).getConfig();
      generate(props, null);
    }
    handler.endElement("", GATEWAY_EVENT_FILTER, GATEWAY_EVENT_FILTER);
  }

  private void generateGatewayTransportFilter(GatewayTransportFilter gef) throws SAXException {
    handler.startElement("", GATEWAY_TRANSPORT_FILTER, GATEWAY_TRANSPORT_FILTER, EMPTY);
    String className = gef.getClass().getName();

    handler.startElement("", CLASS_NAME, CLASS_NAME, EMPTY);
    handler.characters(className.toCharArray(), 0, className.length());
    handler.endElement("", CLASS_NAME, CLASS_NAME);
    Properties props;
    if (gef instanceof Declarable2) {
      props = ((Declarable2) gef).getConfig();
      generate(props, null);
    }
    handler.endElement("", GATEWAY_TRANSPORT_FILTER, GATEWAY_TRANSPORT_FILTER);
  }

  private void generateGatewayEventSubstitutionFilter(GatewayEventSubstitutionFilter filter)
      throws SAXException {

    handler.startElement("", GATEWAY_EVENT_SUBSTITUTION_FILTER, GATEWAY_EVENT_SUBSTITUTION_FILTER,
        EMPTY);
    String className = filter.getClass().getName();

    handler.startElement("", CLASS_NAME, CLASS_NAME, EMPTY);
    handler.characters(className.toCharArray(), 0, className.length());
    handler.endElement("", CLASS_NAME, CLASS_NAME);
    Properties props;
    if (filter instanceof Declarable2) {
      props = ((Declarable2) filter).getConfig();
      generate(props, null);
    }
    handler.endElement("", GATEWAY_EVENT_SUBSTITUTION_FILTER, GATEWAY_EVENT_SUBSTITUTION_FILTER);
  }

  /**
   * Generates XML for a given region
   */
  private void generate(Region<?, ?> region, String elementName) throws SAXException {
    if (region == null) {
      return;
    }

    AttributesImpl atts = new AttributesImpl();
    atts.addAttribute("", "", NAME, "", region.getName());
    if (region instanceof RegionCreation) {
      RegionCreation rc = (RegionCreation) region;
      String refId = rc.getRefid();
      if (refId != null) {
        atts.addAttribute("", "", REFID, "", refId);
      }
    }
    handler.startElement("", elementName, elementName, atts);

    if (region instanceof RegionCreation) {
      RegionCreation rc = (RegionCreation) region;
      if (rc.hasAttributes()) {
        generate(null /* unknown id */, region.getAttributes());
      }
    } else {
      generate(null /* unknown id */, region.getAttributes());
    }

    // generate index data here
    Collection<Index> indexesForRegion = cache.getQueryService().getIndexes(region);
    for (Index index : indexesForRegion) {
      generate(index);
    }

    if (region instanceof PartitionedRegion) {
      if (includeKeysValues) {
        if (!region.isEmpty()) {
          for (Object o : region.entrySet(false)) {
            Region.Entry entry = (Region.Entry) o;
            generate(entry);
          }
        }
      }
    } else {
      if (includeKeysValues) {
        for (Object o : region.entrySet(false)) {
          Region.Entry entry = (Region.Entry) o;
          generate(entry);
        }
      }
    }

    TreeSet<Region<?, ?>> rSet = new TreeSet<>(new RegionComparator());
    rSet.addAll(region.subregions(false));
    for (Region<?, ?> subregion : rSet) {
      generate(subregion, REGION);
    }

    if (region instanceof Extensible) {
      @SuppressWarnings({"unchecked"})
      Extensible<Region<?, ?>> extensible = (Extensible<Region<?, ?>>) region;
      generate(extensible);
    }

    handler.endElement("", elementName, elementName);
  }

  /**
   * Generates XML for a region entry
   */
  private void generate(Region.Entry entry) throws SAXException {
    if ((entry == null)) {
      return;
    }

    handler.startElement("", ENTRY, ENTRY, EMPTY);

    handler.startElement("", KEY, KEY, EMPTY);
    generate(entry.getKey());
    handler.endElement("", KEY, KEY);

    handler.startElement("", VALUE, VALUE, EMPTY);
    generate(entry.getValue());
    handler.endElement("", VALUE, VALUE);

    handler.endElement("", ENTRY, ENTRY);
  }

  /**
   * Generates XML for an index
   */
  private void generate(Index index) throws SAXException {
    if (index == null) {
      return;
    }
    AttributesImpl atts = new AttributesImpl();

    if (index instanceof IndexCreationData) {
      IndexCreationData indexData = (IndexCreationData) index;
      atts.addAttribute("", "", NAME, "", indexData.getIndexName());
      String indexType = indexData.getIndexType();
      if (indexType.equals("KEY")) {
        atts.addAttribute("", "", KEY_INDEX, "", "true");
      } else {
        // convert the indexType to the xml indexType
        if (indexType.equals("HASH")) {
          indexType = HASH_INDEX_TYPE;
        } else {
          indexType = RANGE_INDEX_TYPE;
        }
        atts.addAttribute("", "", KEY_INDEX, "", "false");
        atts.addAttribute("", "", INDEX_TYPE, "", "" + indexType);
      }
      atts.addAttribute("", "", FROM_CLAUSE, "", indexData.getIndexFromClause());
      atts.addAttribute("", "", EXPRESSION, "", indexData.getIndexExpression());
    } else {
      atts.addAttribute("", "", NAME, "", index.getName());
      if (index instanceof PrimaryKeyIndex) {
        atts.addAttribute("", "", KEY_INDEX, "", "true");
      } else {
        atts.addAttribute("", "", KEY_INDEX, "", "false");
        String indexType = "range";
        if (index instanceof HashIndex) {
          indexType = "hash";
        }
        atts.addAttribute("", "", INDEX_TYPE, "", "" + indexType);
      }
      atts.addAttribute("", "", FROM_CLAUSE, "", index.getFromClause());
      atts.addAttribute("", "", EXPRESSION, "", index.getIndexedExpression());
    }
    handler.startElement("", INDEX, INDEX, atts);

    handler.endElement("", INDEX, INDEX);
  }

  /**
   * Generates XML for region attributes.
   *
   * @param id The id of the named region attributes (may be <code>null</code>)
   */
  private void generate(String id, RegionAttributes<?, ?> attrs) throws SAXException {
    AttributesImpl atts = new AttributesImpl();

    if (id != null) {
      atts.addAttribute("", "", ID, "", id);
    }

    // Unless, the attrs is a "creation" instance,
    // we have no way of generating a refid, because by this
    // point, the refid information is lost.
    if (attrs instanceof RegionAttributesCreation) {
      String refId = ((RegionAttributesCreation) attrs).getRefid();
      if (refId != null) {
        atts.addAttribute("", "", REFID, "", refId);
      }
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasScope())) {
      String scopeString;
      Scope scope = attrs.getScope();
      if (scope.equals(Scope.LOCAL)) {
        scopeString = LOCAL;

      } else if (scope.equals(Scope.DISTRIBUTED_NO_ACK)) {
        scopeString = DISTRIBUTED_NO_ACK;

      } else if (scope.equals(Scope.DISTRIBUTED_ACK)) {
        scopeString = DISTRIBUTED_ACK;

      } else if (scope.equals(Scope.GLOBAL)) {
        scopeString = GLOBAL;

      } else {
        throw new InternalGemFireException(
            String.format("Unknown scope: %s", scope));
      }


      final boolean isPartitionedRegion;
      if (attrs instanceof RegionAttributesCreation) {
        RegionAttributesCreation rac = (RegionAttributesCreation) attrs;
        isPartitionedRegion = rac.getPartitionAttributes() != null
            || (rac.hasDataPolicy() && rac.getDataPolicy().withPartitioning());
      } else {
        isPartitionedRegion =
            attrs.getPartitionAttributes() != null || attrs.getDataPolicy().withPartitioning();
      }

      if (!isPartitionedRegion) {
        // Partitioned Region don't support setting scope
        if (generateDefaults() || !scope.equals(AbstractRegion.DEFAULT_SCOPE))
          atts.addAttribute("", "", SCOPE, "", scopeString);
      }
    } // hasScope

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasEarlyAck())) {
      if (generateDefaults() || attrs.getEarlyAck())
        atts.addAttribute("", "", EARLY_ACK, "", String.valueOf(attrs.getEarlyAck()));
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasMulticastEnabled())) {
      if (generateDefaults() || attrs.getMulticastEnabled())
        atts.addAttribute("", "", MULTICAST_ENABLED, "",
            String.valueOf(attrs.getMulticastEnabled()));
    }
    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasPublisher())) {
      if (generateDefaults() || attrs.getPublisher())
        atts.addAttribute("", "", PUBLISHER, "", String.valueOf(attrs.getPublisher()));
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasEnableAsyncConflation())) {
      if (generateDefaults() || attrs.getEnableAsyncConflation())
        atts.addAttribute("", "", ENABLE_ASYNC_CONFLATION, "",
            String.valueOf(attrs.getEnableAsyncConflation()));
    }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_0) >= 0) {

      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasEnableSubscriptionConflation())) {
        if (version.compareTo(CacheXmlVersion.GEMFIRE_5_7) >= 0) {
          // starting with 5.7 it is enable-subscription-conflation
          if (generateDefaults() || attrs.getEnableSubscriptionConflation())
            atts.addAttribute("", "", ENABLE_SUBSCRIPTION_CONFLATION, "",
                String.valueOf(attrs.getEnableSubscriptionConflation()));
        } else {
          // before 5.7 it was enable-bridge-conflation
          if (generateDefaults() || attrs.getEnableSubscriptionConflation())
            atts.addAttribute("", "", ENABLE_BRIDGE_CONFLATION, "",
                String.valueOf(attrs.getEnableSubscriptionConflation()));
        }
      }

      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasDataPolicy())) {
        String dpString;
        DataPolicy dp = attrs.getDataPolicy();
        if (dp.isEmpty()) {
          dpString = EMPTY_DP;
        } else if (dp.isNormal()) {
          dpString = NORMAL_DP;
        } else if (dp.isPreloaded()) {
          dpString = PRELOADED_DP;
        } else if (dp.isReplicate()) {
          dpString = REPLICATE_DP;
        } else if (dp == DataPolicy.PERSISTENT_REPLICATE) {
          dpString = PERSISTENT_REPLICATE_DP;
        } else if (dp == DataPolicy.PERSISTENT_PARTITION) {
          dpString = PERSISTENT_PARTITION_DP;
        } else if (dp.isPartition()) {
          if (version.compareTo(CacheXmlVersion.GEMFIRE_5_1) >= 0) {
            dpString = PARTITION_DP;
          } else {
            // prior to 5.1 the data policy for partitioned regions was EMPTY
            dpString = EMPTY_DP;
          }
        } else {
          throw new InternalGemFireException(
              String.format("Unknown data policy: %s", dp));
        }

        if (generateDefaults() || !dp.equals(DataPolicy.DEFAULT))
          atts.addAttribute("", "", DATA_POLICY, "", dpString);
      } // hasDataPolicy
    } // GEMFIRE_5_0 >= 0
    else { // GEMFIRE_5_0 < 0
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasEnableSubscriptionConflation())) {
        if (generateDefaults() || attrs.getEnableSubscriptionConflation())
          atts.addAttribute("", "", "enable-conflation", "",
              String.valueOf(attrs.getEnableSubscriptionConflation()));
      }

      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasMirrorType())) {
        String mirrorString;
        MirrorType mirror = attrs.getMirrorType();
        if (mirror.equals(MirrorType.NONE))
          mirrorString = NONE;
        else if (mirror.equals(MirrorType.KEYS))
          mirrorString = KEYS;
        else if (mirror.equals(MirrorType.KEYS_VALUES))
          mirrorString = KEYS_VALUES;
        else
          throw new InternalGemFireException(
              String.format("Unknown mirror type: %s", mirror));
        atts.addAttribute("", "", MIRROR_TYPE, "", mirrorString);
      }
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasPersistBackup())) {
        atts.addAttribute("", "", PERSIST_BACKUP, "",
            String.valueOf(attrs.getDataPolicy() == DataPolicy.PERSISTENT_REPLICATE));
      }
    } // GEMFIRE_5_0 < 0

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasInitialCapacity())) {
      if (generateDefaults() || attrs.getInitialCapacity() != 16)
        atts.addAttribute("", "", INITIAL_CAPACITY, "", String.valueOf(attrs.getInitialCapacity()));
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasLoadFactor())) {
      if (generateDefaults() || attrs.getLoadFactor() != 0.75f)
        atts.addAttribute("", "", LOAD_FACTOR, "", String.valueOf(attrs.getLoadFactor()));
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasConcurrencyLevel())) {
      if (generateDefaults() || attrs.getConcurrencyLevel() != 16)
        atts.addAttribute("", "", CONCURRENCY_LEVEL, "",
            String.valueOf(attrs.getConcurrencyLevel()));
    }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_7_0) >= 0) {
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasConcurrencyChecksEnabled())) {
        if (generateDefaults() || !attrs.getConcurrencyChecksEnabled())
          atts.addAttribute("", "", CONCURRENCY_CHECKS_ENABLED, "",
              String.valueOf(attrs.getConcurrencyChecksEnabled()));
      }
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasStatisticsEnabled())) {
      if (generateDefaults() || attrs.getStatisticsEnabled())
        atts.addAttribute("", "", STATISTICS_ENABLED, "",
            String.valueOf(attrs.getStatisticsEnabled()));
    }

    if (!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasIgnoreJTA()) {
      if (generateDefaults() || attrs.getIgnoreJTA())
        atts.addAttribute("", "", IGNORE_JTA, "", String.valueOf(attrs.getIgnoreJTA()));
    }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_4_0) >= 0) {
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasIsLockGrantor())) {
        if (generateDefaults() || attrs.isLockGrantor())
          atts.addAttribute("", "", IS_LOCK_GRANTOR, "", String.valueOf(attrs.isLockGrantor()));
      }
    }
    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_7) >= 0) {
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasPoolName())) {
        String cpVal = attrs.getPoolName();
        if (cpVal == null) {
          cpVal = "";
        }
        if (generateDefaults() || !cpVal.equals(""))
          atts.addAttribute("", "", POOL_NAME, "", cpVal);
      }
    }
    if (version.compareTo(CacheXmlVersion.GEMFIRE_6_5) >= 0) {
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasDiskStoreName())) {
        String dsVal = attrs.getDiskStoreName();
        if (dsVal != null) {
          atts.addAttribute("", "", DISK_STORE_NAME, "", dsVal);
        }
      }
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasDiskSynchronous())) {
        if (generateDefaults()
            || attrs.isDiskSynchronous() != AttributesFactory.DEFAULT_DISK_SYNCHRONOUS)
          atts.addAttribute("", "", DISK_SYNCHRONOUS, "",
              String.valueOf(attrs.isDiskSynchronous()));
      }
    }
    if (version.compareTo(CacheXmlVersion.GEMFIRE_6_1) >= 0)
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasCloningEnabled())) {
        if (generateDefaults() || attrs.getCloningEnabled())
          atts.addAttribute("", "", CLONING_ENABLED, "", String.valueOf(attrs.getCloningEnabled()));
      }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_7_0) >= 0) {
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasGatewaySenderId())) {
        Set<String> senderIds = new HashSet<>(attrs.getGatewaySenderIds());
        StringBuilder senderStringBuff = new StringBuilder();
        if (senderIds.size() != 0) {
          for (String senderId : senderIds) {
            if (!(senderStringBuff.length() == 0)) {
              senderStringBuff.append(",");
            }
            senderStringBuff.append(senderId);
          }
        }
        if (generateDefaults() || senderStringBuff.length() > 0)
          atts.addAttribute("", "", GATEWAY_SENDER_IDS, "", senderStringBuff.toString());
      }
    }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_7_0) >= 0) {
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasAsyncEventListeners())) {
        Set<String> asyncEventQueueIds = new HashSet<>(attrs.getAsyncEventQueueIds());
        StringBuilder asyncEventQueueStringBuff = new StringBuilder();
        if (asyncEventQueueIds.size() != 0) {
          for (String asyncEventQueueId : asyncEventQueueIds) {
            if (!(asyncEventQueueStringBuff.length() == 0)) {
              asyncEventQueueStringBuff.append(",");
            }
            asyncEventQueueStringBuff.append(asyncEventQueueId);
          }
        }
        if (generateDefaults() || asyncEventQueueStringBuff.length() > 0)
          atts.addAttribute("", "", ASYNC_EVENT_QUEUE_IDS, "",
              asyncEventQueueStringBuff.toString());
      }
    }

    if (version.compareTo(CacheXmlVersion.GEODE_1_0) >= 0) {
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasOffHeap())) {
        if (generateDefaults() || attrs.getOffHeap()) {
          atts.addAttribute("", "", OFF_HEAP, "", String.valueOf(attrs.getOffHeap()));
        }
      }
    }

    handler.startElement("", REGION_ATTRIBUTES, REGION_ATTRIBUTES, atts);

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasKeyConstraint())) {
      generate(attrs.getKeyConstraint(), KEY_CONSTRAINT);
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasValueConstraint())) {
      generate(attrs.getValueConstraint(), VALUE_CONSTRAINT);
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasRegionTimeToLive())) {
      if (generateDefaults() || !attrs.getRegionTimeToLive().equals(ExpirationAttributes.DEFAULT))
        generate(REGION_TIME_TO_LIVE, attrs.getRegionTimeToLive(), null);
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasRegionIdleTimeout())) {
      if (generateDefaults() || !attrs.getRegionIdleTimeout().equals(ExpirationAttributes.DEFAULT))
        generate(REGION_IDLE_TIME, attrs.getRegionIdleTimeout(), null);
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasEntryTimeToLive()
        || ((RegionAttributesCreation) attrs).hasCustomEntryTimeToLive())) {
      if (generateDefaults() || !attrs.getEntryTimeToLive().equals(ExpirationAttributes.DEFAULT)
          || attrs.getCustomEntryTimeToLive() != null)
        generate(ENTRY_TIME_TO_LIVE, attrs.getEntryTimeToLive(), attrs.getCustomEntryTimeToLive());
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasEntryIdleTimeout()
        || ((RegionAttributesCreation) attrs).hasCustomEntryIdleTimeout())) {
      if (generateDefaults() || !attrs.getEntryIdleTimeout().equals(ExpirationAttributes.DEFAULT)
          || attrs.getCustomEntryIdleTimeout() != null)
        generate(ENTRY_IDLE_TIME, attrs.getEntryIdleTimeout(), attrs.getCustomEntryIdleTimeout());
    }

    if (attrs.getDiskStoreName() == null
        && (generateDefaults() || version.compareTo(CacheXmlVersion.GEMFIRE_6_5) < 0)) {
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasDiskWriteAttributes())) {
        generate(attrs.getDiskWriteAttributes());
      }

      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasDiskDirs())) {
        File[] diskDirs = attrs.getDiskDirs();
        int[] diskSizes = attrs.getDiskDirSizes();
        if (diskDirs != null && diskDirs.length > 0) {
          handler.startElement("", DISK_DIRS, DISK_DIRS, EMPTY);
          for (int i = 0; i < diskDirs.length; i++) {
            AttributesImpl diskAtts = new AttributesImpl();
            if (diskSizes[i] != DiskStoreFactory.DEFAULT_DISK_DIR_SIZE) {
              diskAtts.addAttribute("", "", DIR_SIZE, "", String.valueOf(diskSizes[i]));
            }
            handler.startElement("", DISK_DIR, DISK_DIR, diskAtts);
            File dir = diskDirs[i];
            String name = generateDefaults() ? dir.getAbsolutePath() : dir.getPath();
            handler.characters(name.toCharArray(), 0, name.length());
            handler.endElement("", DISK_DIR, DISK_DIR);
          }
          handler.endElement("", DISK_DIRS, DISK_DIRS);
        }
      }
    } // pre 6.5

    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_0) >= 0) {
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasPartitionAttributes())) {
        PartitionAttributes p = attrs.getPartitionAttributes();
        if (p != null) {
          generate(p);
        }
      }
    }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_0) >= 0) {
      MembershipAttributes p = attrs.getMembershipAttributes();
      if (p != null && p.hasRequiredRoles()) {
        generate(p);
      }
    }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_0) >= 0) {
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasSubscriptionAttributes())) {
        SubscriptionAttributes sa = attrs.getSubscriptionAttributes();
        if (sa != null) {
          if (generateDefaults() || !sa.equals(new SubscriptionAttributes()))
            generate(sa);
        }
      }
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasCacheLoader())) {
      generate(CACHE_LOADER, attrs.getCacheLoader());
    }
    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasCacheWriter())) {
      generate(CACHE_WRITER, attrs.getCacheWriter());
    }
    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasCacheListeners())) {
      CacheListener[] listeners = attrs.getCacheListeners();
      for (CacheListener listener : listeners) {
        generate(CACHE_LISTENER, listener);
      }
    }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_8_0) >= 0) {
      if ((!(attrs instanceof RegionAttributesCreation)
          || ((RegionAttributesCreation) attrs).hasCompressor())) {
        generate(COMPRESSOR, attrs.getCompressor());
      }
    }

    if ((!(attrs instanceof RegionAttributesCreation)
        || ((RegionAttributesCreation) attrs).hasEvictionAttributes())) {
      generate(attrs.getEvictionAttributes());
    }

    handler.endElement("", REGION_ATTRIBUTES, REGION_ATTRIBUTES);
  }

  /**
   * Generates XML for a <code>CacheCallback</code>
   */
  private void generate(String kind, Object callback) throws SAXException {
    if (callback == null) {
      return;
    }

    handler.startElement("", kind, kind, EMPTY);

    String className = callback.getClass().getName();
    handler.startElement("", CLASS_NAME, CLASS_NAME, EMPTY);
    handler.characters(className.toCharArray(), 0, className.length());
    handler.endElement("", CLASS_NAME, CLASS_NAME);

    Properties props = null;
    if (callback instanceof Declarable2) {
      props = ((Declarable2) callback).getConfig();
    } else if (callback instanceof ReflectionBasedAutoSerializer) {
      props = ((ReflectionBasedAutoSerializer) callback).getConfig();
    } else if (callback instanceof Declarable && cache instanceof GemFireCacheImpl) {
      props = ((InternalCache) cache).getDeclarableProperties((Declarable) callback);
    }
    generate(props, null);

    handler.endElement("", kind, kind);
  }

  private void generate(Declarable d, Properties p) throws SAXException {
    if (d == null) {
      return;
    }

    handler.startElement("", CacheXml.INITIALIZER, CacheXml.INITIALIZER, EMPTY);

    String className = d.getClass().getName();
    handler.startElement("", CLASS_NAME, CLASS_NAME, EMPTY);
    handler.characters(className.toCharArray(), 0, className.length());
    handler.endElement("", CLASS_NAME, CLASS_NAME);

    generate(p, null);

    handler.endElement("", CacheXml.INITIALIZER, CacheXml.INITIALIZER);
  }

  private void generate(EvictionAttributes ea) throws SAXException {
    EvictionAction eAction = ea.getAction();
    if (eAction.isNone()) {
      return;
    }

    AttributesImpl atts = new AttributesImpl();
    atts.addAttribute("", "", ACTION, "", eAction.toString());

    handler.startElement("", EVICTION_ATTRIBUTES, EVICTION_ATTRIBUTES, EMPTY);
    if (ea.getAlgorithm() == EvictionAlgorithm.LRU_ENTRY) {
      atts.addAttribute("", "", MAXIMUM, "", String.valueOf(ea.getMaximum()));
      handler.startElement("", LRU_ENTRY_COUNT, LRU_ENTRY_COUNT, atts);
      handler.endElement("", LRU_ENTRY_COUNT, LRU_ENTRY_COUNT);
    } else if (ea.getAlgorithm() == EvictionAlgorithm.LRU_MEMORY) {
      atts.addAttribute("", "", MAXIMUM, "", String.valueOf(ea.getMaximum()));
      handler.startElement("", LRU_MEMORY_SIZE, LRU_MEMORY_SIZE, atts);
      ObjectSizer os = ea.getObjectSizer();
      if (os != null && os != ObjectSizer.DEFAULT) {
        generate((Declarable) os, false);
      }
      handler.endElement("", LRU_MEMORY_SIZE, LRU_MEMORY_SIZE);
    } else if (ea.getAlgorithm() == EvictionAlgorithm.LRU_HEAP) {
      handler.startElement("", LRU_HEAP_PERCENTAGE, LRU_HEAP_PERCENTAGE, atts);
      if (version.compareTo(CacheXmlVersion.GEMFIRE_6_0) >= 0) {
        ObjectSizer os = ea.getObjectSizer();
        if (!(os instanceof SizeClassOnceObjectSizer)) {
          if (os != null) {
            generate((Declarable) os, false);
          }
        }
      }
      handler.endElement("", LRU_HEAP_PERCENTAGE, LRU_HEAP_PERCENTAGE);
    }
    handler.endElement("", EVICTION_ATTRIBUTES, EVICTION_ATTRIBUTES);
  }

  /**
   * Generates XML for <code>ExpirationAttributes</code>
   */
  private void generate(String kind, ExpirationAttributes attrs, CustomExpiry custom)
      throws SAXException {

    if (attrs == null) {
      return;
    }

    handler.startElement("", kind, kind, EMPTY);

    int timeout = attrs.getTimeout();
    ExpirationAction action = attrs.getAction();
    AttributesImpl atts = new AttributesImpl();
    atts.addAttribute("", "", TIMEOUT, "", String.valueOf(timeout));

    String actionString;
    if (action.equals(ExpirationAction.DESTROY)) {
      actionString = DESTROY;

    } else if (action.equals(ExpirationAction.INVALIDATE)) {
      actionString = INVALIDATE;

    } else if (action.equals(ExpirationAction.LOCAL_DESTROY)) {
      actionString = LOCAL_DESTROY;

    } else if (action.equals(ExpirationAction.LOCAL_INVALIDATE)) {
      actionString = LOCAL_INVALIDATE;

    } else {
      throw new InternalGemFireException(
          String.format("Unknown ExpirationAction: %s", action));
    }

    atts.addAttribute("", "", ACTION, "", actionString);

    handler.startElement("", EXPIRATION_ATTRIBUTES, EXPIRATION_ATTRIBUTES, atts);
    if (custom != null) {
      AttributesImpl endAtts = new AttributesImpl();
      handler.startElement("", CUSTOM_EXPIRY, CUSTOM_EXPIRY, endAtts);
      generate(custom, false);
      handler.endElement("", CUSTOM_EXPIRY, CUSTOM_EXPIRY);
    }
    handler.endElement("", EXPIRATION_ATTRIBUTES, EXPIRATION_ATTRIBUTES);

    handler.endElement("", kind, kind);
  }

  /**
   * Generates XML for <code>SubscriptionAttributes</code>
   */
  private void generate(SubscriptionAttributes attrs) throws SAXException {
    if (attrs == null) {
      return;
    }

    String interestString;
    InterestPolicy ip = attrs.getInterestPolicy();
    AttributesImpl atts = new AttributesImpl();

    if (ip.isAll()) {
      interestString = ALL;
    } else if (ip.isCacheContent()) {
      interestString = CACHE_CONTENT;
    } else {
      throw new InternalGemFireException(
          String.format("Unknown InterestPolicy: %s", ip));
    }

    atts.addAttribute("", "", INTEREST_POLICY, "", interestString);

    handler.startElement("", SUBSCRIPTION_ATTRIBUTES, SUBSCRIPTION_ATTRIBUTES, atts);
    handler.endElement("", SUBSCRIPTION_ATTRIBUTES, SUBSCRIPTION_ATTRIBUTES);
  }

  /**
   * Generates XML for a <code>PartitionAttributes</code>
   */
  private void generate(PartitionAttributes<?, ?> pa) throws SAXException {
    AttributesImpl atts = new AttributesImpl();

    if (generateDefaults() || pa.getRedundantCopies() != 0)
      atts.addAttribute("", "", PARTITION_REDUNDANT_COPIES, "",
          String.valueOf(pa.getRedundantCopies()));

    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_1) >= 0) {
      if (generateDefaults()
          || pa.getLocalMaxMemory() != ((PartitionAttributesImpl) pa).getLocalMaxMemoryDefault())
        atts.addAttribute("", "", LOCAL_MAX_MEMORY, "", String.valueOf(pa.getLocalMaxMemory()));
      if (generateDefaults()
          || pa.getTotalMaxMemory() != PartitionAttributesFactory.GLOBAL_MAX_MEMORY_DEFAULT)
        atts.addAttribute("", "", TOTAL_MAX_MEMORY, "", String.valueOf(pa.getTotalMaxMemory()));
      if (generateDefaults()
          || pa.getTotalNumBuckets() != PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT)
        atts.addAttribute("", "", TOTAL_NUM_BUCKETS, "", String.valueOf(pa.getTotalNumBuckets()));
    } // GEMFIRE_5_1

    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_8) >= 0) {
      if (pa.getColocatedWith() != null)
        atts.addAttribute("", "", PARTITION_COLOCATED_WITH, "", pa.getColocatedWith());

    }
    if (version.compareTo(CacheXmlVersion.GEMFIRE_6_0) >= 0) {
      if (generateDefaults()
          || pa.getRecoveryDelay() != PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT)
        atts.addAttribute("", "", RECOVERY_DELAY, "", String.valueOf(pa.getRecoveryDelay()));
      if (generateDefaults() || pa
          .getStartupRecoveryDelay() != PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT)
        atts.addAttribute("", "", STARTUP_RECOVERY_DELAY, "",
            String.valueOf(pa.getStartupRecoveryDelay()));
    }

    if (!generateDefaults() && atts.getLength() == 0 && pa.getPartitionResolver() == null
        && pa.getPartitionListeners().length == 0 && (pa.getFixedPartitionAttributes() == null
            || pa.getFixedPartitionAttributes().isEmpty())) {
      return;
    }

    handler.startElement("", PARTITION_ATTRIBUTES, PARTITION_ATTRIBUTES, atts);

    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_8) >= 0) {
      PartitionResolver rr = pa.getPartitionResolver();
      if (rr != null) {
        generate(rr);
      }
    }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_6_1) >= 0) {
      PartitionListener[] listeners = pa.getPartitionListeners();
      for (PartitionListener listener : listeners) {
        if (listener != null) {
          generate(listener);
        }
      }
    }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_6_6) >= 0) {
      List<FixedPartitionAttributes> staticAttrs = pa.getFixedPartitionAttributes();
      if (staticAttrs != null) {
        generateFixedPartitionAttributes(staticAttrs);
      }
    }

    if (version.compareTo(CacheXmlVersion.GEMFIRE_5_1) < 0) {
      Properties p = pa.getLocalProperties();
      generate(p, LOCAL_PROPERTIES);

      p = pa.getGlobalProperties();
      generate(p, GLOBAL_PROPERTIES);
    }

    handler.endElement("", PARTITION_ATTRIBUTES, PARTITION_ATTRIBUTES);
  }

  /**
   * Generate XML for partition-resolver element in PartitionedRegion Attributes
   */
  private void generate(PartitionResolver rr) throws SAXException {
    if (rr == null)
      return;

    handler.startElement("", CacheXml.PARTITION_RESOLVER, CacheXml.PARTITION_RESOLVER, EMPTY);

    String className = rr.getClass().getName();

    handler.startElement("", CLASS_NAME, CLASS_NAME, EMPTY);
    handler.characters(className.toCharArray(), 0, className.length());
    handler.endElement("", CLASS_NAME, CLASS_NAME);
    Properties props;
    if (rr instanceof Declarable2) {
      props = ((Declarable2) rr).getConfig();
      generate(props, null);
    }
    handler.endElement("", CacheXml.PARTITION_RESOLVER, CacheXml.PARTITION_RESOLVER);
  }

  /**
   * Generate XML for partition-listener element in PartitionedRegion Attributes
   */
  private void generate(PartitionListener pl) throws SAXException {
    if (pl == null)
      return;

    handler.startElement("", CacheXml.PARTITION_LISTENER, CacheXml.PARTITION_LISTENER, EMPTY);

    String className = pl.getClass().getName();

    handler.startElement("", CLASS_NAME, CLASS_NAME, EMPTY);
    handler.characters(className.toCharArray(), 0, className.length());
    handler.endElement("", CLASS_NAME, CLASS_NAME);
    Properties props;
    if (pl instanceof Declarable2) {
      props = ((Declarable2) pl).getConfig();
      generate(props, null);
    }
    handler.endElement("", CacheXml.PARTITION_LISTENER, CacheXml.PARTITION_LISTENER);
  }

  /**
   * Generate XML for FixedPartitionAttribute element in PartitionedRegion Attributes
   */
  private void generateFixedPartitionAttributes(List<FixedPartitionAttributes> allStaticAttrs)
      throws SAXException {
    for (FixedPartitionAttributes attr : allStaticAttrs) {
      AttributesImpl sAtts = new AttributesImpl();
      sAtts.addAttribute("", "", PARTITION_NAME, "", attr.getPartitionName());
      sAtts.addAttribute("", "", IS_PRIMARY, "", String.valueOf(attr.isPrimary()));
      sAtts.addAttribute("", "", NUM_BUCKETS, "", String.valueOf(attr.getNumBuckets()));
      handler.startElement("", CacheXml.FIXED_PARTITION_ATTRIBUTES,
          CacheXml.FIXED_PARTITION_ATTRIBUTES, sAtts);
      handler.endElement("", CacheXml.FIXED_PARTITION_ATTRIBUTES,
          CacheXml.FIXED_PARTITION_ATTRIBUTES);
    }
  }

  /**
   * Generates XML for a <code>DiskWriteAttributes</code>
   */
  private void generate(DiskWriteAttributes dwa) throws SAXException {
    if (dwa == null) {
      // now dwa could be null in AbstractRegion if disk store is configured
      return;
    }
    long maxOplogSize = dwa.getMaxOplogSize();
    String maxOplogSizeString;
    if (maxOplogSize == DiskWriteAttributesImpl.getDefaultMaxOplogSizeLimit()) {
      maxOplogSizeString = "0";
    } else {
      maxOplogSizeString = "" + maxOplogSize;
    }
    {
      AttributesImpl atts = new AttributesImpl();
      if (dwa.isRollOplogs() != DiskWriteAttributesImpl.getDefaultRollOplogsValue()) {
        atts.addAttribute("", "", ROLL_OPLOG, "", String.valueOf(dwa.isRollOplogs()));
      }
      if (dwa.getMaxOplogSize() != DiskWriteAttributesImpl.getDefaultMaxOplogSize()) {
        atts.addAttribute("", "", MAX_OPLOG_SIZE, "", maxOplogSizeString);
      }
      handler.startElement("", DISK_WRITE_ATTRIBUTES, DISK_WRITE_ATTRIBUTES, atts);
    }
    if (dwa.isSynchronous()) {
      handler.startElement("", SYNCHRONOUS_WRITES, SYNCHRONOUS_WRITES, EMPTY);
      handler.endElement("", SYNCHRONOUS_WRITES, SYNCHRONOUS_WRITES);

    } else {
      AttributesImpl atts = new AttributesImpl();
      if (dwa.getTimeInterval() != -1) {
        atts.addAttribute("", "", TIME_INTERVAL, "", String.valueOf(dwa.getTimeInterval()));
      } else {
        atts.addAttribute("", "", TIME_INTERVAL, "", "1000");
      }
      atts.addAttribute("", "", BYTES_THRESHOLD, "", String.valueOf(dwa.getBytesThreshold()));
      handler.startElement("", ASYNCHRONOUS_WRITES, ASYNCHRONOUS_WRITES, atts);
      handler.endElement("", ASYNCHRONOUS_WRITES, ASYNCHRONOUS_WRITES);
    }

    handler.endElement("", DISK_WRITE_ATTRIBUTES, DISK_WRITE_ATTRIBUTES);
  }

  /**
   * Generates XML for a <code>MembershipAttributes</code>
   */
  private void generate(MembershipAttributes ra) throws SAXException {
    Set roles = ra.getRequiredRoles();

    String laction = ra.getLossAction().toString().toLowerCase().replace('_', '-');
    String raction = ra.getResumptionAction().toString().toLowerCase().replace('_', '-');

    AttributesImpl raAtts = new AttributesImpl();
    raAtts.addAttribute("", "", LOSS_ACTION, "", laction);
    raAtts.addAttribute("", "", RESUMPTION_ACTION, "", raction);

    handler.startElement("", MEMBERSHIP_ATTRIBUTES, MEMBERSHIP_ATTRIBUTES, raAtts);

    for (Object o : roles) {
      Role role = (Role) o;
      AttributesImpl roleAtts = new AttributesImpl();
      roleAtts.addAttribute("", "", NAME, "", role.getName());
      handler.startElement("", REQUIRED_ROLE, REQUIRED_ROLE, roleAtts);
      handler.endElement("", REQUIRED_ROLE, REQUIRED_ROLE);
    }

    handler.endElement("", MEMBERSHIP_ATTRIBUTES, MEMBERSHIP_ATTRIBUTES);
  }

  /**
   * Generates XML for a <code>String</code>
   */
  private void generate(String s) throws SAXException {
    handler.startElement("", STRING, STRING, EMPTY);
    handler.characters(s.toCharArray(), 0, s.length());
    handler.endElement("", STRING, STRING);
  }

  /**
   * Generates XML for a <code>Declarable</code>. Will handle the config <code>Properties</code> for
   * a {@link Declarable2}.
   */
  private void generate(Declarable d) throws SAXException {
    generate(d, true);
  }

  private void generate(Declarable d, boolean includeDeclarable) throws SAXException {
    if (includeDeclarable) {
      handler.startElement("", DECLARABLE, DECLARABLE, EMPTY);
    }

    String className = d.getClass().getName();
    handler.startElement("", CLASS_NAME, CLASS_NAME, EMPTY);
    handler.characters(className.toCharArray(), 0, className.length());
    handler.endElement("", CLASS_NAME, CLASS_NAME);

    if (d instanceof Declarable2) {
      Properties props = ((Declarable2) d).getConfig();
      generate(props, null);
    }

    if (includeDeclarable) {
      handler.endElement("", DECLARABLE, DECLARABLE);
    }
  }

  /**
   * Generates XML for key constraints
   *
   * @param element The kind of element to generate (KEY_CONSTRAINT or VALUE_CONSTRAINT).
   */
  private void generate(Class c, String element) throws SAXException {
    if (c != null) {
      handler.startElement("", element, element, EMPTY);
      String className = c.getName();
      handler.characters(className.toCharArray(), 0, className.length());
      handler.endElement("", element, element);
    }
  }

  /**
   * Generates XML for an arbitrary object. It special cases {@link String}s and
   * {@link Declarable}s.
   */
  private void generate(Object o) throws SAXException {
    if (o instanceof String) {
      handler.startElement("", STRING, STRING, EMPTY);
      String s = (String) o;
      handler.characters(s.toCharArray(), 0, s.length());
      handler.endElement("", STRING, STRING);

    } else if (o instanceof Declarable) {
      generate((Declarable) o);

    } else if (o == null) {
      handler.startElement("", STRING, STRING, EMPTY);
      String s = "null";
      handler.characters(s.toCharArray(), 0, s.length());
      handler.endElement("", STRING, STRING);

    } else {
      // Instead of blowing up, just put a String entry...

      handler.startElement("", STRING, STRING, EMPTY);
      String s = o.getClass().getName();
      handler.characters(s.toCharArray(), 0, s.length());
      handler.endElement("", STRING, STRING);
    }
  }

  private void generate(final Properties props, String elementName) throws SAXException {
    if (props == null || props.isEmpty()) {
      return;
    }
    if (elementName != null) {
      handler.startElement("", elementName, elementName, EMPTY);
    }
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      String name = (String) entry.getKey();
      Object value = entry.getValue();

      AttributesImpl atts = new AttributesImpl();
      atts.addAttribute("", "", NAME, "", name);

      handler.startElement("", PARAMETER, PARAMETER, atts);

      if (value instanceof String) {
        generate((String) value);

      } else if (value instanceof Declarable) {
        generate((Declarable) value);
      }

      handler.endElement("", PARAMETER, PARAMETER);

    }
    if (elementName != null) {
      handler.endElement("", elementName, elementName);
    }
  }

  private void generate(final Extensible<?> extensible) throws SAXException {
    for (final Extension<?> extension : extensible.getExtensionPoint().getExtensions()) {
      extension.getXmlGenerator().generate(this);
    }
  }

  /**
   * Keep track of the content handler for use during {@link #parse(String)}.
   */
  @Override
  public void setContentHandler(ContentHandler handler) {
    this.handler = handler;
  }

  @Override
  public ContentHandler getContentHandler() {
    return handler;
  }

  @Override
  public ErrorHandler getErrorHandler() {
    return this;
  }

  @Override
  public boolean getFeature(String name) {
    return false;
  }

  @Override
  public void setFeature(String name, boolean value) {
    // nothing
  }

  @Override
  public Object getProperty(String name) {

    return null;
  }

  @Override
  public void setProperty(String name, Object value) {
    // nothing
  }

  @Override
  public void setEntityResolver(EntityResolver resolver) {
    // nothing
  }

  @Override
  public EntityResolver getEntityResolver() {
    return this;
  }

  @Override
  public void setDTDHandler(DTDHandler handler) {
    // nothing
  }

  @Override
  public DTDHandler getDTDHandler() {
    return null;
  }

  @Override
  public void setErrorHandler(ErrorHandler handler) {
    // nothing
  }

  @Override
  public void parse(String systemId) {
    // nothing
  }

  /**
   * Used by gemfire build.xml to generate a default gemfire.properties for use by applications. See
   * bug 30995 for the feature request.
   */
  public static void main(String[] args) throws IOException {
    FileWriter fw = new FileWriter(new File("cache.xml"));
    PrintWriter pw = new PrintWriter(fw);

    generateDefault(pw);
    pw.close();
    fw.close();
  }

}
