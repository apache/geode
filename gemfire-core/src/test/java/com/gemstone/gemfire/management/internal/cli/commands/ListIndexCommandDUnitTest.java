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
package com.gemstone.gemfire.management.internal.cli.commands;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.lang.MutableIdentifiable;
import com.gemstone.gemfire.internal.lang.ObjectUtils;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.domain.IndexDetails;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * The ListIndexCommandDUnitTest class is distributed test suite of test cases for testing the index-based GemFire shell
 * (Gfsh) commands. </p>
 *
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase
 * @see com.gemstone.gemfire.management.internal.cli.commands.IndexCommands
 * @since 7.0
 */
@SuppressWarnings("unused")
public class ListIndexCommandDUnitTest extends CliCommandTestBase {

  protected static final int DEFAULT_REGION_INITIAL_CAPACITY = 10000;

  private final AtomicLong idGenerator = new AtomicLong(0l);

  protected static String toString(final Result result) {
    assert result != null : "The Result object from the command execution cannot be null!";

    final StringBuilder buffer = new StringBuilder(System.getProperty("line.separator"));

    while (result.hasNextLine()) {
      buffer.append(result.nextLine());
      buffer.append(System.getProperty("line.separator"));
    }

    return buffer.toString();
  }


  public ListIndexCommandDUnitTest(final String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    createDefaultSetup(null);
    setupGemFire();
  }

  protected Index createIndex(final String name, final String indexedExpression, final String fromClause) {
    return createIndex(name, IndexType.FUNCTIONAL, indexedExpression, fromClause);
  }

  protected Index createIndex(final String name, final IndexType type, final String indexedExpression,
      final String fromClause) {
    return new IndexAdapter(name, type, indexedExpression, fromClause);
  }

  protected Peer createPeer(final VM vm, final Properties distributedSystemProperties,
      final RegionDefinition... regions) {
    final Peer peer = new Peer(vm, distributedSystemProperties);
    peer.add(regions);
    return peer;
  }

  protected RegionDefinition createRegionDefinition(final String regionName, final Class<?> keyConstraint,
      final Class<?> valueConstraint, final Index... indexes) {
    final RegionDefinition regionDefinition = new RegionDefinition(regionName, keyConstraint, valueConstraint);
    regionDefinition.add(indexes);
    return regionDefinition;
  }

  protected void setupGemFire() throws Exception {
    final Host host = Host.getHost(0);

    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);

    final Peer peer1 = createPeer(vm1, createDistributedSystemProperties("consumerServer"),
        createRegionDefinition("consumers", Long.class, Consumer.class,
            createIndex("cidIdx", IndexType.PRIMARY_KEY, "id", "/consumers"),
            createIndex("cnameIdx", "name", "/consumers")));

    final Peer peer2 = createPeer(vm2, createDistributedSystemProperties("producerServer"),
        createRegionDefinition("producers", Long.class, Producer.class, createIndex("pidIdx", "id", "/producers")));

    createRegionWithIndexes(peer1);
    createRegionWithIndexes(peer2);

    loadConsumerData(peer1, 10000);
    loadProducerData(peer2, 10000);
  }

  protected Properties createDistributedSystemProperties(final String gemfireName) {
    final Properties distributedSystemProperties = new Properties();

    distributedSystemProperties.setProperty(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());
    distributedSystemProperties.setProperty(DistributionConfig.NAME_NAME, gemfireName);

    return distributedSystemProperties;
  }

  protected void createRegionWithIndexes(final Peer peer) {
    peer.run(new SerializableRunnable(
        String.format("Creating Regions with Indexes on GemFire peer (%1$s).", peer.getName())) {
      public void run() {
        // create the GemFire distributed system with custom configuration properties...
        getSystem(peer.getConfiguration());

        final Cache cache = getCache();
        final RegionFactory regionFactory = cache.createRegionFactory();

        for (RegionDefinition regionDefinition : peer) {
          regionFactory.setDataPolicy(DataPolicy.REPLICATE);
          regionFactory.setIndexMaintenanceSynchronous(true);
          regionFactory.setInitialCapacity(DEFAULT_REGION_INITIAL_CAPACITY);
          regionFactory.setKeyConstraint(regionDefinition.getKeyConstraint());
          regionFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
          regionFactory.setStatisticsEnabled(true);
          regionFactory.setValueConstraint(regionDefinition.getValueConstraint());

          final Region region = regionFactory.create(regionDefinition.getRegionName());
          String indexName = null;

          try {
            for (Index index : regionDefinition) {
              indexName = index.getName();
              if (IndexType.PRIMARY_KEY.equals(index.getType())) {
                cache.getQueryService().createKeyIndex(indexName, index.getIndexedExpression(), region.getFullPath());
              } else {
                cache.getQueryService().createIndex(indexName, index.getIndexedExpression(), region.getFullPath());
              }
            }
          } catch (Exception e) {
            LogWriterUtils.getLogWriter().error(
                String.format("Error occurred creating Index (%1$s) on Region (%2$s) - (%3$s)", indexName,
                    region.getFullPath(), e.getMessage()));
          }
        }
      }
    });
  }

  protected void loadConsumerData(final Peer peer, final int operationsTotal) {
    peer.run(new SerializableRunnable("Load /consumers Region with data") {
      public void run() {
        final Cache cache = getCache();
        final Region<Long, Consumer> consumerRegion = cache.getRegion("/consumers");

        final Random random = new Random(System.currentTimeMillis());
        int count = 0;

        final List<Proxy> proxies = new ArrayList<Proxy>();

        Consumer consumer;
        Proxy proxy;

        while (count++ < operationsTotal) {
          switch (CrudOperation.values()[random.nextInt(CrudOperation.values().length)]) {
            case RETRIEVE:
              if (!proxies.isEmpty()) {
                proxy = proxies.get(random.nextInt(proxies.size()));
                consumer = query(consumerRegion, "id = " + proxy.getId() + "l"); // works
                //consumer = query(consumerRegion, "Id = " + proxy.getId()); // works
                //consumer = query(consumerRegion, "id = " + proxy.getId()); // does not work
                proxy.setUnitsSnapshot(consumer.getUnits());
                break;
              }
            case UPDATE:
              if (!proxies.isEmpty()) {
                proxy = proxies.get(random.nextInt(proxies.size()));
                consumer = query(consumerRegion, "Name = " + proxy.getName());
                consumer.consume();
                break;
              }
            case CREATE:
            default:
              consumer = new Consumer(idGenerator.incrementAndGet());
              proxies.add(new Proxy(consumer));
              consumerRegion.put(consumer.getId(), consumer);
              assertTrue(consumerRegion.containsKey(consumer.getId()));
              assertTrue(consumerRegion.containsValueForKey(consumer.getId()));
              assertSame(consumer, consumerRegion.get(consumer.getId()));
          }
        }
      }
    });
  }

  protected void loadProducerData(final Peer peer, final int operationsTotal) {
    peer.run(new SerializableRunnable("Load /producers Region with data") {
      public void run() {
        final Cache cache = getCache();
        final Region<Long, Producer> producerRegion = cache.getRegion("/producers");

        final Random random = new Random(System.currentTimeMillis());
        int count = 0;

        final List<Proxy> proxies = new ArrayList<Proxy>();

        Producer producer;
        Proxy proxy;

        while (count++ < operationsTotal) {
          switch (CrudOperation.values()[random.nextInt(CrudOperation.values().length)]) {
            case RETRIEVE:
              if (!proxies.isEmpty()) {
                proxy = proxies.get(random.nextInt(proxies.size()));
                producer = query(producerRegion, "Id = " + proxy.getId());
                proxy.setUnitsSnapshot(producer.getUnits());
                break;
              }
            case UPDATE:
              if (!proxies.isEmpty()) {
                proxy = proxies.get(random.nextInt(proxies.size()));
                producer = query(producerRegion, "Id = " + proxy.getId());
                producer.produce();
                break;
              }
            case CREATE:
            default:
              producer = new Producer(idGenerator.incrementAndGet());
              proxies.add(new Proxy(producer));
              producerRegion.put(producer.getId(), producer);
              assertTrue(producerRegion.containsKey(producer.getId()));
              assertTrue(producerRegion.containsValueForKey(producer.getId()));
              assertSame(producer, producerRegion.get(producer.getId()));
          }
        }
      }
    });
  }

  @SuppressWarnings("unchecked")
  protected <T extends Comparable<T>, B extends AbstractBean<T>> B query(final Cache cache, final String queryString) {
    try {
      LogWriterUtils.getLogWriter().info(String.format("Running Query (%1$s) in GemFire...", queryString));

      final SelectResults<B> results = (SelectResults<B>) cache.getQueryService().newQuery(queryString).execute();

      LogWriterUtils.getLogWriter().info(
          String.format("Running Query (%1$s) in GemFire returned (%2$d) result(s).", queryString, results.size()));

      return (results.iterator().hasNext() ? results.iterator().next() : null);
    } catch (Exception e) {
      throw new RuntimeException(String.format("An error occurred running Query (%1$s)!", queryString), e);
    }
  }

  protected <T extends Comparable<T>, B extends AbstractBean<T>> B query(final Region<T, B> region,
      final String queryPredicate) {
    try {
      LogWriterUtils.getLogWriter().info(
          String.format("Running Query (%1$s) on Region (%2$s)...", queryPredicate, region.getFullPath()));

      final SelectResults<B> results = region.query(queryPredicate);

      LogWriterUtils.getLogWriter().info(
          String.format("Running Query (%1$s) on Region (%2$s) returned (%3$d) result(s).", queryPredicate,
              region.getFullPath(), results.size()));

      return (results.iterator().hasNext() ? results.iterator().next() : null);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("An error occurred running Query (%1$s) on Region (%2$s)!", queryPredicate,
              region.getFullPath()), e);
    }
  }

  public void testListIndex() throws Exception {
    final Result result = executeCommand(CliStrings.LIST_INDEX + " --" + CliStrings.LIST_INDEX__STATS);

    assertNotNull(result);
    LogWriterUtils.getLogWriter().info(toString(result));
    assertEquals(Result.Status.OK, result.getStatus());
  }

  protected static class Peer implements Iterable<RegionDefinition>, Serializable {

    private final Properties distributedSystemProperties;

    private final Set<RegionDefinition> regions = new HashSet<RegionDefinition>();

    private final VM vm;

    public Peer(final VM vm, final Properties distributedSystemProperties) {
      assert distributedSystemProperties != null : "The GemFire Distributed System configuration properties cannot be null!";
      this.distributedSystemProperties = distributedSystemProperties;
      this.vm = vm;
    }

    public Properties getConfiguration() {
      return this.distributedSystemProperties;
    }

    public String getName() {
      return getConfiguration().getProperty(DistributionConfig.NAME_NAME);
    }

    public VM getVm() {
      return vm;
    }

    public boolean add(final RegionDefinition... regionDefinitions) {
      return (regionDefinitions != null && regions.addAll(Arrays.asList(regionDefinitions)));
    }

    public Iterator<RegionDefinition> iterator() {
      return Collections.unmodifiableSet(regions).iterator();
    }

    public boolean remove(final RegionDefinition... regionDefinitions) {
      return (regionDefinitions != null && regions.removeAll(Arrays.asList(regionDefinitions)));
    }

    public void run(final SerializableRunnableIF runnable) {
      if (getVm() == null) {
        runnable.run();
      } else {
        getVm().invoke(runnable);
      }
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
      buffer.append(" {configuration = ").append(getConfiguration());
      buffer.append(", name = ").append(getName());
      buffer.append(", pid = ").append(getVm().getPid());
      buffer.append("}");
      return buffer.toString();
    }
  }

  protected static class IndexAdapter implements Index, Serializable {

    private final IndexDetails.IndexType type;

    private final String fromClause;
    private final String indexedExpression;
    private final String name;

    protected IndexAdapter(final String name, final String indexedExpression, final String fromClause) {
      this(name, IndexType.FUNCTIONAL, indexedExpression, fromClause);
    }

    protected IndexAdapter(final String name, final IndexType type, final String indexedExpression,
        final String fromClause) {
      assert name != null : "The name of the Index cannot be null!";
      assert indexedExpression != null : String.format("The expression to index for Index (%1$s) cannot be null!",
          name);
      assert fromClause != null : String.format("The from clause for Index (%1$s) cannot be null!", name);

      this.type = ObjectUtils.defaultIfNull(IndexDetails.IndexType.valueOf(type), IndexDetails.IndexType.FUNCTIONAL);
      this.name = name;
      this.indexedExpression = indexedExpression;
      this.fromClause = fromClause;
    }

    public String getName() {
      return this.name;
    }

    public String getFromClause() {
      return this.fromClause;
    }

    public String getCanonicalizedFromClause() {
      return this.fromClause;
    }

    public String getIndexedExpression() {
      return this.indexedExpression;
    }

    public String getCanonicalizedIndexedExpression() {
      return this.indexedExpression;
    }

    public String getProjectionAttributes() {
      throw new UnsupportedOperationException("Not Implemented!");
    }

    public String getCanonicalizedProjectionAttributes() {
      throw new UnsupportedOperationException("Not Implemented!");
    }

    public Region<?, ?> getRegion() {
      throw new UnsupportedOperationException("Not Implemented!");
    }

    public IndexStatistics getStatistics() {
      throw new UnsupportedOperationException("Not Implemented!");
    }

    public IndexType getType() {
      return type.getType();
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
      buffer.append(" {indexName = ").append(getName());
      buffer.append(", indexType = ").append(getType());
      buffer.append(", indexedExpression = ").append(getIndexedExpression());
      buffer.append(", fromClause = ").append(getFromClause());
      buffer.append("}");
      return buffer.toString();
    }
  }

  protected static class RegionDefinition implements Iterable<Index>, Serializable {

    private final Class<?> keyConstraint;
    private final Class<?> valueConstraint;

    private final Set<Index> indexes = new HashSet<Index>();

    private final String regionName;

    @SuppressWarnings("unchecked")
    protected RegionDefinition(final String regionName, final Class<?> keyConstraint, final Class<?> valueConstraint) {
      assert !StringUtils.isBlank(regionName) : "The name of the Region must be specified!";
      this.regionName = regionName;
      this.keyConstraint = ObjectUtils.defaultIfNull(keyConstraint, Object.class);
      this.valueConstraint = ObjectUtils.defaultIfNull(valueConstraint, Object.class);
    }

    public String getRegionName() {
      return regionName;
    }

    public Class<?> getKeyConstraint() {
      return keyConstraint;
    }

    public Class<?> getValueConstraint() {
      return valueConstraint;
    }

    public boolean add(final Index... indexes) {
      return (indexes != null && this.indexes.addAll(Arrays.asList(indexes)));
    }

    public Iterator<Index> iterator() {
      return Collections.unmodifiableSet(indexes).iterator();
    }

    public boolean remove(final Index... indexes) {
      return (indexes != null && this.indexes.removeAll(Arrays.asList(indexes)));
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof RegionDefinition)) {
        return false;
      }

      final RegionDefinition that = (RegionDefinition) obj;

      return ObjectUtils.equals(getRegionName(), that.getRegionName());
    }

    @Override
    public int hashCode() {
      int hashValue = 17;
      hashValue = 37 * hashValue + ObjectUtils.hashCode(getRegionName());
      return hashValue;
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
      buffer.append(" {regionName = ").append(getRegionName());
      buffer.append(", keyConstraint = ").append(getKeyConstraint());
      buffer.append(", valueConstraint = ").append(getValueConstraint());
      buffer.append("}");
      return buffer.toString();
    }
  }

  protected static abstract class AbstractBean<T extends Comparable<T>> implements MutableIdentifiable<T>, Serializable {

    private T id;
    private String name;

    public AbstractBean() {
    }

    public AbstractBean(final T id) {
      this.id = id;
    }

    public T getId() {
      return id;
    }

    public void setId(final T id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(final String name) {
      this.name = name;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(getClass().isInstance(obj))) {
        return false;
      }

      final AbstractBean bean = (AbstractBean) obj;

      return ObjectUtils.equals(getId(), bean.getId());
    }

    @Override
    public int hashCode() {
      int hashValue = 17;
      hashValue = 37 * hashValue + ObjectUtils.hashCode(getId());
      return hashValue;
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
      buffer.append(" {id = ").append(getId());
      buffer.append(", name = ").append(getName());
      buffer.append("}");
      return buffer.toString();
    }
  }

  public static class Consumer extends AbstractBean<Long> {

    private volatile int units;

    public Consumer() {
    }

    public Consumer(final Long id) {
      super(id);
    }

    public int getUnits() {
      return units;
    }

    public int consume() {
      return ++units;
    }
  }

  public static class Producer extends AbstractBean<Long> {

    private volatile int units;

    public Producer() {
    }

    public Producer(final Long id) {
      super(id);
    }

    public int getUnits() {
      return units;
    }

    public int produce() {
      return ++units;
    }
  }

  public static class Proxy extends AbstractBean<Long> {

    private final AbstractBean<Long> bean;
    private int unitsSnapshot;

    public Proxy(final AbstractBean<Long> bean) {
      assert bean != null : "The bean to proxy cannot be null!";
      this.bean = bean;
    }

    public AbstractBean<Long> getBean() {
      return bean;
    }

    @Override
    public Long getId() {
      return getBean().getId();
    }

    @Override
    public String getName() {
      return getBean().getName();
    }

    public int getUnitsSnapshot() {
      return unitsSnapshot;
    }

    public void setUnitsSnapshot(final int unitsSnapshot) {
      this.unitsSnapshot = unitsSnapshot;
    }
  }

  protected static enum CrudOperation {
    CREATE,
    RETRIEVE,
    UPDATE,
    DELETE
  }
}
