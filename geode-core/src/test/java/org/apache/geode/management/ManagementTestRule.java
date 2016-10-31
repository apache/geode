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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Host.*;
import static org.assertj.core.api.Assertions.*;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.rules.serializable.SerializableStatement;

/**
 * Note: Overriding MethodRule is only way to get {@code Object target}
 */
@SuppressWarnings("unused")
public class ManagementTestRule implements MethodRule, Serializable {

  public static Builder builder() {
    return new Builder();
  }

  private final int numberOfManagers;
  private final int numberOfMembers;
  private final boolean start;
  private final boolean defineManagersFirst;
  private final boolean defineManagers;
  private final boolean defineMembers;

  private JUnit4CacheTestCase helper;

  private VM[] managers;
  private VM[] members;

  protected ManagementTestRule(final Builder builder) {
    this.helper = new JUnit4CacheTestCase() {};
    this.numberOfManagers = builder.numberOfManagers;
    this.numberOfMembers = builder.numberOfMembers;
    this.start = builder.start;
    this.defineManagersFirst = builder.defineManagersFirst;
    this.defineManagers = builder.defineManagers;
    this.defineMembers = builder.defineMembers;
  }

  @Override
  public Statement apply(final Statement base, final FrameworkMethod method, final Object target) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        setUp(target);
        try {
          base.evaluate();
        } finally {
          tearDown();
        }
      }
    };
  }

  public DistributedMember getDistributedMember() {
    return getCache().getDistributedSystem().getDistributedMember();
  }

  public DistributedMember getDistributedMember(final VM vm) {
    return vm.invoke("getDistributedMember", () -> getDistributedMember());
  }

  public void createManagers() {
    for (VM manager : this.managers) {
      manager.invoke(() -> createManager(true));
    }
  }

  public void createMembers() {
    for (VM member : this.members) {
      member.invoke(() -> createMember());
    }
  }

  public void createManager() {
    createManager(true);
  }

  public void createManager(final Properties properties) {
    createManager(properties, true);
  }

  public void createManager(final boolean start) {
    createManager(new Properties(), start);
  }

  public void createManager(final Properties properties, final boolean start) {
    setPropertyIfNotSet(properties, JMX_MANAGER, "true");
    setPropertyIfNotSet(properties, JMX_MANAGER_START, "false");
    setPropertyIfNotSet(properties, JMX_MANAGER_PORT, "0");
    setPropertyIfNotSet(properties, HTTP_SERVICE_PORT, "0");
    setPropertyIfNotSet(properties, ENABLE_TIME_STATISTICS, "true");
    setPropertyIfNotSet(properties, STATISTIC_SAMPLING_ENABLED, "true");

    this.helper.getCache(properties);

    if (start) {
      startManager();
    }
  }

  public void createManager(final VM managerVM) {
    managerVM.invoke("createManager", () -> createManager());
  }

  public void createManager(final VM managerVM, final boolean start) {
    managerVM.invoke("createManager", () -> createManager(start));
  }

  public void createManager(final VM managerVM, final Properties properties) {
    managerVM.invoke("createManager", () -> createManager(properties, true));
  }

  public void createManager(final VM managerVM, final Properties properties, final boolean start) {
    managerVM.invoke("createManager", () -> createManager(properties, start));
  }

  public void createMember() {
    createMember(new Properties());
  }

  public void createMember(final Properties properties) {
    setPropertyIfNotSet(properties, JMX_MANAGER, "false");
    setPropertyIfNotSet(properties, ENABLE_TIME_STATISTICS, "true");
    setPropertyIfNotSet(properties, STATISTIC_SAMPLING_ENABLED, "true");

    this.helper.getCache(properties);
  }

  public void createMember(final VM memberVM) {
    Properties properties = new Properties();
    properties.setProperty(NAME, "memberVM-" + memberVM.getPid());
    memberVM.invoke("createMember", () -> createMember(properties));
  }

  public void createMember(final VM memberVM, final Properties properties) throws Exception {
    memberVM.invoke("createMember", () -> createMember(properties));
  }

  public Cache getCache() {
    return this.helper.getCache();
  }

  public ClientCache getClientCache() {
    return this.helper.getClientCache(new ClientCacheFactory());
  }

  public boolean hasCache() {
    return this.helper.hasCache();
  }

  public Cache basicGetCache() {
    return this.helper.basicGetCache();
  }

  public ManagementService getManagementService() {
    assertThat(hasCache()).isTrue();
    return ManagementService.getManagementService(basicGetCache());
  }

  public SystemManagementService getSystemManagementService() {
    assertThat(hasCache()).isTrue();
    return (SystemManagementService) ManagementService.getManagementService(basicGetCache());
  }

  public ManagementService getExistingManagementService() {
    assertThat(hasCache()).isTrue();
    return ManagementService.getExistingManagementService(basicGetCache());
  }

  public void startManager() {
    SystemManagementService service = getSystemManagementService();
    service.createManager();
    service.startManager();
  }

  public void startManager(final VM managerVM) {
    managerVM.invoke("startManager", () -> startManager());
  }

  public void stopManager() {
    if (getManagementService().isManager()) {
      getManagementService().stopManager();
    }
  }

  public void stopManager(final VM managerVM) {
    managerVM.invoke("stopManager", () -> stopManager());
  }

  public Set<DistributedMember> getOtherNormalMembers() {
    Set<DistributedMember> allMembers = new HashSet<>(getAllNormalMembers());
    allMembers.remove(getDistributedMember());
    return allMembers;
  }

  public Set<DistributedMember> getAllNormalMembers() {
    return getDistributionManager().getNormalDistributionManagerIds(); // excludes LOCATOR_DM_TYPE
  }

  public void disconnectAllFromDS() {
    stopManagerQuietly();
    Invoke.invokeInEveryVM("stopManager", () -> stopManagerQuietly());
    JUnit4DistributedTestCase.disconnectFromDS();
    Invoke.invokeInEveryVM("disconnectFromDS", () -> JUnit4DistributedTestCase.disconnectFromDS());
  }

  private DM getDistributionManager() {
    return ((GemFireCacheImpl) getCache()).getDistributionManager();
  }

  private void setPropertyIfNotSet(final Properties properties, final String key,
      final String value) {
    if (!properties.containsKey(key)) {
      properties.setProperty(key, value);
    }
  }

  private void stopManagerQuietly() {
    try {
      if (hasCache() && !basicGetCache().isClosed()) {
        stopManager();
      }
    } catch (DistributedSystemDisconnectedException | NullPointerException ignore) {
    }
  }

  private void setUp(final Object target) throws Exception {
    DUnitLauncher.launchIfNeeded();
    JUnit4DistributedTestCase.disconnectAllFromDS();

    int whichVM = 0;

    this.managers = new VM[this.numberOfManagers];
    for (int i = 0; i < this.numberOfManagers; i++) {
      this.managers[i] = getHost(0).getVM(whichVM);
      whichVM++;
    }

    this.members = new VM[this.numberOfMembers];
    for (int i = 0; i < this.numberOfMembers; i++) {
      this.members[i] = getHost(0).getVM(whichVM);
      whichVM++;
    }

    if (this.start) {
      start();
    }

    processAnnotations(target);
  }

  private void start() {
    if (this.defineManagers && this.defineManagersFirst) {
      createManagers();
    }
    if (this.defineMembers) {
      createMembers();
    }
    if (this.defineManagers && !this.defineManagersFirst) {
      createManagers();
    }
  }

  private void tearDown() throws Exception {
    JUnit4DistributedTestCase.disconnectAllFromDS();
  }

  private void processAnnotations(final Object target) {
    try {
      Class<?> clazz = target.getClass();

      Field[] fields = clazz.getDeclaredFields();
      for (Field field : fields) {
        boolean alreadyAssigned = false;
        for (Annotation annotation : field.getAnnotations()) {
          if (annotation.annotationType().equals(Manager.class)) {
            // annotated with @Manager
            throwIfAlreadyAssigned(field, alreadyAssigned);
            assignManagerField(target, field);
            alreadyAssigned = true;
          }
          if (annotation.annotationType().equals(Member.class)) {
            // annotated with @Manager
            throwIfAlreadyAssigned(field, alreadyAssigned);
            assignMemberField(target, field);
            alreadyAssigned = true;
          }
        }
      }
    } catch (IllegalAccessException e) {
      throw new Error(e);
    }
  }

  private void throwIfAlreadyAssigned(final Field field, final boolean alreadyAssigned) {
    if (alreadyAssigned) {
      throw new IllegalStateException(
          "Field " + field.getName() + " is already annotated with " + field.getAnnotations());
    }
  }

  private void assignManagerField(final Object target, final Field field)
      throws IllegalAccessException {
    throwIfNotSameType(field, VM.class);

    field.setAccessible(true);
    if (field.getType().isArray()) {
      field.set(target, this.managers);
    } else {
      field.set(target, this.managers[0]);
    }
  }

  private void assignMemberField(final Object target, final Field field)
      throws IllegalAccessException {
    throwIfNotSameType(field, VM.class);

    field.setAccessible(true);
    if (field.getType().isArray()) {
      field.set(target, this.members);
    } else {
      field.set(target, this.members[0]);
    }
  }

  private void throwIfNotSameType(final Field field, final Class clazz) {
    if (!field.getType().equals(clazz) && // non-array
        !field.getType().getComponentType().equals(clazz)) { // array
      throw new IllegalArgumentException(
          "Field " + field.getName() + " is not same type as " + clazz.getName());
    }
  }

  /**
   * Configures and builds a ManagementTestRule
   */
  public static class Builder {

    private boolean start = false;

    private boolean defineManagers = true;

    private boolean defineMembers = true;

    private int numberOfManagers = 1;

    private int numberOfMembers = 3;

    private boolean defineManagersFirst = true;

    private Builder() {}

    /**
     * Define VMs annotated with {@literal @}Manager as Managers. Default is true.
     */
    public Builder defineManagers(final boolean value) {
      this.defineManagers = value;
      return this;
    }

    /**
     * Define VMs annotated with {@literal @}Manager as Members. Default is true.
     */
    public Builder defineMembers(final boolean value) {
      this.defineMembers = value;
      return this;
    }

    /**
     * Number of Manager(s) to define. Default is 1.
     */
    public Builder numberOfManagers(final int count) {
      this.numberOfManagers = count;
      return this;
    }

    /**
     * Number of Member(s) to define. Default is 3.
     */
    public Builder numberOfMembers(final int count) {
      this.numberOfMembers = count;
      return this;
    }

    /**
     * Define Manager(s) to DUnit VMs before Member(s). Default is true.
     */
    public Builder defineManagersFirst(final boolean value) {
      this.defineManagersFirst = value;
      return this;
    }

    /**
     * Start Manager(s) and Member(s) before tests. Default is true.
     */
    public Builder start(final boolean value) {
      this.start = value;
      return this;
    }

    public ManagementTestRule build() {
      return new ManagementTestRule(this);
    }
  }
}
