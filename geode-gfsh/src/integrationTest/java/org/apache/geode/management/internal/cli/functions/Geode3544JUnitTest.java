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
package org.apache.geode.management.internal.cli.functions;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;

public class Geode3544JUnitTest {

  private static Cache cache;

  private static final String PARTITIONED_REGION = "emp_region";
  private static String emp_key;

  static class EmpProfile implements Serializable {
    private static final long serialVersionUID = 1L;
    private long data;

    public EmpProfile() {

    }

    public EmpProfile(long in_data) {
      this.data = in_data;
    }

    public long getData() {
      return data;
    }

    public void setData(long data) {
      this.data = data;
    }
  }
  public static class EmpData extends EmpProfile {
    private short empId;
    private Integer empNumber;
    private long empAccount;

    public EmpData() {
      super();
    }

    public EmpData(long in_data, short in_empId, Integer in_empNumber, long in_empAccount) {
      super(in_data);

      this.empId = in_empId;
      this.empNumber = in_empNumber;
      this.empAccount = in_empAccount;

    }

    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other instanceof EmpData) {
        return this.getEmpId() == (((EmpData) other).getEmpId());
      }
      return true;
    }

    @Override
    public String toString() {
      return "data:" + getData() + "," + "empId" + getEmpId();
    }

    public short getEmpId() {
      return empId;
    }

    public void setEmpId(short empId) {
      this.empId = empId;
    }

    public Integer getEmpNumber() {
      return empNumber;
    }

    public void setEmpNumber(Integer empNumber) {
      this.empNumber = empNumber;
    }

    public long getEmpAccount() {
      return empAccount;
    }

    public void setEmpAccount(long empAccount) {
      this.empAccount = empAccount;
    }

    @Override
    public int hashCode() {
      return Objects.hash(empAccount, empNumber, empId);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    RegionFactory<EmpData, String> factory = cache.createRegionFactory(RegionShortcut.PARTITION);
    Region<EmpData, String> region1 = factory.create(PARTITIONED_REGION);
    EmpData emp_data_key = new EmpData(1, (short) 1, 1, 1);
    region1.put(emp_data_key, "value_1");
    ObjectMapper mapper = new ObjectMapper();
    emp_key = mapper.writeValueAsString(emp_data_key);

  }

  @AfterClass
  public static void tearDown() {
    cache.close();
    cache = null;
  }

  /*
   * This test addresses GEODE-3544
   */
  @Test
  public void testLocateKeyIsObject() {
    DataCommandFunction dataCmdFn = new DataCommandFunction();

    DataCommandResult result = dataCmdFn.locateEntry(emp_key, EmpData.class.getName(),
        String.class.getName(), PARTITIONED_REGION, false, (InternalCache) cache);

    assertNotNull(result);
    result.aggregate(null);
    List<DataCommandResult.KeyInfo> keyInfos = result.getLocateEntryLocations();
    assertEquals(1, keyInfos.size());
  }
}
