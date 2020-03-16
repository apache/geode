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

import static org.apache.geode.management.internal.cli.domain.DataCommandResult.MISSING_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({GfshTest.class})
public class DataCommandFunctionWithPDXJUnitTest {
  private static final String PARTITIONED_REGION = "part_region";

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withPDXPersistent()
      .withPDXReadSerialized().withRegion(RegionShortcut.PARTITION, PARTITIONED_REGION);

  private Customer alice;
  private Customer bob;
  private CustomerWithPhone charlie;
  private CustomerWithPhone dan;
  private InternalCache cache;

  @Before
  public void setup() {
    alice = new Customer("0", "Alice", "Anderson");
    bob = new Customer("1", "Bob", "Bailey");
    charlie = new CustomerWithPhone("2", "Charlie", "Chaplin", "(222) 222-2222");
    dan = new CustomerWithPhone("3", "Dan", "Dickinson", "(333) 333-3333");

    cache = server.getCache();
    Region<Integer, Customer> region = cache.getRegion(PARTITIONED_REGION);
    region.put(0, alice);
    region.put(1, bob);
    region.put(2, charlie);
    region.put(3, dan);
  }

  // GEODE-2662: building a table where early values are missing keys causes the data to shift
  // upward during reporting.
  @Test
  public void testTableIsRectangular() {
    TabularResultModel rawTable = getTableFromQuery("select * from /" + PARTITIONED_REGION);
    // Verify any table built
    Map<String, List<String>> content = rawTable.getContent();
    assertThat(content).isNotEmpty();

    // Verify table is rectangular
    SoftAssertions softly = new SoftAssertions();
    for (String k : new String[] {"id", "phone", "firstName", "lastName"}) {
      softly.assertThat(content.get(k).size()).isEqualTo(4);
    }
    softly.assertAll();
  }

  @Test
  public void testVerifyDataDoesNotShift() {
    TabularResultModel rawTable =
        getTableFromQuery("select * from /" + PARTITIONED_REGION + " order by id");
    // Verify any table built
    Map<String, List<String>> content = rawTable.getContent();
    assertThat(content).isNotEmpty();

    // Table only contains correct keys
    assertThat(content.get("missingKey")).isNull();

    // Table contains correct data
    assertThatRowIsBuiltCorrectly(content, 0, alice);
    assertThatRowIsBuiltCorrectly(content, 1, bob);
    assertThatRowIsBuiltCorrectly(content, 2, charlie);
    assertThatRowIsBuiltCorrectly(content, 3, dan);
  }

  @Test
  public void testFilteredQueryWithPhone() {
    TabularResultModel rawTable = getTableFromQuery(
        "select * from /" + PARTITIONED_REGION + " c where IS_DEFINED ( c.phone ) order by id");
    Map<String, List<String>> content = rawTable.getContent();
    assertThat(content).isNotEmpty();

    for (String k : new String[] {"id", "phone", "firstName", "lastName"}) {
      assertThat(content.get(k).size()).isEqualTo(2);
    }
    assertThatRowIsBuiltCorrectly(content, 0, charlie);
    assertThatRowIsBuiltCorrectly(content, 1, dan);
  }

  @Test
  public void testFilteredQueryWithoutPhone() {
    TabularResultModel rawTable = getTableFromQuery(
        "select * from /" + PARTITIONED_REGION + " c where IS_UNDEFINED ( c.phone ) order by id");
    Map<String, List<String>> content = rawTable.getContent();
    assertThat(content).isNotNull();
    for (String k : new String[] {"id", "firstName", "lastName"}) {
      assertThat(content.get(k).size()).isEqualTo(2);
    }
    assertThatRowIsBuiltCorrectly(content, 0, alice);
    assertThatRowIsBuiltCorrectly(content, 1, bob);
  }

  private TabularResultModel getTableFromQuery(String query) {
    DataCommandRequest request = new DataCommandRequest();
    request.setQuery(query);
    DataCommandResult result = new DataCommandFunction().select(request, cache);
    ResultModel m = result.toSelectCommandResult();
    return m.getTableSection(DataCommandResult.QUERY_SECTION);
  }

  private void assertThatRowIsBuiltCorrectly(Map<String, List<String>> table, int rowNum,
      Customer customer) {
    SoftAssertions softly = new SoftAssertions();
    String id = table.get("id").get(rowNum);
    String firstName = table.get("firstName").get(rowNum);
    String lastName = table.get("lastName").get(rowNum);

    softly.assertThat(id).describedAs("Customer ID").isEqualTo(customer.id);
    softly.assertThat(firstName).describedAs("First name").isEqualTo(customer.firstName);
    softly.assertThat(lastName).describedAs("Last name").isEqualTo(customer.lastName);

    List<String> phoneArray = table.get("phone");

    if (phoneArray == null) {
      softly.assertThat(customer).describedAs("No phone data")
          .isNotInstanceOf(CustomerWithPhone.class);
    } else {
      String phone = phoneArray.get(rowNum);

      if (customer instanceof CustomerWithPhone) {
        softly.assertThat(phone).describedAs("Phone")
            .isEqualTo(((CustomerWithPhone) customer).phone);
      } else {
        softly.assertThat(phone).describedAs("Phone (missing)").isEqualTo(MISSING_VALUE);
      }
    }
    softly.assertAll();
  }

  public static class Customer implements PdxSerializable {
    protected String id;
    protected String firstName;
    protected String lastName;

    Customer() {}

    Customer(String id, String firstName, String lastName) {
      this.id = id;
      this.firstName = firstName;
      this.lastName = lastName;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("id", id).markIdentityField("id").writeString("firstName", firstName)
          .writeString("lastName", lastName);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("id");
      firstName = reader.readString("firstName");
      lastName = reader.readString("lastName");
    }
  }

  public static class CustomerWithPhone extends Customer {
    private String phone;

    CustomerWithPhone(String id, String firstName, String lastName, String phone) {
      this.id = id;
      this.firstName = firstName;
      this.lastName = lastName;
      this.phone = phone;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("id", id).markIdentityField("id").writeString("firstName", firstName)
          .writeString("lastName", lastName).writeString("phone", phone);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("id");
      firstName = reader.readString("firstName");
      lastName = reader.readString("lastName");
      phone = reader.readString("phone");
    }
  }
}
