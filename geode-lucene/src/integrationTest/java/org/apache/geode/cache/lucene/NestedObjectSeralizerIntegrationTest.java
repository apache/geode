/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.test.Customer;
import org.apache.geode.cache.lucene.test.Page;
import org.apache.geode.cache.lucene.test.Person;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class NestedObjectSeralizerIntegrationTest extends LuceneIntegrationTest {

  protected static int WAIT_FOR_FLUSH_TIME = 10000;
  protected static final Logger logger = LogService.getLogger();
  LuceneQuery<Integer, Object> query;
  PageableLuceneQueryResults<Integer, Object> results;

  protected CustomerFactory getCustomerFactory() {
    return Customer::new;
  }

  protected interface CustomerFactory {
    Customer create(String name, Collection<String> phoneNumbers,
        Collection<Person> contacts,
        Page[] myHomePages);
  }

  protected Region createRegionAndIndex() {
    luceneService.createIndexFactory().setLuceneSerializer(new FlatFormatSerializer())
        .addField("name").addField("phoneNumbers").addField("myHomePages.content")
        .addField("contacts.name").addField("contacts.email", new KeywordAnalyzer())
        .addField("contacts.phoneNumbers").addField("contacts.address")
        .addField("contacts.homepage.content").addField("contacts.homepage.id")
        .addField(LuceneService.REGION_VALUE_FIELD).create(INDEX_NAME, REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    return region;
  }

  protected Region createRegionAndIndexOnInvalidFields() {
    luceneService.createIndexFactory().setLuceneSerializer(new FlatFormatSerializer())
        .addField("name").addField("contacts").addField("contacts.page")
        .addField("contacts.missing").addField("missing2").create(INDEX_NAME, REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    return region;
  }

  protected void feedSomeNestedObjects(Region region) throws InterruptedException {
    Person contact1 = new Person("Tommi Jackson", new String[] {"5036330001", "5036330002"}, 1);
    Person contact2 = new Person("Tommi2 Skywalker", new String[] {"5036330003", "5036330004"}, 2);
    HashSet<Person> contacts1 = new HashSet();
    contacts1.add(contact1);
    contacts1.add(contact2);
    Page[] myHomePages1 = new Page[] {new Page(131), new Page(132)};
    ArrayList<String> phoneNumbers = new ArrayList();
    phoneNumbers.add("5035330001");
    phoneNumbers.add("5035330002");
    Customer customer13 =
        getCustomerFactory().create("Tommy Jackson", phoneNumbers, contacts1, myHomePages1);
    region.put("object-13", customer13);

    Person contact3 = new Person("Johnni Jackson", new String[] {"5036330005", "5036330006"}, 3);
    Person contact4 = new Person("Jackson Skywalker", new String[] {"5036330007", "5036330008"}, 4);
    ArrayList<Person> contacts2 = new ArrayList();
    contacts2.add(contact3);
    contacts2.add(contact4);
    phoneNumbers = new ArrayList();
    phoneNumbers.add("5035330003");
    phoneNumbers.add("5035330004");
    Page[] myHomePages2 = new Page[] {new Page(14), new Page(141)};

    Customer customer14 =
        getCustomerFactory().create("Johnny Jackson", phoneNumbers, contacts2, myHomePages2);
    region.put("object-14", customer14);

    Person contact5 = new Person("Johnni Jackson2", new String[] {"5036330009", "5036330010"}, 5);
    Person contact6 =
        new Person("Jackson2 Skywalker", new String[] {"5036330011", "5036330012"}, 6);
    ArrayList<Person> contacts3 = new ArrayList();
    contacts3.add(contact5);
    contacts3.add(contact6);
    phoneNumbers = new ArrayList();
    phoneNumbers.add("5035330005");
    phoneNumbers.add("5035330006");
    Page[] myHomePages3 = new Page[] {new Page(15), new Page(151)};

    Customer customer15 =
        getCustomerFactory().create("Johnny Jackson2", phoneNumbers, contacts3, myHomePages3);
    region.put("object-15", customer15);

    Person contact7 = new Person("Johnni Jackson21", new String[] {"5036330013", "5036330014"}, 7);
    Person contact8 =
        new Person("Jackson21 Skywalker", new String[] {"5036330015", "5036330016"}, 8);
    ArrayList<Person> contacts4 = new ArrayList();
    contacts4.add(contact7);
    contacts4.add(contact8);
    phoneNumbers = new ArrayList();
    phoneNumbers.add("5035330007");
    phoneNumbers.add("5035330008");
    Page[] myHomePages4 = new Page[] {new Page(16), new Page(161)};

    Customer customer16 =
        getCustomerFactory().create("Johnny Jackson21", phoneNumbers, contacts4, myHomePages4);
    region.put("object-16", customer16);

    region.put("key-1", "region value 1");
    region.put("key-2", "region value 2");
    region.put("key-3", "region value 3");
    region.put("key-4", "region value 4");

    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);
  }

  @Test
  public void queryOnTopLevelObjectCollectionField_AND_BothExist()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "131 AND 132",
        "myHomePages.content");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnTopLevelObjectCollectionField_AND_OneExist()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "131 AND 133",
        "myHomePages.content");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnTopLevelObjectCollectionField_OR_OneExist()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "131 OR 133",
        "myHomePages.content");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnTopLevelStringCollectionField_AND_BothExist()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "5035330007 AND 5035330008", "phoneNumbers");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnTopLevelStringCollectionField_AND_OneExist()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);
    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "5035330007 AND 5035330009", "phoneNumbers");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnTopLevelStringCollectionField_OR_OneExist()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "5035330007 OR 5035330009", "phoneNumbers");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnContactNameWithExpression() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "contacts.name:jackson2*", "name");
    results = query.findPages();
    assertEquals(2, results.size());
    printResults(results);
  }

  @Test
  public void queryOnContactNameWithExactMath() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "\"Johnni Jackson\"", "contacts.name");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnNameWithWrongValue() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "\"Johnni Jackson\"", "name");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnNameWithExactMatch() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "\"Johnny Jackson\"", "name");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnContactEmailWithAnalyzer() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "Johnni.Jackson2@pivotal.io", "contacts.email");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnNonExistEmailField() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "Johnni.Jackson2@pivotal.io", "email");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnContactAddressWithStandardAnalyzer()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "97006",
        "contacts.address");
    results = query.findPages();
    assertEquals(4, results.size());
    printResults(results);
  }

  @Test
  public void queryOnNonExistAddressField() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "97006",
        "address");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnThreeLayerField() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "contacts.homepage.content:Hello", "name");
    results = query.findPages();
    printResults(results);
    assertEquals(4, results.size());
  }

  @Test
  public void queryOnThirdLayerFieldDirectlyShouldNotGetResult()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "Hello",
        "content");
    results = query.findPages();
    printResults(results);
  }

  @Test
  public void queryOnRegionValueField() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "region",
        LuceneService.REGION_VALUE_FIELD);
    results = query.findPages();
    printResults(results);
    assertEquals(4, results.size());
  }

  @Test
  public void nonExistFieldsShouldBeIgnored() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "Jackson2*", "name");
    PageableLuceneQueryResults<Integer, Object> results = query.findPages();
    assertEquals(2, results.size());
    printResults(results);
  }

  @Test
  public void queryOnNotIndexedFieldShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "\"Johnni Jackson\"", "contacts.name");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryWithExactMatchWhileIndexOnSomeWrongFields()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "\"Johnny Jackson\"", "name");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnNotIndexedFieldWithAnalyzerShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "Johnni.Jackson2@pivotal.io", "contacts.email");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnNotIndexedContactAddressFieldShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "97006",
        "contacts.address");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnNotIndexedThreeLayerFieldShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "contacts.homepage.content:Hello", "name");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnNotExistSecondLevelFieldShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "*",
        "contacts.missing");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnNotExistTopLevelFieldShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query =
        luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "*", "missing2");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  protected void printResults(PageableLuceneQueryResults<Integer, Object> results) {
    if (results.size() > 0) {
      while (results.hasNext()) {
        results.next().stream().forEach(struct -> {
          if (logger.isDebugEnabled()) {
            logger.debug("Result is:" + struct.getValue());
          }
        });
      }
    }
  }

  protected Region createRegionAndIndexForPdxObject() {
    luceneService.createIndexFactory().setLuceneSerializer(new FlatFormatSerializer())
        .addField("ID").addField("description").addField("status").addField("names")
        .addField("position1.country").addField("position1.secId").addField("positions.secId")
        .addField("positions.country").create(INDEX_NAME, REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    return region;
  }

  protected void feedSomePdxObjects(Region region) throws InterruptedException {
    SimplePortfolioPdx.resetCounter();
    SimplePositionPdx.resetCounter();
    for (int i = 1; i < 10; i++) {
      SimplePortfolioPdx pdx = new SimplePortfolioPdx(i);
      region.put("object-" + i, pdx);
    }

    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);
  }

  @Test
  public void queryOnTopLevelPdxField() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexForPdxObject();
    feedSomePdxObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "active",
        "status");
    results = query.findPages();
    // even id number: active status; odd id number: inactive status
    assertEquals(4, results.size());
    printResults(results);
  }

  @Test
  public void queryOnTopLevelPdxArrayField() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexForPdxObject();
    feedSomePdxObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "bbb AND ccc",
        "names");
    results = query.findPages();
    // all the entries should be found
    assertEquals(9, results.size());
    printResults(results);
  }

  @Test
  public void queryOnSecondLevelPdxCollectionField()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexForPdxObject();
    feedSomePdxObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "NOVL",
        "positions.secId");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnSecondLevelPdxField() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexForPdxObject();
    feedSomePdxObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "DELL",
        "position1.secId");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void insertAndQueryJSONObject() throws InterruptedException, LuceneQueryException {
    SimplePortfolioPdx.resetCounter();
    SimplePositionPdx.resetCounter();
    Region region = createRegionAndIndexForPdxObject();

    String jsonCustomer = "{" + "\"ID\" : 3," + "\"position1\" : {" + "\"country\" : \"USA\","
        + "\"secId\" : \"DELL\"," + "\"sharesOutstanding\" : 9000.0," + "\"pid\" : 9,"
        + "\"portfolioId\" : 0" + "}," + "\"positions\" : [ {" + "\"country\" : \"USA\","
        + "\"secId\" : \"NOVL\"," + "\"sharesOutstanding\" : 11000.0," + "\"pid\" : 11,"
        + "\"portfolioId\" : 0" + "}," + "{" + "\"country\" : \"USA\"," + "\"secId\" : \"RHAT\","
        + "\"sharesOutstanding\" : 10000.0," + "\"pid\" : 10," + "\"portfolioId\" : 0" + "} ],"
        + "\"status\" : \"inactive\"," + "\"names\" : [ \"aaa\", \"bbb\", \"ccc\", \"ddd\" ],"
        + "\"description\" : \"XXXX\"," + "\"createTime\" : 0" + "}";
    region.put("jsondoc1", JSONFormatter.fromJSON(jsonCustomer));
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);
    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "NOVL",
        "positions.secId");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  public static class SimplePortfolioPdx implements Serializable, PdxSerializable {
    private int ID;
    public String description;
    public long createTime;
    public String status;
    public String[] names = {"aaa", "bbb", "ccc", "ddd"};
    public int[] intArr = {2001, 2017};

    public SimplePositionPdx position1;
    public HashSet positions = new HashSet();

    public static int numInstance = 0;

    /*
     * public String getStatus(){ return status;
     */
    public int getID() {
      return ID;
    }

    public long getCreateTime() {
      return createTime;
    }

    public void setCreateTime(long time) {
      createTime = time;
    }

    public HashSet getPositions() {
      return positions;
    }

    public SimplePositionPdx getP1() {
      return position1;
    }

    public boolean isActive() {
      return status.equals("active");
    }

    public static String[] secIds = {"SUN", "IBM", "YHOO", "GOOG", "MSFT", "AOL", "APPL", "ORCL",
        "SAP", "DELL", "RHAT", "NOVL", "HP"};

    /* public no-arg constructor required for Deserializable */
    public SimplePortfolioPdx() {
      numInstance++;
    }

    public SimplePortfolioPdx(int i) {
      numInstance++;
      ID = i;
      if (i % 2 == 0) {
        description = "YYYY";
      } else {
        description = "XXXX";
      }
      status = i % 2 == 0 ? "active" : "inactive";
      position1 = new SimplePositionPdx(secIds[SimplePositionPdx.cnt % secIds.length],
          SimplePositionPdx.cnt * 1000);

      positions.add(new SimplePositionPdx(secIds[SimplePositionPdx.cnt % secIds.length],
          SimplePositionPdx.cnt * 1000));
      positions.add(new SimplePositionPdx(secIds[SimplePositionPdx.cnt % secIds.length],
          SimplePositionPdx.cnt * 1000));
    }

    public SimplePortfolioPdx(int i, int j) {
      this(i);
      position1.portfolioId = j;
    }

    public static void resetCounter() {
      numInstance = 0;
    }

    private boolean eq(Object o1, Object o2) {
      return o1 == null ? o2 == null : o1.equals(o2);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SimplePortfolioPdx)) {
        return false;
      }
      SimplePortfolioPdx p2 = (SimplePortfolioPdx) o;
      return ID == p2.getID();
    }

    @Override
    public int hashCode() {
      return ID;
    }


    public String toString() {
      String out = "SimplePortfolioPdx [ID=" + ID + " status=" + status + "\n ";
      Iterator iter = positions.iterator();
      while (iter.hasNext()) {
        out += iter.next() + ", ";
      }
      out += "\n P1:" + position1;
      return out + "\n]";
    }

    /**
     * Getter for property type.S
     *
     * @return Value of property type.
     */
    public boolean boolFunction(String strArg) {
      return "active".equals(strArg);
    }

    public int intFunction(int j) {
      return j;
    }

    public String funcReturnSecId(Object o) {
      return ((SimplePositionPdx) o).getSecId();
    }

    public long longFunction(long j) {
      return j;
    }

    @Override
    public void fromData(PdxReader in) {
      ID = in.readInt("ID");
      position1 = (SimplePositionPdx) in.readObject("position1");
      positions = (HashSet) in.readObject("positions");
      status = in.readString("status");
      names = in.readStringArray("names");
      description = in.readString("description");
      createTime = in.readLong("createTime");
      intArr = in.readIntArray("intArr");
    }

    @Override
    public void toData(PdxWriter out) {
      out.writeInt("ID", ID);
      out.writeObject("position1", position1);
      out.writeObject("positions", positions);
      out.writeString("status", status);
      out.writeStringArray("names", names);
      out.writeString("description", description);
      out.writeLong("createTime", createTime);
      out.writeIntArray("intArr", intArr);
      // Identity Field.
      out.markIdentityField("ID");
    }

  }

  public static class SimplePositionPdx implements Serializable, PdxSerializable, Comparable {
    public String secId;
    private String country = "USA";
    private int pid;
    private double sharesOutstanding;
    public int portfolioId = 0;
    public static int cnt = 0;

    public static int numInstance = 0;

    public SimplePositionPdx() {
      numInstance++;
    }

    public SimplePositionPdx(String id, double out) {
      secId = id;
      sharesOutstanding = out;
      pid = cnt++;

      numInstance++;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SimplePositionPdx)) {
        return false;
      }
      return secId.equals(((SimplePositionPdx) o).secId);
    }

    @Override
    public int hashCode() {
      return secId.hashCode();
    }

    public String getSecId() {
      return secId;
    }

    public static void resetCounter() {
      cnt = 0;
    }

    public double getSharesOutstanding() {
      return sharesOutstanding;
    }

    public String toString() {
      return "SimplePositionPdx [secId=" + secId + " pid=" + pid + " out="
          + sharesOutstanding + "]";
    }

    @Override
    public void fromData(PdxReader in) {
      country = in.readString("country");
      secId = in.readString("secId");
      sharesOutstanding = in.readDouble("sharesOutstanding");
      pid = in.readInt("pid");
      portfolioId = in.readInt("portfolioId");
    }

    @Override
    public void toData(PdxWriter out) {
      out.writeString("country", country);
      out.writeString("secId", secId);
      out.writeDouble("sharesOutstanding", sharesOutstanding);
      out.writeInt("pid", pid);
      out.writeInt("portfolioId", portfolioId);
      // Identity Field.
      out.markIdentityField("secId");
    }


    @Override
    public int compareTo(Object o) {
      if (o == this || ((SimplePositionPdx) o).secId.equals(secId)) {
        return 0;
      } else {
        return pid < ((SimplePositionPdx) o).pid ? -1 : 1;
      }

    }
  }
}
