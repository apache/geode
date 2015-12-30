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
package com.gemstone.gemfire.modules;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.modules.Owner.Status;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.AnnotationConfiguration;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class HibernateJUnitTest {

  private Logger log = LoggerFactory.getLogger(getClass());

  //private static final String jdbcURL = "jdbc:hsqldb:hsql://localhost/test";
  private static final String jdbcURL = "jdbc:hsqldb:mem:test";

  static File tmpDir;

  static String gemfireLog;

  @Before
  public void setUp() throws Exception {
    // Create a per-user scratch directory
    tmpDir = new File(System.getProperty("java.io.tmpdir"),
            "gemfire_modules-" + System.getProperty("user.name"));
    tmpDir.mkdirs();
    tmpDir.deleteOnExit();

    gemfireLog = tmpDir.getPath() +
            System.getProperty("file.separator") + "gemfire_modules.log";
  }

  public static SessionFactory getSessionFactory(Properties overrideProps) {
    System.setProperty("gemfire.home", "GEMFIREHOME");
    Configuration cfg = new Configuration();
    cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.HSQLDialect");
    cfg.setProperty("hibernate.connection.driver_class",
        "org.hsqldb.jdbcDriver");
    // cfg.setProperty("hibernate.connection.url", "jdbc:hsqldb:mem:test");
    cfg.setProperty("hibernate.connection.url", jdbcURL);
    cfg.setProperty("hibernate.connection.username", "sa");
    cfg.setProperty("hibernate.connection.password", "");
    cfg.setProperty("hibernate.connection.pool_size", "1");
    cfg.setProperty("hibernate.connection.autocommit", "true");
    cfg.setProperty("hibernate.hbm2ddl.auto", "update");

    cfg.setProperty("hibernate.cache.region.factory_class",
        "com.gemstone.gemfire.modules.hibernate.GemFireRegionFactory");
    cfg.setProperty("hibernate.show_sql", "true");
    cfg.setProperty("hibernate.cache.use_query_cache", "true");
    //cfg.setProperty("gemfire.mcast-port", AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS)+"");
    cfg.setProperty("gemfire.mcast-port", "0");
    cfg.setProperty("gemfire.statistic-sampling-enabled", "true");
    cfg.setProperty("gemfire.log-file", gemfireLog);
    cfg.setProperty("gemfire.writable-working-dir", tmpDir.getPath());
    //cfg.setProperty("gemfire.statistic-archive-file", "plugin-stats-file.gfs");
    //cfg.setProperty("gemfire.default-client-region-attributes-id", "CACHING_PROXY");
    //cfg.setProperty("gemfire.cache-topology", "client-server");
    //cfg.setProperty("gemfire.locators", "localhost[5432]");
    //cfg.setProperty("gemfire.log-level", "fine");
    // cfg.setProperty("", "");
    cfg.addClass(Person.class);
    cfg.addClass(Event.class);
    if (overrideProps != null) {
      Iterator it = overrideProps.keySet().iterator();
      while (it.hasNext()) {
        String key = (String)it.next();
        cfg.setProperty(key, overrideProps.getProperty(key));
      }
    }
    return cfg.buildSessionFactory();
  }

  @Test
  public void testpreload() {
    log.info("SWAP:creating session factory In hibernateTestCase");

    Session session = getSessionFactory(null).openSession();
    log.info("SWAP:session opened");
    session.beginTransaction();
    Event theEvent = new Event();
    theEvent.setTitle("title");
    theEvent.setDate(new Date());
    session.save(theEvent);
    Long id = theEvent.getId();
    session.getTransaction().commit();
    session.beginTransaction();
    Event ev = (Event)session.get(Event.class, id);
    log.info("SWAP:load complete: " + ev);
    session.getTransaction().commit();
  }

  @Test
  public void testNothing() throws Exception {
    java.util.logging.Logger.getLogger("org.hibernate").setLevel(Level.ALL);
    log.info("SWAP:creating session factory In hibernateTestCase");

    Session session = getSessionFactory(null).openSession();
    log.info("SWAP:session opened");
    // session.setFlushMode(FlushMode.COMMIT);
    session.beginTransaction();
    Event theEvent = new Event();
    theEvent.setTitle("title");
    theEvent.setDate(new Date());
    //session.save(theEvent);
    session.persist(theEvent);
    Long id = theEvent.getId();
    session.getTransaction().commit();
    log.info("commit complete...doing load");
    session.beginTransaction();
    Event ev = (Event)session.load(Event.class, id);
    log.info("load complete: " + ev);
    log.trace("SWAP");
    ev.setTitle("newTitle");
    session.save(ev);
    log.info("commit");
    session.getTransaction().commit();
    log.info("save complete " + ev);

    session.beginTransaction();
    ev = (Event)session.load(Event.class, id);
    log.info("load complete: " + ev);
    ev.setTitle("newTitle2");
    session.save(ev);
    log.info("commit");
    session.getTransaction().commit();
    log.info("save complete " + ev);

    ev = (Event)session.load(Event.class, id);
    log.info("second load " + ev);
    session.flush();
    session.close();
    log.info("flush complete session:" + session);

    for (int i=0; i<5; i++) {
      session = getSessionFactory(null).openSession();
      log.info("doing get "+id);
      // ev = (Event) session.load(Event.class, id);
      ev = (Event)session.get(Event.class, id);
      log.info("third load " + ev);
    }
    printExistingDB();
    Iterator it = GemFireCacheImpl.getInstance().rootRegions().iterator();
    while (it.hasNext()) {
      Region r = (Region)it.next();
      System.out.println("Region:"+r);
      Iterator enIt = r.entrySet().iterator();
      while (enIt.hasNext()) {
        Region.Entry re = (Entry)enIt.next();
        System.out.println("key:"+re.getKey()+" value:"+re.getValue());
      }
    }
    Thread.sleep(3000);
     //System.in.read();
    // try direct data

  }

  public void _testInvalidation() {
    Session s = getSessionFactory(null).openSession();
  }

  static Long personId;

  @Test
  public void testRelationship() throws Exception {
    //java.util.logging.Logger.getLogger("org.hibernate").setLevel(Level.ALL);
    Properties props = new Properties();
    props.put("gemfire.topology", "client-server");
    Session session = getSessionFactory(null).openSession();
    session.beginTransaction();

    Person thePerson = new Person();
    thePerson.setFirstname("foo");
    thePerson.setLastname("bar");
    thePerson.setAge(1);
    thePerson.setId(99L);
    session.save(thePerson);
    personId = thePerson.getId();
    log.info("person saved");
    
    Event theEvent = new Event();
    theEvent.setTitle("title");
    theEvent.setDate(new Date());
    session.save(theEvent);
    Long eventId = theEvent.getId();
    log.info("event saved");
    
    Event theEvent2 = new Event();
    theEvent2.setTitle("title2");
    theEvent2.setDate(new Date());
    session.save(theEvent2);
    Long eventId2 = theEvent2.getId();
    log.info("event2 saved");
    session.getTransaction().commit();
    
    session.beginTransaction();
    Person aPerson = (Person) session.load(Person.class, personId);
    Event anEvent = (Event) session.load(Event.class, eventId);
    Event anEvent2 = (Event) session.load(Event.class, eventId2);
    log.info("adding event to person");
    aPerson.getE().add(anEvent);
    aPerson.getE().add(anEvent2);
    log.info("calling commit");

    session.getTransaction().commit();
    log.info("commit complete");
    session.close();
    log.info("opening new session");
    session = getSessionFactory(null).openSession();
    log.info("SWAP:loading person");
    aPerson = (Person)session.load(Person.class, personId);
    log.info("loading events");
    Iterator<Event> e = aPerson.getE().iterator();
    while (e.hasNext()) {
      e.next();
    }
    session.close();
    log.info("opening new session");
    session = getSessionFactory(null).openSession();
    log.info("SWAP:loading person");
    aPerson = (Person)session.load(Person.class, personId);
    log.info("loading events");
    e = aPerson.getE().iterator();
    while (e.hasNext()) {
      e.next();
    }

    log.info(aPerson.getE()+"");
    session.close();
    //System.in.read();
//    log.info("opening third session");
//    session = getSessionFactory().openSession();
//    log.info("loading person");
//    aPerson = (Person)session.load(Person.class, personId);
//    log.info("loading events");
//    log.info(aPerson.getEvents()+"");
  }
  
  public void _testQueryCache() throws Exception {
    Session session = getSessionFactory(null).openSession();
    Query q = session.createQuery("from Event");
    q.setCacheable(true);
    List l = q.list();
    log.info("list:"+l);
//    log.info("Sleeping for 10 seconds");
//    Thread.sleep(10000);
    l = q.list();
    log.info("list2:"+l);
    log.info("updating an event");
    session.beginTransaction();
    Event e = (Event)l.get(0);
    e.setDate(new Date());
    session.saveOrUpdate(e);
    session.getTransaction().commit();
    l = q.list();
    log.info("list3:"+l);
  }

  @Test
  public void testInsert() {
    Session session = getSessionFactory(null).openSession();
    Region r = GemFireCacheImpl.getExisting().getRegion(Person.class.getCanonicalName());
    int initSize = r.size();
    session.beginTransaction();
    log.info("SWAP: Saving Person");
    Person p = new Person();
    p.setId(10L);
    p.setFirstname("foo");
    p.setLastname("bar");
    session.saveOrUpdate("Person", p);
    session.getTransaction().commit();
    assertEquals(1, session.getStatistics().getEntityCount());
    assertEquals(initSize+1, r.size());

    session.beginTransaction();
    p.setAge(1);
    session.saveOrUpdate(p);
    session.getTransaction().commit();
    assertEquals(1, session.getStatistics().getEntityCount());
  }

  @Test
  public void testNormalRegion() {
    Properties props = new Properties();
    props.setProperty("gemfire.default-region-attributes-id", "LOCAL");
    Session session = getSessionFactory(props).openSession();
    session.beginTransaction();
    Event theEvent = new Event();
    theEvent.setTitle("title");
    theEvent.setDate(new Date());
    session.save(theEvent);
    Long id = theEvent.getId();
    session.getTransaction().commit();
    session.beginTransaction();
    Event ev = (Event)session.load(Event.class, id);
    ev.setTitle("newTitle");
    session.save(ev);
    session.getTransaction().commit();
  }
  
  private void printExistingDB() throws SQLException {
    try {
      Class.forName("org.hsqldb.jdbc.JDBCDriver");
    }
    catch (Exception e) {
      System.err.println("ERROR: failed to load HSQLDB JDBC driver.");
      e.printStackTrace();
      return;
    }

    Connection c = DriverManager.getConnection(jdbcURL, "SA", "");
    log.info("issuing query...");
    ResultSet rs = c.createStatement().executeQuery("select * from events");
    int col = rs.getMetaData().getColumnCount();
    while (rs.next()) {
      StringBuilder b = new StringBuilder();
      for (int i = 1; i <= col; i++) {
        b.append(" col:" + i + ":" + rs.getString(i));
      }
      log.info("Query result:" + b.toString());
    }
  }

  @Test
  public void testEnum() {
    AnnotationConfiguration cfg = new AnnotationConfiguration();
    cfg.addAnnotatedClass(Owner.class);
    cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.HSQLDialect");
    cfg.setProperty("hibernate.connection.driver_class",
        "org.hsqldb.jdbcDriver");
    cfg.setProperty("hibernate.connection.url", jdbcURL);
    cfg.setProperty("hibernate.connection.username", "sa");
    cfg.setProperty("hibernate.connection.password", "");
    cfg.setProperty("hibernate.connection.pool_size", "1");
    cfg.setProperty("hibernate.connection.autocommit", "true");
    cfg.setProperty("hibernate.hbm2ddl.auto", "update");

    cfg.setProperty("hibernate.cache.region.factory_class",
        "com.gemstone.gemfire.modules.hibernate.GemFireRegionFactory");
    cfg.setProperty("hibernate.show_sql", "true");
    cfg.setProperty("hibernate.cache.use_query_cache", "true");
    cfg.setProperty("gemfire.statistic-sampling-enabled", "true");
    cfg.setProperty("gemfire.log-file", gemfireLog);
    cfg.setProperty("gemfire.writable-working-dir", tmpDir.getPath());
    cfg.setProperty("gemfire.mcast-port", "0");
    //cfg.setProperty("gemfire.cache-topology", "client-server");

    SessionFactory sf = cfg.buildSessionFactory();
    Session session = sf.openSession();
    session.beginTransaction();
    Owner o = new Owner();
    o.setAddress("addr");
    o.setCity("pdx");
    o.setStatus(Status.PREMIUM);
    session.save(o);
    long id = o.getId();
    log.info("testEnum:commiting tx");
    session.getTransaction().commit();
    session.close();
    
    session = sf.openSession();
    Owner o1 = (Owner) session.load(Owner.class, id);
    log.info("loaded:"+o);
    assertEquals(o.getAddress(), o1.getAddress());
    assertEquals(o.getCity(), o1.getCity());
    assertEquals(o.getStatus(), o1.getStatus());
    o1.setAddress("address2");
    session.save(o1);
  }
}
