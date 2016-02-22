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
package com.gemstone.gemfire.cache.query;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import junit.framework.Assert;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.query.data.Numbers;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Utility class for testing supported queries
 * 
 * @author Tejas Nomulwar
 * 
 */
public class QueryTestUtils implements Serializable {

  private static Cache cache;

  private static QueryService qs;

  public static final String KEY = "key-";

  public HashMap<String, String> queries;

  public HashMap<String, String> bindQueries;

  public QueryTestUtils() {
    queries = new HashMap<String, String>();
    bindQueries = new HashMap<String, String>();
    queries.put("1","SELECT DISTINCT * FROM /exampleRegion WHERE status = 'active'");
    queries.put("2","SELECT * FROM /exampleRegion WHERE \"type\" = 'type1' and status = 'active'");
    queries.put("3","SELECT key, positions FROM /exampleRegion.entrySet, value.positions.values positions WHERE positions.mktValue >= 25.00");
    queries.put("4","SELECT DISTINCT entry.value FROM /exampleRegion.entries entry WHERE entry.key = '1'");
    queries.put("5","SELECT * FROM /exampleRegion.entries entry WHERE entry.value.ID > 1");
    queries.put("6","SELECT entry.value FROM /exampleRegion.entries entry WHERE entry.key = '1'");
    queries.put("7","SELECT * FROM /exampleRegion.keySet key WHERE key = '1'");
    queries.put("8","SELECT * FROM /exampleRegion.values portfolio WHERE portfolio.status = 'active'");
    queries.put("9","SELECT entry.key, entry.value FROM /exampleRegion.entries entry WHERE entry.value['version'] = '100'");
    queries.put("10","SELECT DISTINCT * FROM /exampleRegion WHERE ID < 101 ORDER BY ID");
    queries.put("11","SELECT DISTINCT * FROM /exampleRegion WHERE ID < 101 ORDER BY ID asc");
    queries.put("12","SELECT DISTINCT * FROM /exampleRegion WHERE ID < 101 ORDER BY ID desc");
    queries.put("13","SELECT * FROM /exampleRegion p WHERE p.status LIKE 'active'");
    queries.put("14","SELECT * FROM /exampleRegion p WHERE p.status LIKE 'activ%'");
    queries.put("15","SELECT * FROM /exampleRegion p, p.positions.values AS pos WHERE pos.secId != '1'");
    queries.put("16","SELECT DISTINCT * FROM /exampleRegion WHERE TRUE");
    queries.put("17","SELECT * FROM /exampleRegion portfolio WHERE portfolio.ID IN SET(1, 2)");
    queries.put("18","SELECT * FROM /exampleRegion portfolio, portfolio.positions.values positions WHERE portfolio.Pk IN SET ('1', '2') AND positions.secId = '1'");
    queries.put("19","SELECT * FROM /exampleRegion portfolio, portfolio.positions.values positions WHERE portfolio.Pk IN SET ('1', '2') OR positions.secId IN SET ('1', '2', '3')");
    queries.put("20","SELECT * FROM /exampleRegion portfolio, portfolio.positions.values positions WHERE portfolio.Pk IN SET ('1', '2') OR positions.secId IN SET ('1', '2', '3') and portfolio.status = 'active'");
    queries.put("21","SELECT * FROM /exampleRegion portfolio WHERE portfolio.ID <> 2");
    queries.put("22","SELECT * FROM /exampleRegion portfolio WHERE portfolio.ID != 2");
    queries.put("23","(SELECT * FROM /exampleRegion).size");
    queries.put("24","(SELECT * FROM /exampleRegion p WHERE p.ID = 1).size");
    queries.put("25","SELECT * FROM /exampleRegion p WHERE p.status.length > 1");
    queries.put("26","select * from /exampleRegion sp where sp = set ('20', '21', '22')");
    queries.put("27","select * from /exampleRegion sp where sp.containsAll(set('20', '21', '22'))");
    queries.put("28","select * from /exampleRegion sp where sp IN SET (set('20', '21', '22'), set('10', '11', '12'))");
    queries.put("29","select distinct r.name, pVal, r.\"type\"   from /exampleRegion r , r.positions.values pVal where  ( r.name IN Set('name_11' , 'name_12') OR false ) AND pVal.mktValue = 1.00");
    queries.put("30","select distinct r.name, pVal, r.\"type\"   from /exampleRegion r , r.positions.values pVal where  ( r.undefinedTestField.toString = UNDEFINED  AND true ) AND pVal.mktValue = 1.00");
    queries.put("31","select distinct r.name, pVal, r.\"type\"   from /exampleRegion r , r.positions.values pVal where  ( r.undefinedTestField.toString = UNDEFINED  OR false )");
    queries.put("32","select distinct r.name, pVal, r.\"type\"   from /exampleRegion r , r.positions.values pVal where  (r.name='name_11' OR r.name='name_12') AND pVal.mktValue >=1.00");
    queries.put("33","(select distinct * from /exampleRegion).size");
    queries.put("34","/exampleRegion.fullPath");
    queries.put("35","/exampleRegion.size > 0");
    queries.put("36","/exampleRegion.size");
    queries.put("37","2 IN SET(1,2,3)");
    queries.put("38","IMPORT com.gemstone.gemfire.cache.\"query\".data.Manager;SELECT DISTINCT manager_id FROM (set<Manager>)/exampleRegion where empId > 0");
    queries.put("39","IMPORT com.gemstone.gemfire.cache.\"query\".data.Portfolio; SELECT DISTINCT * FROM (set<Portfolio>)/exampleRegion where iD > 0");
    queries.put("40","IMPORT com.gemstone.gemfire.cache.\"query\".data.Position;IMPORT com.gemstone.gemfire.cache.\"query\".data.Portfolio;SELECT DISTINCT secId FROM (set<Portfolio>)/exampleRegion, (set<Position>)positions.values WHERE iD > 0");
    queries.put("41","IMPORT com.gemstone.gemfire.cache.\"query\".data.Position;SELECT DISTINCT pos.secId FROM /exampleRegion, pos IN positions.values TYPE Position WHERE iD > 0");
    queries.put("42","IMPORT com.gemstone.gemfire.cache.\"query\".data.Position;SELECT DISTINCT secId FROM /exampleRegion,  positions.values pos TYPE Position WHERE iD > 0");
    queries.put("43","IMPORT com.gemstone.gemfire.cache.\"query\".data.Position;SELECT DISTINCT secId FROM /exampleRegion, (set<Position>)positions.values WHERE iD > 0");
    queries.put("44","IMPORT com.gemstone.gemfire.cache.\"query\".data.Position;SELECT DISTINCT secId FROM /exampleRegion, positions.values AS pos TYPE Position WHERE iD > 0");
    queries.put("45","SELECT   DISTINCT iD as portfolio_id, pos.secId as sec_id from /exampleRegion p , p.positions.values pos  where p.status= 'active'");
    queries.put("46","SELECT  distinct * FROM /exampleRegion order by pkid desc");
    queries.put("47","SELECT  distinct * FROM /exampleRegion pf1  order by pkid asc");
    queries.put("48","SELECT  distinct * FROM /exampleRegion pf1 order by pkid");
    queries.put("49","SELECT  distinct * FROM /exampleRegion pf1 where ID != 10 order by ID asc, pkid asc limit 10");
    queries.put("50","SELECT  distinct * FROM /exampleRegion pf1 where ID != 10 order by ID desc");
    queries.put("51","SELECT  distinct * FROM /exampleRegion pf1 where ID != 10 order by ID desc, pkid desc limit 10");
    queries.put("52","SELECT  distinct * FROM /exampleRegion pf1 where ID > 0 order by pkid asc");
    queries.put("53","SELECT  distinct * FROM /exampleRegion pf1 where ID > 0 order by pkid");
    queries.put("54","SELECT  distinct * FROM /exampleRegion pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc limit 5");
    queries.put("55","SELECT  distinct * FROM /exampleRegion pf1 where ID > 10 and ID < 20 order by ID desc");
    queries.put("56","SELECT  distinct * FROM /exampleRegion pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5");
    queries.put("57","SELECT  distinct * FROM /exampleRegion pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc");
    queries.put("58","SELECT  distinct * FROM /exampleRegion pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5");
    queries.put("59","SELECT  distinct ID, description, createTime FROM /exampleRegion pf1 where ID > 10 and ID < 20 order by ID desc, pkid asc limit 5");
    queries.put("60","SELECT  distinct ID, description, createTime FROM /exampleRegion pf1 where ID > 10 order by ID desc limit 5");
    queries.put("61","SELECT  distinct ID, description, createTime FROM /exampleRegion pf1 where ID > 10 order by ID desc, pkid desc limit 5");
    queries.put("62","SELECT  distinct ID, description, createTime FROM /exampleRegion pf1 where ID > 10 order by ID desc, pkid desc");
    queries.put("63","SELECT  distinct ID, description, createTime FROM /exampleRegion pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid desc");
    queries.put("64","SELECT  distinct ID, description, createTime FROM /exampleRegion pf1 where ID >= 10 and ID <= 20 order by ID desc limit 5");
    queries.put("65","SELECT  distinct ID, description, createTime FROM /exampleRegion pf1 where ID >= 10 and ID <= 20 order by ID desc");
    queries.put("66","SELECT  distinct ID, description, createTime FROM /exampleRegion pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid desc limit 5");
    queries.put("67","SELECT  distinct ID, description, createTime FROM /exampleRegion pf1 where pf1.ID != '10' order by ID desc limit 5");
    queries.put("68","SELECT  distinct ID, description, createTime FROM /exampleRegion pf1 where pkid > 'a' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5");
    queries.put("69","SELECT  distinct ID, pkid FROM /exampleRegion pf1 where ID < 1000 order by pkid");
    queries.put("70","SELECT  distinct ID, pkid FROM /exampleRegion pf1 where ID > 0 order by pkid");
    queries.put("71","SELECT  distinct ID, pkid FROM /exampleRegion pf1 where ID > 0 order by pkid, ID asc");
    queries.put("72","SELECT  distinct ID, pkid FROM /exampleRegion pf1 where ID > 0 order by pkid, ID desc");
    queries.put("73","SELECT  distinct pkid FROM /exampleRegion pf1 order by pkid");
    queries.put("74","SELECT  distinct pkid FROM /exampleRegion pf1 where ID > 0 order by pkid asc");
    queries.put("75","SELECT  distinct pkid FROM /exampleRegion pf1 where ID > 0 order by pkid desc");
    queries.put("76","SELECT  distinct pkid FROM /exampleRegion pf1 where ID > 0 order by pkid");
    queries.put("77","SELECT  distinct pkid FROM /exampleRegion pf1 where pkid != 'XXXX' order by pkid asc");
    queries.put("78","SELECT  distinct pkid FROM /exampleRegion pf1 where pkid != 'XXXX' order by pkid desc");
    queries.put("79","SELECT * FROM /exampleRegion p where p.intVar < 9");
    queries.put("80","SELECT * FROM /exampleRegion pf WHERE pf = 'XX1'");
    queries.put("81","SELECT * FROM /exampleRegion pf WHERE pf IN SET( 'XX5', 'XX6', 'XX7')");
    queries.put("82","SELECT * FROM /exampleRegion");
    queries.put("83","SELECT * FROM /exampleRegion.keys k WHERE k.ID = 1");
    queries.put("84","SELECT * FROM /exampleRegion.keys key WHERE key.ID = 1");
    queries.put("85","SELECT * FROM /exampleRegion.keys key WHERE key.ID > 5");
    queries.put("86","SELECT * FROM /exampleRegion.values pf WHERE pf IN SET( 'XX5', 'XX6', 'XX7')");
    queries.put("87","SELECT * from /exampleRegion");
    queries.put("88","SELECT ALL * FROM /exampleRegion");
    queries.put("89","SELECT DISTINCT  q.cusip, q.quoteType, q.dealerPortfolio, q.channelName, q.dealerCode, q.priceType, q.price, q.lowerQty, q.upperQty, q.ytm, r.minQty, r.maxQty, r.incQty FROM /exampleRegion q, /exampleRegion r WHERE q.cusip = r.cusip AND q.quoteType = r.quoteType");
    queries.put("90","SELECT DISTINCT * FROM (SELECT DISTINCT * FROM /exampleRegion AS ptf, positions AS pos) WHERE pos.value.secId = 'IBM'");
    queries.put("91","SELECT DISTINCT * FROM (SELECT DISTINCT * FROM /exampleRegion ptf, positions pos) WHERE pos.value.secId = 'IBM'");
    queries.put("92","SELECT DISTINCT * FROM (SELECT DISTINCT * FROM /exampleRegion ptf, positions pos) p WHERE p.get('pos').value.secId = 'IBM'");
    queries.put("93","SELECT DISTINCT * FROM (SELECT DISTINCT * FROM /exampleRegion ptf, positions pos) p WHERE p.pos.value.secId = 'IBM'");
    queries.put("94","SELECT DISTINCT * FROM (SELECT DISTINCT * FROM /exampleRegion ptf, positions pos) p WHERE pos.value.secId = 'IBM'");
    queries.put("95","SELECT DISTINCT * FROM (SELECT DISTINCT * FROM /exampleRegion, positions) WHERE positions.value.secId = 'IBM'");
    queries.put("96","SELECT DISTINCT * FROM (SELECT DISTINCT * FROM /exampleRegion, positions) p WHERE p.positions.value.secId = 'IBM'");
    queries.put("97","SELECT DISTINCT * FROM (SELECT DISTINCT * FROM ptf IN /exampleRegion, pos IN positions) WHERE pos.value.secId = 'IBM'");
    queries.put("98","SELECT DISTINCT * FROM (SELECT DISTINCT pos AS myPos FROM /exampleRegion ptf, positions pos) WHERE myPos.value.secId = 'IBM'");
    queries.put("99","SELECT DISTINCT * FROM /exampleRegion  pf WHERE pf.ID > 10 limit 5");
    queries.put("100","SELECT DISTINCT * FROM /exampleRegion  pf, pf.collectionHolderMap.keySet  WHERE pf.ID > 10 limit 20");
    queries.put("101","SELECT DISTINCT * FROM /exampleRegion WHERE NOT(SELECT DISTINCT * FROM positions.values p WHERE p.secId = 'IBM').isEmpty");
    queries.put("102","SELECT DISTINCT * FROM /exampleRegion as a, a.collectionHolderMap['0'].arr as b where a.status = 'active' and a.ID = 0");
    queries.put("103","SELECT DISTINCT * FROM /exampleRegion c WHERE c.name = 'INDIA'");
    queries.put("104","SELECT DISTINCT * FROM /exampleRegion c, c.states s, s.districts d WHERE d.name = 'PUNEDIST' AND s.name = 'GUJARAT'");
    queries.put("105","SELECT DISTINCT * FROM /exampleRegion c, c.states s, s.districts d, d.cities ct WHERE ct.name = 'MUMBAI'");
    queries.put("106","SELECT DISTINCT * FROM /exampleRegion c, c.states s, s.districts d, d.cities ct, d.villages v WHERE c.name = 'INDIA'");
    queries.put("107","SELECT DISTINCT * FROM /exampleRegion c, c.states s, s.districts d, d.villages v, d.cities ct WHERE ct.name = 'PUNE' AND s.name = 'MAHARASHTRA'");
    queries.put("108","SELECT DISTINCT * FROM /exampleRegion c, c.states s, s.districts d, d.villages v, d.cities ct WHERE v.name = 'MAHARASHTRA_VILLAGE1'");
    queries.put("109","SELECT DISTINCT * FROM /exampleRegion c, c.states s, s.districts d, d.villages v, d.cities ct WHERE v.name='MAHARASHTRA_VILLAGE1' AND ct.name = 'PUNE'");
    queries.put("110","SELECT DISTINCT * FROM /exampleRegion itr1  WHERE itr1.liist[0] >= 2");
    queries.put("111","SELECT DISTINCT * FROM /exampleRegion itr1  WHERE itr1.maap.get('key2') >= 16");
    queries.put("112","SELECT DISTINCT * FROM /exampleRegion itr1  WHERE itr1.maap.get('key2') >= 3");
    queries.put("113","SELECT DISTINCT * FROM /exampleRegion itr1  WHERE itr1.maap['key2'] >= 16");
    queries.put("114","SELECT DISTINCT * FROM /exampleRegion itr1  WHERE itr1.maap['key2'] >= 3  and  itr1.maap['key3'] < 18");
    queries.put("115","SELECT DISTINCT * FROM /exampleRegion itr1  WHERE itr1.maap['key2'] >= 3 and itr1.maap['key2'] <=18");
    queries.put("116","SELECT DISTINCT * FROM /exampleRegion itr1  WHERE itr1.maap['key2'] >= 3");
    queries.put("117","SELECT DISTINCT * FROM /exampleRegion itr1  WHERE itr1.maap['key3'] >= 3  and  itr1.maap['key3'] >= 13");
    queries.put("118","SELECT DISTINCT * FROM /exampleRegion itr1  WHERE itr1.maap['key3'] >= 3");
    queries.put("119","SELECT DISTINCT * FROM /exampleRegion itr1  WHERE itr1.maap['key4'] >= 16");
    queries.put("120","SELECT DISTINCT * FROM /exampleRegion num WHERE num.id1 >= -200");
    queries.put("121","SELECT DISTINCT * FROM /exampleRegion p  WHERE p.ID <= 10 order by p.ID asc limit 1");
    queries.put("122","SELECT DISTINCT * FROM /exampleRegion p  WHERE p.ID <= 10 order by p.ID desc limit 1");
    queries.put("123","SELECT DISTINCT * FROM /exampleRegion p  WHERE p.ID <= 10 order by p.pkid desc limit 1");
    queries.put("124","SELECT DISTINCT * FROM /exampleRegion pf WHERE pf.ID > 0 limit 5");
    queries.put("125","SELECT DISTINCT * FROM /exampleRegion pf where pf.description = 'XXXX'  and pf.status='active' and pf.createTime = 5");
    queries.put("126","SELECT DISTINCT * FROM /exampleRegion pf where pf.description = 'XXXX'  and pf.status='active'");
    queries.put("127","SELECT DISTINCT * FROM /exampleRegion pf,  positions.values pos where pf.ID > 0 and pf.ID < 250  and pf.status='active' and  pos.secId != null");
    queries.put("128","SELECT DISTINCT * FROM /exampleRegion pf,  positions.values pos where pf.status='active' and pos.secId= 'IBM' and pf.ID = 0");
    queries.put("129","SELECT DISTINCT * FROM /exampleRegion pf, /exampleRegion emp WHERE pf.ID = emp.empId");
    queries.put("130","SELECT DISTINCT * FROM /exampleRegion pf, pf.positions pos, /exampleRegion emp WHERE pf.iD = emp.empId and pf.status='active' and emp.age > 900");
    queries.put("131","SELECT DISTINCT * FROM /exampleRegion pf, pf.positions pos, /exampleRegion emp WHERE pf.iD = emp.empId");
    queries.put("132","SELECT DISTINCT * FROM /exampleRegion pf, pf.positions.values WHERE pf.ID > 0 limit 5");
    queries.put("133","SELECT DISTINCT * FROM /exampleRegion pf, pf.positions.values position WHERE (pf.ID > 0 OR position.secId = 'SUN') OR false");
    queries.put("134","SELECT DISTINCT * FROM /exampleRegion pf, pf.positions.values position WHERE (true = null OR position.secId = 'SUN') AND true");
    queries.put("135","SELECT DISTINCT * FROM /exampleRegion pf, pf.positions.values position WHERE (true = null OR position.secId = 'SUN') OR true");
    queries.put("136","SELECT DISTINCT * FROM /exampleRegion pf, pf.positions.values position WHERE true = null OR position.secId = 'SUN'");
    queries.put("137","SELECT DISTINCT * FROM /exampleRegion value  where value.length > 6");
    queries.put("138","SELECT DISTINCT * FROM /exampleRegion value");
    queries.put("139","SELECT DISTINCT * FROM /exampleRegion where (status = 'active' OR ID = 2) AND P1.secId  = 'IBM'");
    queries.put("140","SELECT DISTINCT * FROM /exampleRegion where FALSE");
    queries.put("141","SELECT DISTINCT * FROM /exampleRegion where NOT (status = 'active') AND ID = 2");
    queries.put("142","SELECT DISTINCT * FROM /exampleRegion where NOT (status = 'active') OR ID = 2");
    queries.put("143","SELECT DISTINCT * FROM /exampleRegion where NULL");
    queries.put("144","SELECT DISTINCT * FROM /exampleRegion where P1.secId != 'IBM'");
    queries.put("145","SELECT DISTINCT * FROM /exampleRegion where P1.secId > 'IBM'");
    queries.put("146","SELECT DISTINCT * FROM /exampleRegion where P1.secId >= 'IBM'");
    queries.put("147","SELECT DISTINCT * FROM /exampleRegion where P1.secId!='DELL'");
    queries.put("148","SELECT DISTINCT * FROM /exampleRegion where P1.secId<'DELL'");
    queries.put("149","SELECT DISTINCT * FROM /exampleRegion where P1.secId<='DELL'");
    queries.put("150","SELECT DISTINCT * FROM /exampleRegion where P1.secId<>'DELL'");
    queries.put("151","SELECT DISTINCT * FROM /exampleRegion where P1.secId='DELL'");
    queries.put("152","SELECT DISTINCT * FROM /exampleRegion where P1.secId>'DELL'");
    queries.put("153","SELECT DISTINCT * FROM /exampleRegion where P1.secId>='DELL'");
    queries.put("154","SELECT DISTINCT * FROM /exampleRegion where P1.secType = 'a'");
    queries.put("155","SELECT DISTINCT * FROM /exampleRegion where P2!=null");
    queries.put("156","SELECT DISTINCT * FROM /exampleRegion where P2.secId = UNDEFINED");
    queries.put("157","SELECT DISTINCT * FROM /exampleRegion where P2.secId = null");
    queries.put("158","SELECT DISTINCT * FROM /exampleRegion where P2.secId!= UNDEFINED");
    queries.put("159","SELECT DISTINCT * FROM /exampleRegion where P2.secId< UNDEFINED");
    queries.put("160","SELECT DISTINCT * FROM /exampleRegion where P2.secId<= UNDEFINED");
    queries.put("161","SELECT DISTINCT * FROM /exampleRegion where P2.secId<> UNDEFINED");
    queries.put("162","SELECT DISTINCT * FROM /exampleRegion where P2.secId= UNDEFINED");
    queries.put("163","SELECT DISTINCT * FROM /exampleRegion where P2.secId> UNDEFINED");
    queries.put("164","SELECT DISTINCT * FROM /exampleRegion where P2.secId>= UNDEFINED");
    queries.put("165","SELECT DISTINCT * FROM /exampleRegion where P2<=null");
    queries.put("166","SELECT DISTINCT * FROM /exampleRegion where P2<>null");
    queries.put("167","SELECT DISTINCT * FROM /exampleRegion where P2<null");
    queries.put("168","SELECT DISTINCT * FROM /exampleRegion where P2=null");
    queries.put("169","SELECT DISTINCT * FROM /exampleRegion where P2>=null");
    queries.put("170","SELECT DISTINCT * FROM /exampleRegion where P2>null");
    queries.put("171","SELECT DISTINCT * FROM /exampleRegion where TRUE");
    queries.put("172","SELECT DISTINCT * FROM /exampleRegion where UNDEFINED");
    queries.put("173","SELECT DISTINCT * FROM /exampleRegion where ID != 2");
    queries.put("174","SELECT DISTINCT * FROM /exampleRegion where ID <> 2");
    queries.put("175","SELECT DISTINCT * FROM /exampleRegion where ID >= 2");
    queries.put("176","SELECT DISTINCT * FROM /exampleRegion where ID!=2");
    queries.put("177","SELECT DISTINCT * FROM /exampleRegion where ID<2");
    queries.put("178","SELECT DISTINCT * FROM /exampleRegion where ID<=2");
    queries.put("179","SELECT DISTINCT * FROM /exampleRegion where ID<>2");
    queries.put("180","SELECT DISTINCT * FROM /exampleRegion where ID=2");
    queries.put("181","SELECT DISTINCT * FROM /exampleRegion where ID>2");
    queries.put("182","SELECT DISTINCT * FROM /exampleRegion where ID>=2");
    queries.put("183","SELECT DISTINCT * FROM /exampleRegion where status != 'active'");
    queries.put("184","SELECT DISTINCT * FROM /exampleRegion where status < 'active'");
    queries.put("185","SELECT DISTINCT * FROM /exampleRegion where status <= 'active'");
    queries.put("186","SELECT DISTINCT * FROM /exampleRegion where status <> 'active'");
    queries.put("187","SELECT DISTINCT * FROM /exampleRegion where status = 'active' AND ( ID = 2 OR P1.secId  = 'IBM')");
    queries.put("188","SELECT DISTINCT * FROM /exampleRegion where status = 'active' AND NOT( ID = 2 )");
    queries.put("189","SELECT DISTINCT * FROM /exampleRegion where status = 'active' AND P1.secType = 'a'");
    queries.put("190","SELECT DISTINCT * FROM /exampleRegion where status = 'active' AND ID = 1 OR P1.secType = 'a'");
    queries.put("191","SELECT DISTINCT * FROM /exampleRegion where status = 'active' AND ID = 2 AND P1.secId  = 'IBM'");
    queries.put("192","SELECT DISTINCT * FROM /exampleRegion where status = 'active' AND ID = 2 OR P1.secId  = 'IBM'");
    queries.put("193","SELECT DISTINCT * FROM /exampleRegion where status = 'active' AND ID = 2");
    queries.put("194","SELECT DISTINCT * FROM /exampleRegion where status = 'active' AND ID =1 AND P1.secType = 'a'");
    queries.put("195","SELECT DISTINCT * FROM /exampleRegion where status = 'active' OR NOT( ID = 2 )");
    queries.put("196","SELECT DISTINCT * FROM /exampleRegion where status = 'active' OR P1.secType = 'a'");
    queries.put("197","SELECT DISTINCT * FROM /exampleRegion where status = 'active' OR ID = 1 AND P1.secType = 'a'");
    queries.put("198","SELECT DISTINCT * FROM /exampleRegion where status = 'active' OR ID = 2 AND P1.secId  = 'IBM'");
    queries.put("199","SELECT DISTINCT * FROM /exampleRegion where status = 'active' OR ID = 2 OR P1.secId  = 'IBM'");
    queries.put("200","SELECT DISTINCT * FROM /exampleRegion where status = 'active' OR ID = 2");
    queries.put("201","SELECT DISTINCT * FROM /exampleRegion where status = 'active'");
    queries.put("202","SELECT DISTINCT * FROM /exampleRegion where status = 'inactive'");
    queries.put("203","SELECT DISTINCT * FROM /exampleRegion where status > 'active'");
    queries.put("204","SELECT DISTINCT * FROM /exampleRegion where status >= 'active'");
    queries.put("205","SELECT DISTINCT * FROM /exampleRegion where status='active'");
    queries.put("206","SELECT DISTINCT * FROM /exampleRegion where\"AND\"()");
    queries.put("207","SELECT DISTINCT * FROM /exampleRegion where\"DISTINCT\"()");
    queries.put("208","SELECT DISTINCT * FROM /exampleRegion where\"ELEMENT\"()");
    queries.put("209","SELECT DISTINCT * FROM /exampleRegion where\"FALSE\"");
    queries.put("210","SELECT DISTINCT * FROM /exampleRegion where\"FALSE\"()");
    queries.put("211","SELECT DISTINCT * FROM /exampleRegion where\"FROM\"()");
    queries.put("212","SELECT DISTINCT * FROM /exampleRegion where\"NOT\"()");
    queries.put("213","SELECT DISTINCT * FROM /exampleRegion where\"OR\"()");
    queries.put("214","SELECT DISTINCT * FROM /exampleRegion where\"SELECT\"()");
    queries.put("215","SELECT DISTINCT * FROM /exampleRegion where\"TRUE\"");
    queries.put("216","SELECT DISTINCT * FROM /exampleRegion where\"TRUE\"()");
    queries.put("217","SELECT DISTINCT * FROM /exampleRegion where\"TYPE\"()");
    queries.put("218","SELECT DISTINCT * FROM /exampleRegion where\"UNDEFINED\"()");
    queries.put("219","SELECT DISTINCT * FROM /exampleRegion where\"WHERE\"()");
    queries.put("220","SELECT DISTINCT * FROM /exampleRegion where\"and\"");
    queries.put("221","SELECT DISTINCT * FROM /exampleRegion where\"distinct\"");
    queries.put("222","SELECT DISTINCT * FROM /exampleRegion where\"element\"");
    queries.put("223","SELECT DISTINCT * FROM /exampleRegion where\"from\"");
    queries.put("224","SELECT DISTINCT * FROM /exampleRegion where\"not\"");
    queries.put("225","SELECT DISTINCT * FROM /exampleRegion where\"or\"");
    queries.put("226","SELECT DISTINCT * FROM /exampleRegion where\"select\"");
    queries.put("227","SELECT DISTINCT * FROM /exampleRegion where\"undefined\"");
    queries.put("228","SELECT DISTINCT * FROM /exampleRegion where\"where\"");
    queries.put("229","SELECT DISTINCT * FROM /exampleRegion x where status = ELEMENT(SELECT DISTINCT * FROM /exampleRegion p where p.ID = 1).status");
    queries.put("230","SELECT DISTINCT * FROM /exampleRegion x where status = ELEMENT(SELECT DISTINCT * FROM /exampleRegion p where p.ID = x.ID).status");
    queries.put("231","SELECT DISTINCT * FROM /exampleRegion x where status = ELEMENT(SELECT DISTINCT * FROM /exampleRegion p where x.ID = p.ID).status");
    queries.put("232","SELECT DISTINCT * FROM /exampleRegion");
    queries.put("233","SELECT DISTINCT * FROM /exampleRegion er,  positions.values where er.status='active'");
    queries.put("234","SELECT DISTINCT * FROM /exampleRegion er, positions WHERE er.ID = 3");
    queries.put("235","SELECT DISTINCT * FROM NULL");
    queries.put("236","SELECT DISTINCT * FROM UNDEFINED");
    queries.put("237","SELECT DISTINCT * from /exampleRegion order by status, ID desc");
    queries.put("238","SELECT DISTINCT * from /exampleRegion p order by p.getID()");
    queries.put("239","SELECT DISTINCT * from /exampleRegion p order by p.getP1().getSecId()");
    queries.put("240","SELECT DISTINCT * from /exampleRegion p order by p.getP1().secId");
    queries.put("241","SELECT DISTINCT * from /exampleRegion p order by p.getP1().secId, p.ID desc, p.ID LIMIT 9");
    queries.put("242","SELECT DISTINCT * from /exampleRegion p order by p.ID");
    queries.put("243","SELECT DISTINCT * from /exampleRegion p order by p.names[1]");
    queries.put("244","SELECT DISTINCT * from /exampleRegion p order by p.position1.secId");
    queries.put("245","SELECT DISTINCT * from /exampleRegion p order by p.status, p.ID");
    queries.put("246","SELECT DISTINCT * from /exampleRegion p, p.positions.values order by p.ID");
    queries.put("247","SELECT DISTINCT * from /exampleRegion pf , pf.positions.values pos where pos.getSecId = 'IBM' and pf.status = 'inactive'");
    queries.put("248","SELECT DISTINCT * from /exampleRegion pf, pf.positions.values pos where pos.secId = 'IBM'");
    queries.put("249","SELECT DISTINCT c.name, s.name FROM /exampleRegion c, c.states s, s.districts d, d.cities ct WHERE ct.name = 'MUMBAI' AND s.name = 'GUJARAT'");
    queries.put("250","SELECT DISTINCT c.name, s.name FROM /exampleRegion c, c.states s, s.districts d, d.cities ct WHERE ct.name = 'MUMBAI' OR s.name = 'GUJARAT'");
    queries.put("251","SELECT DISTINCT c.name, s.name, ct.name FROM /exampleRegion c, c.states s, s.districts d, d.cities ct, d.getVillages() v WHERE v.getName() = 'PUNJAB_VILLAGE1'");
    queries.put("252","SELECT DISTINCT c.name, s.name, d.name, ct.name FROM /exampleRegion c, c.states s, s.districts d, d.cities ct WHERE ct.name = 'MUMBAI' OR ct.name = 'CHENNAI'");
    queries.put("253","SELECT DISTINCT c1.name, s1.name, ct1.name FROM /exampleRegion c1, c1.states s1, s1.districts d1, d1.cities ct1, d1.getVillages() v1 WHERE v1.getName() = 'PUNJAB_VILLAGE1'");
    queries.put("254","SELECT DISTINCT d.getName(), d.getCities(), d.getVillages() FROM /exampleRegion c, c.states s, s.districts d WHERE d.name = 'MUMBAIDIST'");
    queries.put("255","SELECT DISTINCT e.key from /exampleRegion.entrySet e order by e.key.ID desc, e.key.pkid desc");
    queries.put("256","SELECT DISTINCT e.key from /exampleRegion.entrySet e order by e.key.ID, e.key.pkid desc");
    queries.put("257","SELECT DISTINCT e.key, e.value from /exampleRegion.entrySet e order by e.key.ID, e.value.status desc");
    queries.put("258","SELECT DISTINCT e.key.ID from /exampleRegion.entries e order by e.key.ID");
    queries.put("259","SELECT DISTINCT e.key.ID, e.value.status from /exampleRegion.entries e order by e.key.ID");
    queries.put("260","SELECT DISTINCT e.key.ID, e.value.status from /exampleRegion.entrySet e order by e.key.ID desc , e.value.status desc");
    queries.put("261","SELECT DISTINCT e.key.ID, e.value.status from /exampleRegion.entrySet e order by e.key.ID, e.value.status desc");
    queries.put("262","SELECT DISTINCT r.iD, p.value.secId FROM /exampleRegion r, getPositions('true') p where r.status = 'active' and r.iD = 0");
    queries.put("263","SELECT DISTINCT er.ID, value.secId FROM /exampleRegion er, getPositions value where er.status = 'active' and er.ID = 0");
    queries.put("264","SELECT DISTINCT key from /exampleRegion.keys key order by key.status");
    queries.put("265","SELECT DISTINCT key.ID from /exampleRegion.keys key order by key.ID");
    queries.put("266","SELECT DISTINCT key.ID, key.status from /exampleRegion.keys key order by key.status desc, key.ID");
    queries.put("267","SELECT DISTINCT key.ID, key.status from /exampleRegion.keys key order by key.status");
    queries.put("268","SELECT DISTINCT key.ID, key.status from /exampleRegion.keys key order by key.status, key.ID desc");
    queries.put("269","SELECT DISTINCT key.ID, key.status from /exampleRegion.keys key order by key.status, key.ID");
    queries.put("270","SELECT DISTINCT name FROM /exampleRegion , secIds name where length > 0");
    queries.put("271","SELECT DISTINCT p, pos from /exampleRegion p, p.positions.values pos order by p.ID");
    queries.put("272","SELECT DISTINCT p, pos from /exampleRegion p, p.positions.values pos order by p.ID, pos.secId");
    queries.put("273","SELECT DISTINCT p, pos from /exampleRegion p, p.positions.values pos order by pos.secId");
    queries.put("274","SELECT DISTINCT p.getID() from /exampleRegion p order by p.getID()");
    queries.put("275","SELECT DISTINCT p.ID from /exampleRegion p order by p.ID");
    queries.put("276","SELECT DISTINCT p.ID, p.status from /exampleRegion p order by p.ID");
    queries.put("277","SELECT DISTINCT p.ID from /exampleRegion p, p.positions.values order by p.ID");
    queries.put("278","SELECT DISTINCT p.ID, p.position1.secId from /exampleRegion p order by p.position1.secId");
    queries.put("279","SELECT DISTINCT p.ID, p.position1.secId from /exampleRegion p order by p.position1.secId, p.ID");
    queries.put("280","SELECT DISTINCT p.ID, p.status from /exampleRegion p order by p.ID desc, p.status asc");
    queries.put("281","SELECT DISTINCT p.ID, p.status from /exampleRegion p, p.positions.values order by p.status");
    queries.put("282","SELECT DISTINCT p.ID, p.status from /exampleRegion p, p.positions.values order by p.status, p.ID");
    queries.put("283","SELECT DISTINCT p.ID, pos.secId from /exampleRegion p, p.positions.values pos order by p.ID desc, pos.secId desc");
    queries.put("284","SELECT DISTINCT p.ID, pos.secId from /exampleRegion p, p.positions.values pos order by p.ID desc, pos.secId");
    queries.put("285","SELECT DISTINCT p.ID, pos.secId from /exampleRegion p, p.positions.values pos order by p.ID, pos.secId");
    queries.put("286","SELECT DISTINCT p.ID, pos.secId from /exampleRegion p, p.positions.values pos order by pos.secId");
    queries.put("287","SELECT DISTINCT p.ID, pos.secId from /exampleRegion p, p.positions.values pos order by pos.secId, p.ID");
    queries.put("288","SELECT DISTINCT p.key, p.value FROM /exampleRegion.entrySet p  WHERE p.value.ID <= 10 order by p.value.createTime asc limit 1");
    queries.put("289","SELECT DISTINCT p.key, p.value FROM /exampleRegion.entrySet p  WHERE p.value.ID <= 10 order by p.value.createTime desc limit 1");
    queries.put("290","SELECT DISTINCT p.names[1] from /exampleRegion p order by p.names[1]");
    queries.put("291","SELECT DISTINCT p.position1.secId, p.ID from /exampleRegion p order by p.position1.secId desc, p.ID");
    queries.put("292","SELECT DISTINCT p.position1.secId, p.ID from /exampleRegion p order by p.position1.secId");
    queries.put("293","SELECT DISTINCT p.status, p.ID from /exampleRegion p order by p.status asc, p.ID");
    queries.put("294","SELECT DISTINCT p.status, p.ID from /exampleRegion p order by p.status");
    queries.put("295","SELECT DISTINCT p.status, p.ID from /exampleRegion p order by p.status, p.ID");
    queries.put("296","SELECT DISTINCT pf FROM /exampleRegion pf,  positions.values pos where pf.description = 'XXXX'  and pos.secId= 'IBM'");
    queries.put("297","SELECT DISTINCT pos.secId FROM /exampleRegion er,  positions.values AS pos  WHERE er.ID > 0");
    queries.put("298","SELECT DISTINCT pos.secId FROM /exampleRegion, pos IN positions.values  WHERE pos.ID > 0");
    queries.put("299","SELECT DISTINCT pos.secId from /exampleRegion p, p.positions.values pos order by pos.secId");
    queries.put("300","SELECT DISTINCT s.name, s.getDistricts(), ct.getName() FROM /exampleRegion c, c.getStates() s, s.getDistricts() d, d.getCities() ct WHERE ct.getName() = 'PUNE' OR ct.name = 'CHANDIGARH' OR s.getName() = 'GUJARAT'");
    queries.put("301","SELECT DISTINCT status, ID from /exampleRegion order by status");
    queries.put("302","SELECT DISTINCT status, ID from /exampleRegion order by status, ID");
    queries.put("303","SELECT ID FROM /exampleRegion pf WHERE pf.ID = 1");
    queries.put("304","SELECT ID FROM /exampleRegion pf WHERE pf.ID > 10");
    queries.put("305","SELECT ID FROM /exampleRegion pf WHERE pf.ID > 5");
    queries.put("306","SELECT ID FROM /exampleRegion.keys key WHERE key.ID = 1");
    queries.put("307","SELECT ID FROM /exampleRegion.keys key WHERE key.ID > 5");
    queries.put("308","SELECT k.ID, k.status FROM /exampleRegion.keys k WHERE k.ID = 1 and k.status = 'active'");
    queries.put("309","SELECT key.ID FROM /exampleRegion.keys key WHERE key.ID > 5 and key.status = 'active'");
    queries.put("310","SELECT pkid FROM /exampleRegion ps WHERE ps.pkid = 'abc'");
    queries.put("311","Select * from /exampleRegion pf where pf.ID < 2");
    queries.put("312","Select * from /exampleRegion pf where pf.ID = 2");
    queries.put("313","Select * from /exampleRegion pf where pf.ID > 1");
    queries.put("314","Select * from /exampleRegion pf where pf.position1.secId > '2'");
    queries.put("315","Select * from /exampleRegion.keys key where key.ID = 2");
    queries.put("316","Select distinct  * from /exampleRegion.keySet keys where keys = '4'");
    queries.put("317","Select distinct  security from /exampleRegion  pos , secIds security where length > 2 and pos.ID > 0");
    queries.put("318","Select distinct * from /exampleRegion e, /exampleRegion a, e.getPhoneNo(a.zipCode) ea where e.name ='empName'");
    queries.put("319","Select distinct * from /exampleRegion pf where pf.getID() = -1");
    queries.put("320","Select distinct * from /exampleRegion pf where pf.getID() = -2147483648");
    queries.put("321","Select distinct * from /exampleRegion pf where pf.getID() = 3 and pf.getDoubleMinValue() = 4.9E-324");
    queries.put("322","Select distinct * from /exampleRegion pf where pf.getID() = 3 and pf.getFloatMinValue() = 1.4E-45f");
    queries.put("323","Select distinct * from /exampleRegion pf where pf.getID() = 3 and pf.getLongMinValue() = -9223372036854775808l");
    queries.put("324","Select distinct * from /exampleRegion pf, /exampleRegion e  where e.name ='empName' and pf.status='active'");
    queries.put("325","Select distinct * from /exampleRegion pf, /exampleRegion e  where e.name ='empName'");
    queries.put("326","Select distinct * from /exampleRegion pf, pf.positions.values pos where pf.status = 'active' and pos.secId = 'IBM'");
    queries.put("327","Select distinct * from /exampleRegion pf,/exampleRegion e  where pf.status='active'");
    queries.put("328","Select distinct * from /exampleRegion pf1 where pf1.getID() > 3");
    queries.put("329","Select distinct * from /exampleRegion pf1 where pf1.pkid > '3'");
    queries.put("330","Select distinct * from /exampleRegion, /exampleRegion");
    queries.put("331","Select distinct * from /exampleRegion.entrySet pf where pf.value.getID() = 4");
    queries.put("332","Select distinct e.value.secId from /exampleRegion, getPositions(23) e");
    queries.put("333","Select distinct ID from /exampleRegion");
    queries.put("334","Select distinct p from /exampleRegion p order by p");
    queries.put("335","Select distinct pf from /exampleRegion pf , pf.positions.values ps where ps.secId='SUN'");
    queries.put("336","Select distinct security from /exampleRegion , secIds security where security.length > 2 AND (security.intern <> 'SUN' AND security.intern <> 'DELL' )");
    queries.put("337","Select distinct security  from /exampleRegion, secIds security where security.length > 1");
    queries.put("338","Select distinct pos.secId from /exampleRegion , getPositions(23) pos");
    queries.put("339","Select distinct pos.secId from /exampleRegion, positions pos");
    queries.put("340","Select p.get('acc') from /exampleRegion p");
    queries.put("341","Select p.get('account') from /exampleRegion p");
    queries.put("342","Select p.shortID from /exampleRegion p where p.shortID < 5");
    queries.put("343","Select pf.ID from /exampleRegion pf where pf.ID > 2 and pf.ID < 100");
    queries.put("344","Select status from /exampleRegion pf where status='active'");
    queries.put("345","'a' IN SET('x','y','z')");
    queries.put("346","import com.gemstone.gemfire.cache.\"query\".data.Portfolio; select distinct * from /exampleRegion, (select distinct * from /exampleRegion p TYPE Portfolio, p.positions where value!=null)");
    queries.put("347","import com.gemstone.gemfire.cache.\"query\".data.Position;select distinct value.secId from /exampleRegion, (map<string, Position>)getPositions(23)");
    queries.put("348","select  * from /exampleRegion pf where pf.getID  > 1 and pf.getID < 12000");
    queries.put("349","select  * from /exampleRegion pf where pf.status != 'active' and pf.status != null");
    queries.put("350","select  distinct p.status  from /exampleRegion p  where   p.ID IN  SET( 0) AND p.createTime IN SET( 4l ) AND  p.\"type\" IN SET( 'type0') AND p.status IN SET( 'active')");
    queries.put("351","select  distinct p.status  from /exampleRegion p  where  (p.createTime IN SET( 10l ) OR  p.status IN SET( 'active') )AND  p.ID >  0 AND  p.createTime = 10l");
    queries.put("352","select  distinct p.status  from /exampleRegion p  where  (p.createTime IN SET( 10l ) OR  p.status IN SET( 'active') )AND  p.ID >  0");
    queries.put("353","select  distinct p.status  from /exampleRegion p  where  p.createTime = 10l AND  p.status IN SET( 'active') AND  true");
    queries.put("354","select  distinct p.status  from /exampleRegion p  where  p.createTime > 0 AND p.createTime <11 AND  p.ID IN  SET( 0)");
    queries.put("355","select  distinct p.status  from /exampleRegion p  where  p.ID = 11 AND   p.createTime IN  SET( 10L)");
    queries.put("356","select  distinct p.status  from /exampleRegion p  where  p.ID > 11 AND  p.ID < 19 and  p.createTime IN  SET( 10L)");
    queries.put("357","select  distinct p.status  from /exampleRegion p  where  p.ID > 11 AND  p.ID < 20 AND  p.createTime <>9L");
    queries.put("358","select  distinct p.status  from /exampleRegion p  where  true");
    queries.put("359","select  p.status as sts, p as pos from /exampleRegion p  where  p.ID IN  ( Select x.ID from /exampleRegion x where x.ID > 10)");
    queries.put("360","select  p.status as sts, p as pos from /exampleRegion p  where  p.ID IN  SET( 0,1,2,3,4,5)");
    queries.put("361","select  p.status as sts, p as pos from /exampleRegion p  where  p.ID IN  SET( 0,1,2,3,4,5,101,102,103,104,105) AND p.createTime > 9l");
    queries.put("362","select  p.status as sts, p as pos from /exampleRegion p  where ( p.ID IN  SET( 0,1,2,3) and p.createTime > 0L) OR (p.ID IN  SET( 2,3) and p.createTime > 5L)");
    queries.put("363","select  p.status as sts, p as pos from /exampleRegion p  where p.ID > 0 and p.createTime > 0");
    queries.put("364","select  p.status as sts, p as pos from /exampleRegion p  where p.ID IN  SET( 0,1,2,3) and p.createTime > 0");
    queries.put("365","select  p.status from /exampleRegion p  where p.ID > 0 and p.createTime > 0");
    queries.put("366","select  p.status from /exampleRegion p where p.ID > 0");
    queries.put("367","select  p.status from /exampleRegion p, p.positions pos where p.ID > 0");
    queries.put("368","select * from /exampleRegion p where NOT (p.ID IN SET(1, 2))");
    queries.put("369","select * from /exampleRegion sf where sf.shortField < 10");
    queries.put("370","select * from /exampleRegion this where FALSE");
    queries.put("371","select * from /exampleRegion this where NOT isActive");
    queries.put("372","select * from /exampleRegion this where NULL");
    queries.put("373","select * from /exampleRegion this where P1.secId = 'SUN'");
    queries.put("374","select * from /exampleRegion this where TRUE");
    queries.put("375","select * from /exampleRegion this where UNDEFINED");
    queries.put("376","select * from /exampleRegion this where ID < 1");
    queries.put("377","select * from /exampleRegion this where ID < 5");
    queries.put("378","select * from /exampleRegion this where ID <= 1");
    queries.put("379","select * from /exampleRegion this where ID <= 5");
    queries.put("380","select * from /exampleRegion this where ID = 0 OR ID = 1");
    queries.put("381","select * from /exampleRegion this where ID = 5");
    queries.put("382","select * from /exampleRegion this where ID > 2");
    queries.put("383","select * from /exampleRegion this where ID > 4 AND ID < 9");
    queries.put("384","select * from /exampleRegion this where ID > 5 and ID <=15");
    queries.put("385","select * from /exampleRegion this where ID >= 2");
    queries.put("386","select * from /exampleRegion this where isActive");
    queries.put("387","select * from /exampleRegion this where isActive()");
    queries.put("388","select * from /exampleRegion this where pk = '2'");
    queries.put("389","select * from /exampleRegion this where pk = '7'");
    queries.put("390","select * from /exampleRegion this where status <> 'active'");
    queries.put("391","select * from /exampleRegion this where status = 'active' AND ( ID = 1 OR P1.secId = 'SUN')");
    queries.put("392","select * from /exampleRegion this where status = 'active' AND ID = 0");
    queries.put("393","select * from /exampleRegion this where status = 'active' AND ID = 1");
    queries.put("394","select * from /exampleRegion this where status = 'active' OR ID = 1");
    queries.put("395","select * from /exampleRegion this where status = 'active'");
    queries.put("396","select * from /exampleRegion this where testMethod(true)");
    queries.put("397","select * from /exampleRegion this where this = 'value'");
    queries.put("398","select * from /exampleRegion this where toString()='doOpDuringBucketRemove.VALUE'");
    queries.put("399","select * from /exampleRegion where 3 >= length");
    queries.put("400","select * from /exampleRegion where age < 50");
    queries.put("401","select * from /exampleRegion where intVar = 0");
    queries.put("402","select * from /exampleRegion.entries sf where sf.value.shortField < 10");
    queries.put("403","select ALL * from /exampleRegion where 3 >= length");
    queries.put("404","select distinct  key.status as st from /exampleRegion key where key.ID > 5 order by key.status");
    queries.put("405","select distinct  p.position1.secId  as st from /exampleRegion p order by p.position1.secId");
    queries.put("406","select distinct  p.status, p.ID from /exampleRegion p order by p.status");
    queries.put("407","select distinct  status, ID from /exampleRegion order by status");
    queries.put("408","select distinct 'a' from /exampleRegion p");
    queries.put("409","select distinct 'a',1, p from /exampleRegion p");
    queries.put("410","select distinct 'a',1, p from UNDEFINED");
    queries.put("411","select distinct 'a',1, p from null");
    queries.put("412","select distinct * from /exampleRegion a, /exampleRegion p, /exampleRegion e, a.street s  where s.street ='DPStreet1'");
    queries.put("413","select distinct * from /exampleRegion a, /exampleRegion p, /exampleRegion e, a.street s where p.status='active' and s.street ='DPStreet1'");
    queries.put("414","select distinct * from /exampleRegion itr1,itr1.phoneNo itr2,itr1.street itr3 where itr2.mobile>333");
    queries.put("415","select distinct * from /exampleRegion itr1,itr1.street itr2,itr1.phoneNo itr3 where itr3.mobile>333");
    queries.put("416","select distinct * from /exampleRegion p order by p.getID()");
    queries.put("417","select distinct * from /exampleRegion p order by p.getP1().secId");
    queries.put("418","select distinct * from /exampleRegion p order by status, ID desc");
    queries.put("419","select distinct * from /exampleRegion p where p.ID > 1   and  p.ID < 3");
    queries.put("420","select distinct * from /exampleRegion p");
    queries.put("421","select distinct * from /exampleRegion p, /exampleRegion e  where p.pkid = '1'");
    queries.put("422","select distinct * from /exampleRegion p, /exampleRegion e");
    queries.put("423","select distinct * from /exampleRegion p, /exampleRegion e, /exampleRegion a, a.street s where s.street ='DPStreet1'");
    queries.put("424","select distinct * from /exampleRegion p, /exampleRegion p1 where p.position1.Id = p1.position1.Id and p1.position1.secId in set('MSFT')");
    queries.put("425","select distinct * from /exampleRegion p, p.positions where p.ID = 1");
    queries.put("426","select distinct * from /exampleRegion p, p.positions,/exampleRegion e where p.ID =  e.empId");
    queries.put("427","select distinct * from /exampleRegion p, p.positions,/exampleRegion e, /exampleRegion p1 where p.ID = 1 and p1.ID = 2 and e.empId = 1");
    queries.put("428","select distinct * from /exampleRegion p, p.positions,/exampleRegion e, /exampleRegion p1 where p.ID =p1.ID   and e.empId = 1 and p1.status = 'active' and p.status='active'");
    queries.put("429","select distinct * from /exampleRegion p, p.positions,/exampleRegion e, /exampleRegion p1 where p.ID =p1.ID   and e.empId = p1.ID");
    queries.put("430","select distinct * from /exampleRegion p, p.positions.values order by p.ID");
    queries.put("431","select distinct * from /exampleRegion p, p.positions.values val order by p.ID, val.secId desc");
    queries.put("432","select distinct * from /exampleRegion p, p.positions.values where p.ID < 3");
    queries.put("433","select distinct * from /exampleRegion p, p.positions.values where p.pkid != '53'");
    queries.put("434","select distinct * from /exampleRegion pf where false and ID = 0");
    queries.put("435","select distinct * from /exampleRegion pf where ID = 0 or false");
    queries.put("436","select distinct * from /exampleRegion pf where nvl(pf.position2, pf.position1).secId = 'SUN'");
    queries.put("437","select distinct * from /exampleRegion pf where nvl(pf.position2,'foundNull') = 'foundNull'");
    queries.put("438","select distinct * from /exampleRegion pf where true and ID = 0");
    queries.put("439","select distinct * from /exampleRegion pf where true or ID = 0");
    queries.put("440","select distinct * from /exampleRegion pf, positions.values pos  where  (pf.ID > 1 or pf.status = 'active') or (false AND pos.secId ='IBM')");
    queries.put("441","select distinct * from /exampleRegion pf, positions.values pos  where true = true and pf.ID > 1 and pos.secId ='IBM'");
    queries.put("442","select distinct * from /exampleRegion pf, positions.values pos where  (pf.ID > 1 or pf.status = 'active') or (true AND pos.secId ='IBM')");
    queries.put("443","select distinct * from /exampleRegion pf, positions.values pos where  (true AND pos.secId ='SUN') or (pf.ID > 1 and pf.status != 'active')");
    queries.put("444","select distinct * from /exampleRegion pf, positions.values pos where (ID = 2 or false) or (pf.status = 'active' and (pos.secId != 'IBM' or true))");
    queries.put("445","select distinct * from /exampleRegion pf, positions.values pos where (pf.ID < 1 and pf.status = 'active') and (false or pos.secId = 'IBM')");
    queries.put("446","select distinct * from /exampleRegion pf, positions.values pos where false and pos.secId ='IBM'");
    queries.put("447","select distinct * from /exampleRegion pf, positions.values pos where true = false and pf.ID > 1 and pos.secId ='IBM'");
    queries.put("448","select distinct * from /exampleRegion pf, positions.values pos where true = true and pf.ID > 1 or pos.secId ='IBM'");
    queries.put("449","select distinct * from /exampleRegion pf, positions.values pos where true = true and pos.secId ='IBM'");
    queries.put("450","select distinct * from /exampleRegion s  where 3 >= s.length");
    queries.put("451","select distinct * from /exampleRegion where 3 >= length");
    queries.put("452","select distinct * from /exampleRegion where NOT isActive");
    queries.put("453","select distinct * from /exampleRegion where ID != 53");
    queries.put("454","select distinct * from /exampleRegion where ID < 1");
    queries.put("455","select distinct * from /exampleRegion where ID >= 2");
    queries.put("456","select distinct * from /exampleRegion where isActive");
    queries.put("457","select distinct * from /exampleRegion where isActive()");
    queries.put("458","select distinct * from /exampleRegion where status <> 'active'");
    queries.put("459","select distinct * from /exampleRegion where status = 'active' AND ( ID = 1 OR P1.secId = 'SUN')");
    queries.put("460","select distinct * from /exampleRegion where status = 'active' AND ID = 0");
    queries.put("461","select distinct * from /exampleRegion where status = 'active' AND ID = 1");
    queries.put("462","select distinct * from /exampleRegion where status = 'active' OR ID = 1");
    queries.put("463","select distinct * from /exampleRegion where status = 'active'");
    queries.put("464","select distinct * from /exampleRegion where testMethod(true)");
    queries.put("465","select distinct * from /exampleRegion x, x.positions.values where x.pk = '1'");
    queries.put("466","select distinct * from /exampleRegion x, x.positions.values where x.pkid = '1'");
    queries.put("467","select distinct * from /exampleRegion");
    queries.put("468","select distinct * from /exampleRegion.entries where key = '1'");
    queries.put("469","select distinct * from /exampleRegion.entries where value.ID = 1 and key = '1'");
    queries.put("470","select distinct * from /exampleRegion.entries where value.status = 'active'");
    queries.put("471","select distinct * from /exampleRegion.entries x, x.value.positions.values where x.key = '1'");
    queries.put("472","select distinct * from /exampleRegion.entries x, x.value.positions.values where x.value.pkid = '1'");
    queries.put("473","select distinct * from /exampleRegion.keySet where toString = '1'");
    queries.put("474","select distinct * from /exampleRegion.keys where toString = '1'");
    queries.put("475","select distinct * from /exampleRegion.values where ID = 1");
    queries.put("476","select distinct * from /exampleRegion.values where status = 'active'");
    queries.put("477","select distinct * from null");
    queries.put("478","select distinct 1 from /exampleRegion p");
    queries.put("479","select distinct 1 from UNDEFINED");
    queries.put("480","select distinct 1 from null");
    queries.put("481","select distinct e.key from /exampleRegion.entrySet e order by e.key.ID desc, e.key.pkid desc");
    queries.put("482","select distinct e.key, e.value from /exampleRegion.entrySet e order by e.key.ID, e.value.status desc");
    queries.put("483","select distinct e.key.ID from /exampleRegion.entries e order by e.key.ID");
    queries.put("484","select distinct e.key.ID, e.value.status from /exampleRegion.entries e order by e.key.ID");
    queries.put("485","select distinct e.key.ID, e.value.status from /exampleRegion.entrySet e order by e.key.ID desc , e.value.status desc");
    queries.put("486","select distinct getID, status from /exampleRegion pf where getID < 10 order by getID asc");
    queries.put("487","select distinct getID, status from /exampleRegion pf where getID < 10 order by getID desc");
    queries.put("488","select distinct iD, status from /exampleRegion order by iD");
    queries.put("489","select distinct key from /exampleRegion.entrySet , value.positions.values   where value.ID = 1 and key = '1'");
    queries.put("490","select distinct key.ID from /exampleRegion.keys key order by key.ID");
    queries.put("491","select distinct key.ID, key.status from /exampleRegion.keys key order by key.status desc, key.ID");
    queries.put("492","select distinct key.ID, key.status from /exampleRegion.keys key order by key.status, key.ID desc");
    queries.put("493","select distinct key.ID, key.status from /exampleRegion.keys key order by key.status, key.ID");
    queries.put("494","select distinct key.ID,key.status as st from /exampleRegion key where key.status = 'inactive' order by key.status desc, key.ID");
    queries.put("495","select distinct nm from /exampleRegion prt,names nm where prt.ID>0");
    queries.put("496","select distinct nvl(pf.position2, 'inProjection') from /exampleRegion pf where nvl(pf.position2, pf.position1).secId = 'SUN'");
    queries.put("497","select distinct nvl(pf.position2, 'inProjection') from /exampleRegion pf where nvl(pf.position2,'foundNull') = 'foundNull'");
    queries.put("498","select distinct p from /exampleRegion p where p.ID > 0");
    queries.put("499","select distinct p, pos from /exampleRegion p, p.positions.values pos order by p.ID");
    queries.put("500","select distinct p.ID, pos.secId from /exampleRegion p, p.positions.values pos order by pos.secId, p.ID");
    queries.put("501","select distinct p.names[1] from /exampleRegion p order by p.names[1]");
    queries.put("502","select distinct p.position1.secId as st from /exampleRegion p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId");
    queries.put("503","select distinct p.position1.secId, p.ID from /exampleRegion p order by p.position1.secId");
    queries.put("504","select distinct p.status as STATUS, SECID: p.P1.SecId, ID from /exampleRegion p ");
    queries.put("505","select distinct p.status as st from /exampleRegion p where ID > 0 and status = 'inactive' order by p.status");
    queries.put("506","select distinct p.status from /exampleRegion p order by p.status");
    queries.put("507","select distinct p.status from /exampleRegion p where p.ID > 0");
    queries.put("508","select distinct p.status, p.ID from /exampleRegion p order by p.status asc, p.ID");
    queries.put("509","select distinct p.status, p.ID from /exampleRegion p order by p.status, p.ID");
    queries.put("510","select distinct p.status,p.ID from /exampleRegion p where p.ID > 0");
    queries.put("511","select distinct portfolio: p ,p.P1.SecId from /exampleRegion p where p.ID > 0 ");
    queries.put("512","select distinct pos.secId from /exampleRegion p, p.positions.values pos order by pos.secId");
    queries.put("513","select distinct positions.values.toArray[0], positions.values.toArray[0],status from /exampleRegion");
    queries.put("514","select distinct prt from /exampleRegion prt, names where prt.names[3]='ddd'");
    queries.put("515","select distinct status as st from /exampleRegion where ID > 0 order by status");
    queries.put("516","select distinct status from /exampleRegion where ID > 0 order by status");
    queries.put("517","select distinct status, getID from /exampleRegion pf where getID < 10 order by status asc, getID desc");
    queries.put("518","select distinct status, ID from /exampleRegion order by status, ID");
    queries.put("519","SELECT * FROM /exampleRegion WHERE \"type\" = 'type1'");
    queries.put("520","select count(*) from /exampleRegion");
    queries.put("521","SELECT * from /exampleRegion P1, P1.positions.values WHERE P1.ID = 5");
    queries.put("522","SELECT * from /exampleRegion P1, /exampleRegion2 P2 WHERE P1.ID = 5");
    queries.put("523","SELECT * from /numericRegion n WHERE n.max1 = 50");
    queries.put("524","SELECT * from /numericRegion n WHERE n.max1 = 50.0");
    queries.put("525","SELECT * from /numericRegion n WHERE n.max1 = 50.0f");
    queries.put("526","SELECT * from /numericRegion n WHERE n.max1 = 50.0d");
    queries.put("527","SELECT * from /numericRegion n WHERE n.max1 > 50");
    queries.put("528","SELECT * from /numericRegion n WHERE n.max1 > 50.0");
    queries.put("529","SELECT * from /numericRegion n WHERE n.max1 > 50.0f");
    queries.put("530","SELECT * from /numericRegion n WHERE n.max1 > 50.0d");
    queries.put("531","SELECT * from /numericRegion n WHERE n.max1 < 50");
    queries.put("532","SELECT * from /numericRegion n WHERE n.max1 < 50.0");
    queries.put("533","SELECT * from /numericRegion n WHERE n.max1 < 50.0f");
    queries.put("534","SELECT * from /numericRegion n WHERE n.max1 < 50.0d");    
    queries.put("535","SELECT * from /numericRegion n WHERE n.max1 <= 50");
    queries.put("536","SELECT * from /numericRegion n WHERE n.max1 <= 50.0");
    queries.put("537","SELECT * from /numericRegion n WHERE n.max1 <= 50.0f");
    queries.put("538","SELECT * from /numericRegion n WHERE n.max1 <= 50.0d");   
    queries.put("539","SELECT * from /numericRegion n WHERE n.max1 >= 50");
    queries.put("540","SELECT * from /numericRegion n WHERE n.max1 >= 50.0");
    queries.put("541","SELECT * from /numericRegion n WHERE n.max1 >= 50.0f");
    queries.put("542","SELECT * from /numericRegion n WHERE n.max1 >= 50.0d");   
    queries.put("543", "select distinct * from /portfolios p where p.ID > -1 order by p.ID");
    queries.put("544", "select distinct * from /portfolios p where p.ID > -1 order by p.ID limit 100");
    queries.put("545", "select distinct * from /portfolios p where p.ID > -1");
    queries.put("546", "select distinct * from /portfolios p where p.ID > -1 limit 100");
    
    
    //NESTED QUERIES
    queries.put("701","SELECT ID, status FROM /exampleRegion portfolio WHERE NOT (SELECT DISTINCT * FROM portfolio.positions.values positions WHERE positions.secId='AOL' OR positions.secId='SAP').isEmpty");
    queries.put("702","SELECT * FROM /exampleRegion p where p.status = ELEMENT(SELECT DISTINCT * FROM /exampleRegion p2 WHERE p2.ID = 0).status");
    queries.put("703","IMPORT com.gemstone.gemfire.cache.\"query\".data.Portfolio; SELECT * FROM /exampleRegion, (SELECT DISTINCT * from /exampleRegion p TYPE Portfolio, p.positions WHERE value!=null)");
    queries.put("704","SELECT DISTINCT * FROM (SELECT DISTINCT * FROM /exampleRegion portfolios, positions pos) WHERE pos.value.secId = 'IBM'");
    queries.put("705","SELECT * FROM /exampleRegion portfolio WHERE portfolio.ID IN (SELECT p2.ID  FROM /exampleRegion2 p2 where p2.ID > 1)");
    queries.put("706","SELECT * FROM /root/exampleRegion p where NOT(SELECT DISTINCT * FROM positions.values pos  WHERE pos.secId in SET('YHOO', 'SUN', 'IBM', 'YHOO', 'GOOG',  'MSFT', 'AOL', 'APPL', 'ORCL', 'SAP', 'DELL', 'RHAT', 'NOVL', 'HP')).isEmpty");
    queries.put("707","SELECT DISTINCT * FROM /exampleRegion pf, (SELECT DISTINCT * FROM /exampleRegion ptf, ptf.positions pos where pf.ID != 1 and pos.value.sharesOutstanding > 2000) as x WHERE x.secId = 'IBM'");
    queries.put("708","SELECT DISTINCT * FROM /exampleRegion pf, (SELECT DISTINCT * FROM /exampleRegion ptf, ptf.positions pos where pf.ID != 1 and pos.value.sharesOutstanding > 2000)as y WHERE y.value.secId = 'HP'");
    queries.put("709","SELECT DISTINCT * FROM /exampleRegion where NOT(SELECT DISTINCT * FROM /exampleRegion p where p.ID = 0).isEmpty");
    queries.put("710","SELECT DISTINCT * FROM /root/exampleRegion p, (SELECT DISTINCT pos FROM /root/exampleRegion x, x.positions.values pos WHERE  x.ID = p.ID) as itrX");
    queries.put("711","SELECT DISTINCT * FROM /root/exampleRegion p, p.positions.values outerPos, (SELECT DISTINCT key from /exampleRegion.keys key)");
    queries.put("712","SELECT DISTINCT c.name, s.name, ct.name FROM /exampleRegion c, c.states s, (SELECT DISTINCT * FROM /exampleRegion c, c.states s, s.districts d, d.cities ct WHERE s.name = 'PUNJAB') itr1, s.districts d, d.cities ct WHERE ct.name = 'CHANDIGARH'");
    queries.put("713","SELECT DISTINCT c.name, s.name, ct.name FROM /exampleRegion c, c.states s, s.districts d, d.cities ct WHERE ct.name = element (SELECT DISTINCT ct.name FROM /exampleRegion c, c.states s, s.districts d, d.cities ct WHERE s.name = 'MAHARASHTRA' AND ct.name = 'PUNE')");
    queries.put("714","SELECT DISTINCT c1.name, s1.name, ct1.name FROM /exampleRegion c1, c1.states s1, (SELECT DISTINCT * FROM /exampleRegion2 c2, c2.states s2, s2.districts d2, d2.cities ct2 WHERE s2.name = 'PUNJAB') itr1, s1.districts d1, d1.cities ct1 WHERE ct1.name = 'CHANDIGARH'");
    queries.put("715","SELECT DISTINCT c1.name, s1.name, ct1.name FROM /exampleRegion c1, c1.states s1, s1.districts d1, d1.cities ct1 WHERE ct1.name = element (SELECT DISTINCT ct3.name FROM /exampleRegion3 c3, c3.states s3, s3.districts d3, d3.cities ct3 WHERE s3.name = 'MAHARASHTRA' AND ct3.name = 'PUNE')");
    queries.put("716","Select distinct * from /exampleRegion pfos, pfos.positions.values outerPos, (SELECT DISTINCT key");
    queries.put("717","Select distinct structset.sos, structset.key from /exampleRegion pfos, pfos.positions.values outerPos, "
    			+ "(SELECT DISTINCT key: key, sos: pos.sharesOutstanding from /exampleRegion.entries pf, pf.value.positions.values pos "
    			+ "where outerPos.secId != 'IBM' AND pf.key IN (select distinct * from pf.value.collectionHolderMap['0'].arr)) structset "
    			+ "where structset.sos > 2000");
    queries.put("718","select  DISTINCT * from  ( SELECT   DISTINCT p.ID as portfolio_id, pos.secId as sec_id from /exampleRegion p , p.positions.values pos where p.status= 'active')");
    queries.put("719","select distinct * from /exampleRegion p, (select distinct pos  as poos from /exampleRegion x, x.positions.values pos where pos.secId = 'YHOO') as k");
    queries.put("720","select distinct * from /exampleRegion p, (select distinct pos as poos from /exampleRegion x, p.positions.values pos where x.ID = p.ID) as k");
    queries.put("721","select distinct * from /exampleRegion p, (select distinct val from positions.values as val where val.secId = 'YHOO') as k");
    queries.put("722","select distinct * from /exampleRegion p, (select distinct x as pf , myPos as poos from /exampleRegion x, x.positions.values as myPos) as k   where k.poos.secId = 'YHOO'");
    queries.put("723","select distinct * from /exampleRegion p, (select distinct x.ID as ID  from /exampleRegion x where x.ID = p.ID) as k");
        
    //STRING OPERATIONS
    queries.put("800","SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like  '%'");
    queries.put("801","SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like  'a%bc%'");
    queries.put("802","SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like  'abc%' AND ps.ID > 2 AND ps.ID < 150");
    queries.put("803","SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like  'abc%' OR ps.ID > 6");
    queries.put("804","SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like  'abc%'");
    queries.put("805","SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like  'abcd'");
    queries.put("806","SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like  'abcd\\%'");
    queries.put("807","SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like  'abcd\\_'");
    queries.put("808","SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like 'a_bc'");
    queries.put("809","SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like 'abc_'");
    queries.put("810","SELECT pkid FROM /exampleRegion ps WHERE ps.pkid like '%b%' and ps.pkid like '_bc'");
    queries.put("811","SELECT pkid FROM /exampleRegion ps WHERE ps.pkid like '%b%' and ps.status = 'like'");
    queries.put("812","SELECT pkid FROM /exampleRegion ps WHERE ps.pkid like '%b%' and ps.status like '%ike'");
    queries.put("813","SELECT pkid FROM /exampleRegion ps WHERE ps.pkid like 'abc%'");
    queries.put("814","SELECT pkid FROM /exampleRegion ps WHERE ps.pkid like 'ml%' or ps.status = 'like'");
    queries.put("815","select p from /exampleRegion.values p where p like '%%ti%'");
    queries.put("816","select p from /exampleRegion.values p where p like '%'");
    queries.put("817","select p from /exampleRegion.values p where p like '%c%iv%'");
    queries.put("818","select p from /exampleRegion.values p where p like '%ctiv%'");
    queries.put("819","select p from /exampleRegion.values p where p like '%ctive'");
    queries.put("820","select p from /exampleRegion.values p where p like '1inact\\_+ive'");
    queries.put("821","select p from /exampleRegion.values p where p like '?act?ve'");
    queries.put("822","select p from /exampleRegion.values p where p like 'X%X'");
    queries.put("823","select p from /exampleRegion.values p where p like 'X__X'");
    queries.put("824","select p from /exampleRegion.values p where p like 'Y\\%Y'");
    queries.put("825","select p from /exampleRegion.values p where p like 'Z\\\\%Z'");
    queries.put("826","select p from /exampleRegion.values p where p like '^+act.ve+^'");
    queries.put("827","select p from /exampleRegion.values p where p like '__tive'");
    queries.put("828","select p from /exampleRegion.values p where p like '_c_iv_'");
    queries.put("829","select p from /exampleRegion.values p where p like '_ctiv%'");
    queries.put("830","select p from /exampleRegion.values p where p like '_ctive'");
    queries.put("831","select p from /exampleRegion.values p where p like 'a%e'");
    queries.put("832","select p from /exampleRegion.values p where p like 'a%iv_'");
    queries.put("833","select p from /exampleRegion.values p where p like 'a_tiv%'");
    queries.put("834","select p from /exampleRegion.values p where p like 'ac%'");
    queries.put("835","select p from /exampleRegion.values p where p like 'ac(tiv)e'");
    queries.put("836","select p from /exampleRegion.values p where p like 'ac+t+ve'");
    queries.put("837","select p from /exampleRegion.values p where p like 'ac\\%'");
    queries.put("838","select p from /exampleRegion.values p where p like 'ac\\tive'");
    queries.put("839","select p from /exampleRegion.values p where p like 'ac_ive'");
    queries.put("840","select p from /exampleRegion.values p where p like 'ac_tive'");
    queries.put("841","select p from /exampleRegion.values p where p like 'act%%ve'");
    queries.put("842","select p from /exampleRegion.values p where p like 'act()ive'");
    queries.put("843","select p from /exampleRegion.values p where p like 'act)ve^'");
    queries.put("844","select p from /exampleRegion.values p where p like 'act**ve'");
    queries.put("845","select p from /exampleRegion.values p where p like 'act*+|ve'");
    queries.put("846","select p from /exampleRegion.values p where p like 'act/exampleRegion'");
    queries.put("847","select p from /exampleRegion.values p where p like 'act[]ve'");
    queries.put("848","select p from /exampleRegion.values p where p like 'act][ve'");
    queries.put("849","select p from /exampleRegion.values p where p like 'act^[a-z]ve'");
    queries.put("850","select p from /exampleRegion.values p where p like 'act__e'");
    queries.put("851","select p from /exampleRegion.values p where p like 'activ_'");
    queries.put("852","select p from /exampleRegion.values p where p like 'active'");
    queries.put("853","select p from /exampleRegion.values p where p like 'acxtxve'");
    queries.put("854","select p from /exampleRegion.values p where p like 'inact\\%+ive'");
    queries.put("855","select p from /exampleRegion.values p where p like 'inactive'");
    queries.put("856","select p from /exampleRegion.values p where p like '|+act(ve'");
    queries.put("857","SELECT  *  FROM /exampleRegion ps WHERE ps.status like '%b%' and ps.pkid = '1'");
    queries.put("858","SELECT  *  FROM /exampleRegion ps WHERE ps.status like '%b%' or ps.ID > 0");
    queries.put("859","SELECT  *  FROM /exampleRegion ps WHERE ps.status like '%b%' or ps.pkid = '2'");
    queries.put("860","SELECT  *  FROM /exampleRegion ps WHERE ps.status like '%b%'");
    queries.put("861","SELECT  *  FROM /exampleRegion ps WHERE ps.status like '_b_' or ps.pkid = '2'");
    queries.put("862","SELECT * FROM /exampleRegion ps WHERE ps.pkid like '%b%' and ps.status like '%ctiv%'");
    queries.put("863","SELECT * FROM /exampleRegion ps WHERE ps.pkid like '_bc'");
    queries.put("864","SELECT distinct *  FROM /exampleRegion ps WHERE ps.pkid like '%b%'");
    
    //LIMIT
    queries.put("900","SELECT DISTINCT  key.ID, key.status as st from /exampleRegion.keys key where key.status = 'inactive' order by key.status desc, key.ID LIMIT 1");
    queries.put("901","SELECT DISTINCT  key.ID, key.status as st from /exampleRegion.keys key where key.status = 'inactive' order by key.status desc, key.ID LIMIT 9");
    queries.put("902","SELECT DISTINCT  key.status as st, key.ID from /exampleRegion.keys key where key.ID > 5 order by key.status, key.ID desc LIMIT 1");
    queries.put("903","SELECT DISTINCT  key.status as st, key.ID from /exampleRegion.keys key where key.ID > 5 order by key.status, key.ID desc LIMIT 4");
    queries.put("904","SELECT DISTINCT  key.status as st, key.ID from /exampleRegion.keys key where key.ID > 5 order by key.status, key.ID desc LIMIT 9");
    queries.put("905","SELECT DISTINCT  p.position1.secId , p.ID as st from /exampleRegion p order by p.position1.secId, p.ID LIMIT 1");
    queries.put("906","SELECT DISTINCT  p.position1.secId , p.ID as st from /exampleRegion p order by p.position1.secId, p.ID LIMIT 9");
    queries.put("907","SELECT DISTINCT * from /exampleRegion p order by p.getID() LIMIT 9");
    queries.put("908","SELECT DISTINCT * from /exampleRegion p, p.positions.values val order by p.ID, val.secId LIMIT 1");
    queries.put("909","SELECT DISTINCT * from /exampleRegion p, p.positions.values val order by p.ID, val.secId LIMIT 9");
    queries.put("910","SELECT DISTINCT e.key from /exampleRegion.entrySet e order by e.key.ID, e.key.pkid desc LIMIT 1");
    queries.put("911","SELECT DISTINCT e.key from /exampleRegion.entrySet e order by e.key.ID, e.key.pkid desc LIMIT 4");
    queries.put("912","SELECT DISTINCT e.key from /exampleRegion.entrySet e order by e.key.ID, e.key.pkid desc LIMIT 9");
    queries.put("913","SELECT DISTINCT e.key.ID, e.value.status from /exampleRegion.entrySet e order by e.key.ID, e.value.status desc LIMIT 1");
    queries.put("914","SELECT DISTINCT e.key.ID, e.value.status from /exampleRegion.entrySet e order by e.key.ID, e.value.status desc LIMIT 4");
    queries.put("915","SELECT DISTINCT e.key.ID, e.value.status from /exampleRegion.entrySet e order by e.key.ID, e.value.status desc LIMIT 9");
    queries.put("916","SELECT DISTINCT iD, status from /exampleRegion order by iD LIMIT 1");
    queries.put("917","SELECT DISTINCT iD, status from /exampleRegion order by iD LIMIT 4");
    queries.put("918","SELECT DISTINCT iD, status from /exampleRegion order by iD LIMIT 9");
    queries.put("919","SELECT DISTINCT iD, status from /exampleRegion order by iD");
    queries.put("920","SELECT DISTINCT key from /exampleRegion.keys key order by key.status, key.ID LIMIT 1");
    queries.put("921","SELECT DISTINCT key from /exampleRegion.keys key order by key.status, key.ID LIMIT 4");
    queries.put("922","SELECT DISTINCT key from /exampleRegion.keys key order by key.status, key.ID LIMIT 9");
    queries.put("923","SELECT DISTINCT key.ID from /exampleRegion.keys key order by key.ID LIMIT 1");
    queries.put("924","SELECT DISTINCT key.ID from /exampleRegion.keys key order by key.ID LIMIT 4");
    queries.put("925","SELECT DISTINCT key.ID from /exampleRegion.keys key order by key.ID LIMIT 9");
    queries.put("926","SELECT DISTINCT key.ID, key.status from /exampleRegion.keys key order by key.status desc, key.ID LIMIT 9");
    queries.put("927","SELECT DISTINCT key.ID, key.status from /exampleRegion.keys key order by key.status, key.ID asc LIMIT 1");
    queries.put("928","SELECT DISTINCT key.ID, key.status from /exampleRegion.keys key order by key.status, key.ID asc LIMIT 4");
    queries.put("929","SELECT DISTINCT key.ID, key.status from /exampleRegion.keys key order by key.status, key.ID asc LIMIT 9");
    queries.put("930","SELECT DISTINCT p, pos from /exampleRegion p, p.positions.values pos order by p.ID, pos.secId desc LIMIT 1");
    queries.put("931","SELECT DISTINCT p, pos from /exampleRegion p, p.positions.values pos order by p.ID, pos.secId desc LIMIT 4");
    queries.put("932","SELECT DISTINCT p, pos from /exampleRegion p, p.positions.values pos order by p.ID, pos.secId desc LIMIT 9");
    queries.put("933","SELECT DISTINCT p, pos from /exampleRegion p, p.positions.values pos order by pos.secId, p.ID LIMIT 1");
    queries.put("934","SELECT DISTINCT p, pos from /exampleRegion p, p.positions.values pos order by pos.secId, p.ID LIMIT 4");
    queries.put("935","SELECT DISTINCT p, pos from /exampleRegion p, p.positions.values pos order by pos.secId, p.ID LIMIT 9");
    queries.put("936","SELECT DISTINCT p.ID, p.status from /exampleRegion p order by p.ID LIMIT 9");
    queries.put("937","SELECT DISTINCT p.position1.secId as st, p.ID as ied from /exampleRegion p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId, p.ID LIMIT 1");
    queries.put("938","SELECT DISTINCT p.position1.secId as st, p.ID as ied from /exampleRegion p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId, p.ID LIMIT 4");
    queries.put("939","SELECT DISTINCT p.position1.secId as st, p.ID as ied from /exampleRegion p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId, p.ID LIMIT 9");
    queries.put("940","SELECT DISTINCT p.position1.secId, p.ID from /exampleRegion p order by p.position1.secId, p.ID desc LIMIT 1");
    queries.put("941","SELECT DISTINCT p.position1.secId, p.ID from /exampleRegion p order by p.position1.secId, p.ID desc LIMIT 4");
    queries.put("942","SELECT DISTINCT p.position1.secId, p.ID from /exampleRegion p order by p.position1.secId, p.ID desc LIMIT 9");
    queries.put("943","SELECT DISTINCT p.status as st, p.ID as ID from /exampleRegion p where ID > 0 and status = 'inactive' order by p.status, p.ID desc LIMIT 1");
    queries.put("944","SELECT DISTINCT p.status as st, p.ID as ID from /exampleRegion p where ID > 0 and status = 'inactive' order by p.status, p.ID desc LIMIT 4");
    queries.put("945","SELECT DISTINCT p.status as st, p.ID as ID from /exampleRegion p where ID > 0 and status = 'inactive' order by p.status, p.ID desc LIMIT 9");
    queries.put("946","SELECT DISTINCT p.status from /exampleRegion p order by p.status LIMIT 9");
    queries.put("947","SELECT DISTINCT p.status, p.ID from /exampleRegion p order by p.status asc, p.ID LIMIT 1");
    queries.put("948","SELECT DISTINCT p.status, p.ID from /exampleRegion p order by p.status asc, p.ID LIMIT 4");
    queries.put("949","SELECT DISTINCT p.status, p.ID from /exampleRegion p order by p.status asc, p.ID LIMIT 9");
    queries.put("950","SELECT DISTINCT pf.ID , pf.createTime FROM /exampleRegion  pf WHERE pf.ID > 10 limit 5");
    queries.put("951","SELECT DISTINCT pf.ID FROM /exampleRegion  pf WHERE pf.ID > 10 limit 5");
    queries.put("952","SELECT DISTINCT pf.ID FROM /exampleRegion pf WHERE pf.ID > 0 limit 5");
    queries.put("953","SELECT DISTINCT pf.ID, pf.createTime FROM /exampleRegion pf WHERE pf.ID > 0 limit 5");
    queries.put("954","SELECT DISTINCT status , ID as ied from /exampleRegion where ID > 0 order by status, ID desc LIMIT 1");
    queries.put("955","SELECT DISTINCT status , ID as ied from /exampleRegion where ID > 0 order by status, ID desc LIMIT 4");
    queries.put("956","SELECT DISTINCT status , ID as ied from /exampleRegion where ID > 0 order by status, ID desc LIMIT 9");
    queries.put("957","SELECT DISTINCT status as st from /exampleRegion order by status LIMIT 1");
    queries.put("958","SELECT DISTINCT status as st from /exampleRegion order by status LIMIT 4");
    queries.put("959","SELECT DISTINCT status as st from /exampleRegion order by status LIMIT 9");
    queries.put("960","SELECT * FROM /exampleRegion p WHERE p.ID > 0 LIMIT 2");
    queries.put("961","select * from /exampleRegion this where ID > 2 limit 100");
    
    //FUNCTIONS
    queries.put("1000","SELECT DISTINCT * FROM /exampleRegion where IS_DEFINED(P2.secId)");
    queries.put("1001","SELECT DISTINCT * FROM /exampleRegion where IS_UNDEFINED(P2.secId)");
    queries.put("1002","null IN SET('x','y','z')");
    queries.put("1003","null IN SET(null)");
    queries.put("1004","nvl('notNull', 'foundNull')");
    queries.put("1005","nvl(NULL, 'foundNull')");
    queries.put("1006","select distinct * from UNDEFINED");
    queries.put("1007","to_date('05/09/10', 'yy/dd/yy')");
    queries.put("1008","to_date('05/10/09', 'yy/dd/MM')");
    queries.put("1009","to_date('050910', 'yyddMM')");
    queries.put("1010","to_date('051009', 'yyMMdd')");
    queries.put("1011","to_date('09/05/10', 'dd/MM/yy')");
    queries.put("1012","to_date('09/10/05', 'MM/dd/yy')");
    queries.put("1013","to_date('09/10/2005', 'dd/MM/yyyy')");
    queries.put("1014","to_date('09/2005/10', 'dd/yyyy/MM')");
    queries.put("1015","to_date('090510', 'ddyyMM')");
    queries.put("1016","to_date('091005', 'ddMMyy')");
    queries.put("1017","to_date('09102005', 'ddMMyy')");
    queries.put("1018","to_date('09102005', 'ddMMyyyy')");
    queries.put("1019","to_date('09200510', 'ddyyyyMM')");
    queries.put("1020","to_date('10/05/09', 'MM/dd/yy')");
    queries.put("1021","to_date('10/09/05', 'MM/dd/yy')");
    queries.put("1022","to_date('10/09/2005', 'MM/dd/yyyy')");
    queries.put("1023","to_date('10/2005/09', 'MM/yyyy/dd')");
    queries.put("1024","to_date('100509', 'MMyydd')");
    queries.put("1025","to_date('100905', 'MMddyy')");
    queries.put("1026","to_date('10092005', 'MMddyy')");
    queries.put("1027","to_date('10092005', 'MMddyyyy')");
    queries.put("1028","to_date('10092005121314', 'MMddyyyyHHmmss')");
    queries.put("1029","to_date('10092005121314567', 'MMddyyyyHHmmssSSS')");
    queries.put("1030","to_date('10200509', 'MMyyyydd')");
    queries.put("1031","to_date('2005/09/10', 'yyyy/dd/MM')");
    queries.put("1032","to_date('2005/09/10', 'yyyy/MM/dd')");
    queries.put("1033","to_date('20050910', 'yyyyddMM')");
    queries.put("1034","to_date('20051009', 'yyyyMMdd')");
    queries.put("1035","ELEMENT(SELECT DISTINCT * FROM /exampleRegion value  where value.length > 6)");
    queries.put("1036","ELEMENT(SELECT DISTINCT * FROM /exampleRegion where ID <= 1).status");
    queries.put("1037","ELEMENT(SELECT DISTINCT * FROM /exampleRegion where ID =1).status");
    queries.put("1038","SELECT DISTINCT * FROM /exampleRegion where status = ELEMENT(SELECT DISTINCT * FROM /exampleRegion p where ID = 0).status");
    queries.put("1039","SELECT DISTINCT * FROM /exampleRegion where status = ELEMENT(SELECT DISTINCT * FROM /exampleRegion p where p.ID = 0).status");
    queries.put("1040","UNDEFINED IN SET(1,2,3)");
    queries.put("1041","UNDEFINED IN SET(UNDEFINED)");
    queries.put("1042","UNDEFINED IN SET(UNDEFINED,UNDEFINED)");
      
    //MULTIPLE REGIONS
    queries.put("1100","SELECT * FROM /exampleRegion portfolio1, /exampleRegion2 portfolio2 WHERE portfolio1.status = portfolio2.status");
    queries.put("1101","SELECT portfolio1.ID, portfolio2.status FROM /exampleRegion portfolio1, /exampleRegion2 portfolio2");
    queries.put("1102","SELECT * FROM /exampleRegion portfolio1, portfolio1.positions.values positions1, /exampleRegion2 portfolio2,");
    queries.put("1103","SELECT * FROM /exampleRegion portfolio1, portfolio1.positions.values positions1, /exampleRegion2 portfolio2,");
    queries.put("1104","SELECT * FROM (SELECT * FROM /exampleRegion2 m) r2, (SELECT * FROM /exampleRegion e WHERE e.pkid IN  r2.sp) p");
    queries.put("1105","SELECT * FROM (SELECT * FROM /exampleRegion2 m WHERE m.ID IN SET (1, 5, 10)) r2, (SELECT * FROM /exampleRegion e WHERE e.pkid IN  r2.sp) p");
    queries.put("1106","SELECT DISTINCT * FROM /exampleRegion c1, /exampleRegion2 c2 WHERE c1.name = 'INDIA' AND c2.name = 'ISRAEL'");
    queries.put("1107","SELECT DISTINCT * FROM /exampleRegion c1, /exampleRegion2 c3, c1.states s1, c3.states s3, s1.districts d1, s3.getDistrictsWithSameName(d1) d3 WHERE c1.name = 'INDIA' OR c3.name = 'ISRAEL' OR d3.name = 'MUMBAIDIST' OR d3.name = 'PUNEDIST'");
    queries.put("1108","SELECT DISTINCT * FROM /exampleRegion c1, c1.states s1, s1.districts d1, /exampleRegion2 c2, c2.states s2, s2.districts d2, /exampleRegion2 c3, c3.states s3, s3.districts d3 WHERE d3.name = 'PUNEDIST' AND s2.name = 'GUJARAT'");
    queries.put("1109","SELECT DISTINCT * FROM /exampleRegion c1, c1.states s1, s1.districts d1, d1.cities ct1, d1.villages v1, /exampleRegion2 c2, c2.states s2, s2.districts d2, d2.cities ct2, d2.villages v2 WHERE c1.name = 'INDIA' AND c2.name = 'ISRAEL'");
    queries.put("1110","SELECT DISTINCT * FROM /exampleRegion c1, c1.states s1, s1.districts d1, d1.villages v1, d1.cities ct1, /exampleRegion2 c2, c2.states s2, s2.districts d2, d2.villages v2, d2.cities ct2 WHERE v1.name = 'MAHARASHTRA_VILLAGE1' AND ct2.name = 'MUMBAI'");
    queries.put("1111","SELECT DISTINCT * FROM /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion e WHERE pf1.status = 'active'");
    queries.put("1112","SELECT DISTINCT * FROM /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion e1 WHERE pf1.status = 'active' AND e1.empId < 10");
    queries.put("1113","SELECT DISTINCT * FROM /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion e1 WHERE pf1.status = 'active' AND pf2.status = 'active' AND e1.empId < 10");
    queries.put("1114","SELECT DISTINCT * FROM /exampleRegion pf1, /exampleRegion2 pf2, pf1.positions.values posit1, pf2.positions.values posit2 WHERE posit1.secId='IBM' AND posit2.secId='IBM'");
    queries.put("1115","SELECT DISTINCT * FROM /exampleRegion pf1, pf1.collectionHolderMap.values coll1, pf1.positions.values posit1, /exampleRegion2 pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values posit2  WHERE posit1.secId='IBM' AND posit2.secId='IBM'");
    queries.put("1116","SELECT DISTINCT * FROM /exampleRegion pf1, pf1.positions.values posit1, /exampleRegion2 pf2, /exampleRegion e WHERE posit1.secId='IBM'");
    queries.put("1117","SELECT DISTINCT * FROM /exampleRegion pf1, pf1.positions.values posit1, /exampleRegion2 pf2, pf2.positions.values posit2 WHERE pf2.status='active' AND posit1.secId='IBM'");
    queries.put("1118","SELECT DISTINCT * FROM /exampleRegion pf1,/exampleRegion2 pf2, pf1.positions.values posit1, pf2.positions.values posit2 WHERE posit1.secId='IBM' OR posit2.secId='IBM'");
    queries.put("1119","SELECT DISTINCT * FROM /exampleRegion pf1,/exampleRegion2 pf2, pf1.positions.values posit1, pf2.positions.values posit2, pf1.collectionHolderMap.values coll1,pf2.collectionHolderMap.values coll2  WHERE posit1.secId='IBM' OR posit2.secId='IBM'");
    queries.put("1120","SELECT DISTINCT * FROM /exampleRegion2,  positions.values where status='active'");
    queries.put("1121","SELECT DISTINCT * FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.status = 'active'");
    queries.put("1122","SELECT DISTINCT c1.name, s1.name, d2.name, ct2.name FROM /exampleRegion c1, c1.states s1, s1.districts d1, d1.cities ct1, /exampleRegion2 c2, c2.states s2, s2.districts d2, d2.cities ct2 WHERE ct1.name = 'MUMBAI' OR ct2.name = 'CHENNAI'");
    queries.put("1123","SELECT DISTINCT c1.name, s1.name, d2.name, ct2.name FROM /exampleRegion c1, c1.states s1, s1.districts d1, d1.cities ct1,/exampleRegion2 c2, c2.states s2, s2.districts d2, d2.cities ct2 WHERE ct1.name = 'MUMBAI' OR ct2.name = 'CHENNAI'");
    queries.put("1124","SELECT DISTINCT coll1 as collHldrMap1 , coll2 as CollHldrMap2 FROM /exampleRegion pf1, /exampleRegion2 pf2, pf1.positions.values posit1, pf2.positions.values posit2, pf1.collectionHolderMap.values coll1,pf2.collectionHolderMap.values coll2  WHERE posit1.secId='IBM' OR posit2.secId='IBM'");
    queries.put("1125","SELECT DISTINCT coll1 as collHldrMap1 , coll2 as CollHldrMap2 FROM /exampleRegion pf1, /exampleRegion2 pf2, pf1.positions.values posit1,pf2.positions.values posit2,pf1.collectionHolderMap.values coll1, pf2.collectionHolderMap.values coll2 WHERE posit1.secId='IBM' OR posit2.secId='IBM'");
    queries.put("1126","SELECT DISTINCT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.ID > 100 and p2.ID < 1");
    queries.put("1127","SELECT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.status = 'active' and p2.status = 'active'");
    queries.put("1128","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists, dists.cities ct1, dists.villages villgs1, /exampleRegion2 c2, /exampleRegion3 c3 where c1.name = c2.name and ct1.name != 'PUNE' and villgs1.name = 'MAHARASHTRA_VILLAGE1'");
    queries.put("1129","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists, dists.cities ct1, dists.villages villgs1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where c1.name = c2.name or ct1.name != 'PUNE' or villgs1.name = 'MAHARASHTRA_VILLAGE1'");
    queries.put("1130","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 where c1.name = c2.name and ct1.name != 'PUNE' and villgs1.name = 'MAHARASHTRA_VILLAGE1' and villgs1.name = villgs3.name and s2.name = 'PUNJAB' and ct1.name = ct3.name and dists3.name = 'MUMBAIDIST'");
    queries.put("1131","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 where c1.name = c2.name and ct1.name != 'PUNE' or villgs1.name = 'MAHARASHTRA_VILLAGE1' or villgs1.name = villgs3.name or s2.name = 'PUNJAB' or ct1.name = ct3.name and dists3.name = 'MUMBAIDIST'");
    queries.put("1132","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 where c1.name = c2.name and sts1.name != 'PUNJAB' and ct1.name != 'PUNE' and villgs1.name = 'MAHARASHTRA_VILLAGE1' and villgs1.name = villgs3.name and sts3.name != sts1.name and s2.name = 'PUNJAB' and ct1.name = ct3.name and dists3.name = 'MUMBAIDIST' and dists3.name != s2.name");
    queries.put("1133","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 where c1.name = c2.name or ct1.name != 'PUNE' and villgs1.name = 'MAHARASHTRA_VILLAGE1' or villgs1.name = villgs3.name or s2.name = 'PUNJAB' or ct1.name = ct3.name and dists3.name = 'MUMBAIDIST'");
    queries.put("1134","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 where c1.name = c2.name or ct1.name != 'PUNE' or villgs1.name = 'MAHARASHTRA_VILLAGE1' or villgs1.name = villgs3.name or s2.name = 'PUNJAB' or ct1.name = ct3.name or dists3.name = 'MUMBAIDIST'");
    queries.put("1135","Select distinct * from /exampleRegion pf, /exampleRegion2, /exampleRegion3, /exampleRegion where pf.status='active'");
    queries.put("1136","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and (c1.name = c2.name or pfo3.status != 'inactive') and pfo3.status = pfos.status");
    queries.put("1137","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and c1.name = c2.name or c3.name = 'INDIA' and pfo3.status != 'inactive' or pfo3.\"type\" = 'type1' and pfo3.status = pfos.status");
    queries.put("1138","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and c1.name = c2.name or pfo3.status != 'inactive' and pfo3.status = pfos.status");
    queries.put("1139","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' or c1.name = c2.name or c3.name = 'INDIA' and pfo3.status != 'inactive' or pfo3.\"type\" = 'type1' and pfo3.status = pfos.status");
    queries.put("1140","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' or c1.name = c2.name or pfo3.status != 'inactive' or pfo3.status = pfos.status");
    queries.put("1141","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, c1.states s1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and ((c1.name = c2.name or pfo3.status != 'inactive') and pfo3.status = pfos.status) or s1.name = 'MAHARASHTRA' and s2.name != 'MAHARASHTRA'");
    queries.put("1142","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where (Pos1.secId = 'YHOO' and c1.name = c2.name or pfo3.status != 'inactive') and pfo3.status = pfos.status and villgs1.name = 'MAHARASHTRA_VILLAGE1'");
    queries.put("1143","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and c1.name = c2.name or pfo3.status != 'inactive' and pfo3.status = pfos.status and villgs1.name = 'MAHARASHTRA_VILLAGE1' or pfos.ID != 0");
    queries.put("1144","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and c1.name = c2.name or pfo3.status != 'inactive' and pfo3.status = pfos.status and villgs1.name = 'MAHARASHTRA_VILLAGE1'");
    queries.put("1145","Select distinct * from /exampleRegion, /exampleRegion2");
    queries.put("1146","Select distinct * from /exampleRegion, /exampleRegion2, /exampleRegion3, /exampleRegion");
    queries.put("1147","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and false and c1.name = 'INDIA' and pf2.ID = 2");
    queries.put("1148","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and false and pf1.ID = pf2.ID and c1.name = 'INDIA' and pf2.ID = 2");
    queries.put("1149","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and false and pf1.ID = pf2.ID or c1.name = 'INDIA' or pf2.ID = 2");
    queries.put("1150","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and false and pf1.ID = pf2.ID");
    queries.put("1151","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and false or c1.name = 'INDIA' or pf2.ID = 2");
    queries.put("1152","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and false");
    queries.put("1153","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and true and c1.name = 'INDIA' and pf2.ID = 2");
    queries.put("1154","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and true or c1.name = 'INDIA' or pf1.ID = 2");
    queries.put("1155","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and true or pf1.ID = pf2.ID and c1.name = 'INDIA' and pf2.ID = 2");
    queries.put("1156","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and true or pf1.ID = pf2.ID or c1.name = 'INDIA' or pf2.ID = 2");
    queries.put("1157","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and true or pf1.ID = pf2.ID");
    queries.put("1158","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and true");
    queries.put("1159","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name");
    queries.put("1160","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or false or c1.name = c2.name and c1.name = 'INDIA' and pf1.ID = 2");
    queries.put("1161","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or false or c1.name = c2.name or c1.name = 'INDIA' or pf1.ID = 2");
    queries.put("1162","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or false or c1.name = c2.name or pf2.ID = 1 or c1.name = 'INDIA' and c1.name = 'INDIA' and pf2.ID = 2");
    queries.put("1163","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or false or c1.name = c2.name or pf2.ID = 1 or c1.name = 'INDIA'");
    queries.put("1164","select distinct * from /exampleRegion pf1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or false or c1.name = c2.name");
    queries.put("1165","select distinct * from /exampleRegion pf1, pf1.positions.values pos1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM' and c1.name = 'INDIA' and pf2.ID = 2");
    queries.put("1166","select distinct * from /exampleRegion pf1, pf1.positions.values pos1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM' or c1.name = 'INDIA' or pf2.ID = 2");
    queries.put("1167","select distinct * from /exampleRegion pf1, pf1.positions.values pos1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM' or false and pf1.ID = pf2.ID and c1.name = 'INDIA' and pf2.ID = 2");
    queries.put("1168","select distinct * from /exampleRegion pf1, pf1.positions.values pos1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM' or false and pf1.ID = pf2.ID or c1.name = 'INDIA' or pf1.ID = 2");
    queries.put("1169","select distinct * from /exampleRegion pf1, pf1.positions.values pos1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM' or false and pf1.ID = pf2.ID");
    queries.put("1170","select distinct * from /exampleRegion pf1, pf1.positions.values pos1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM'");
    queries.put("1171","select distinct * from /exampleRegion pf1, pf1.positions.values pos1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and pos1.secId = 'IBM' and true and c1.name = 'INDIA' and pf2.ID = 2");
    queries.put("1172","select distinct * from /exampleRegion pf1, pf1.positions.values pos1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and pos1.secId = 'IBM' and true or pf1.ID != 3");
    queries.put("1173","select distinct * from /exampleRegion pf1, pf1.positions.values pos1, /exampleRegion2 pf2, /exampleRegion c1, /exampleRegion2 c2 where pf1.status = pf2.status or c1.name = c2.name and pos1.secId = 'IBM' and true");
    queries.put("1174","select distinct * from /root/exampleRegion, /root/exampleRegion2");
    queries.put("1175","select distinct a, b.price from /root/exampleRegion a, /root/exampleRegion2 b where a.price = b.price and a.price = 50");
    queries.put("1176","select distinct a, b.price from /root/exampleRegion a, /root/exampleRegion2 b where a.price = b.price");
    queries.put("1177","SELECT DISTINCT * FROM /exampleRegion pf, pf.positions pos, /exampleRegion3 pf3, /exampleRegion emp WHERE pf.iD = emp.empId and pf.status='active' and emp.age > 50 and pf3.status='active'");
    queries.put("1178","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists, dists.cities ct1, dists.villages villgs1, /exampleRegion2 c2, /exampleRegion3 c3 where c1.name = c2.name and ct1.name != 'PUNE' and villgs1.name = 'MAHARASHTRA_VILLAGE1'");
    queries.put("1179","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists, dists.cities ct1, dists.villages villgs1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where c1.name = c2.name or ct1.name != 'PUNE' or villgs1.name = 'MAHARASHTRA_VILLAGE1'");
    queries.put("1180","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 where c1.name = c2.name and ct1.name != 'PUNE' and villgs1.name = 'MAHARASHTRA_VILLAGE1' and villgs1.name = villgs3.name and s2.name = 'PUNJAB' and ct1.name = ct3.name and dists3.name = 'MUMBAIDIST'");
    queries.put("1181","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 where c1.name = c2.name and ct1.name != 'PUNE' or villgs1.name = 'MAHARASHTRA_VILLAGE1' or villgs1.name = villgs3.name or s2.name = 'PUNJAB' or ct1.name = ct3.name and dists3.name = 'MUMBAIDIST'");
    queries.put("1182","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 where c1.name = c2.name and sts1.name != 'PUNJAB' and ct1.name != 'PUNE' and villgs1.name = 'MAHARASHTRA_VILLAGE1' and villgs1.name = villgs3.name and sts3.name != sts1.name and s2.name = 'PUNJAB' and ct1.name = ct3.name and dists3.name = 'MUMBAIDIST' and dists3.name != s2.name");
    queries.put("1183","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 where c1.name = c2.name or ct1.name != 'PUNE' and villgs1.name = 'MAHARASHTRA_VILLAGE1' or villgs1.name = villgs3.name or s2.name = 'PUNJAB' or ct1.name = ct3.name and dists3.name = 'MUMBAIDIST'");
    queries.put("1184","Select distinct * from /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 where c1.name = c2.name or ct1.name != 'PUNE' or villgs1.name = 'MAHARASHTRA_VILLAGE1' or villgs1.name = villgs3.name or s2.name = 'PUNJAB' or ct1.name = ct3.name or dists3.name = 'MUMBAIDIST'");
    queries.put("1185","Select distinct * from /exampleRegion pf, /exampleRegion2, /exampleRegion3, /exampleRegion where pf.status='active'");
    queries.put("1186","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and (c1.name = c2.name or pfo3.status != 'inactive') and pfo3.status = pfos.status");
    queries.put("1187","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and c1.name = c2.name or c3.name = 'INDIA' and pfo3.status != 'inactive' or pfo3.\"type\" = 'type1' and pfo3.status = pfos.status");
    queries.put("1188","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and c1.name = c2.name or pfo3.status != 'inactive' and pfo3.status = pfos.status");
    queries.put("1189","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' or c1.name = c2.name or c3.name = 'INDIA' and pfo3.status != 'inactive' or pfo3.\"type\" = 'type1' and pfo3.status = pfos.status");
    queries.put("1190","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' or c1.name = c2.name or pfo3.status != 'inactive' or pfo3.status = pfos.status");
    queries.put("1191","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, c1.states s1, /exampleRegion2 c2, c2.states s2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and ((c1.name = c2.name or pfo3.status != 'inactive') and pfo3.status = pfos.status) or s1.name = 'MAHARASHTRA' and s2.name != 'MAHARASHTRA'");
    queries.put("1192","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where (Pos1.secId = 'YHOO' and c1.name = c2.name or pfo3.status != 'inactive') and pfo3.status = pfos.status and villgs1.name = 'MAHARASHTRA_VILLAGE1'");
    queries.put("1193","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and c1.name = c2.name or pfo3.status != 'inactive' and pfo3.status = pfos.status and villgs1.name = 'MAHARASHTRA_VILLAGE1' or pfos.ID != 0");
    queries.put("1194","Select distinct * from /exampleRegion pfos, pfos.positions.values Pos1, /exampleRegion c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, /exampleRegion2 c2, /exampleRegion3 c3, /exampleRegion3 pfo3 where Pos1.secId = 'YHOO' and c1.name = c2.name or pfo3.status != 'inactive' and pfo3.status = pfos.status and villgs1.name = 'MAHARASHTRA_VILLAGE1'");
    queries.put("1195","Select distinct * from /exampleRegion, /exampleRegion2, /exampleRegion3, /exampleRegion");
    queries.put("1196","Select distinct * from /exampleRegion3 pf, pf.positions");
    queries.put("1197","Select distinct * from /exampleRegion3, /exampleRegion");
    queries.put("1198","select distinct * from /root/exampleRegion3");
    
    
    //Queries used for Equi-join tests
    queries.put("1199","select * from /region1 r1, /region2 r2 where" +    "r1.ID = r2.id");
    queries.put("1200","select * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id AND r1.ID > 5");
    queries.put("1201","select * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id AND r1.status = 'active'");
    queries.put("1202","select distinct * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id ORDER BY r1.ID");
    queries.put("1203","select distinct * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id ORDER BY r2.id");
    queries.put("1204","select distinct * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id ORDER BY r2.status");
    queries.put("1205","select * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id AND r1.status != r2.status");
    queries.put("1206","select * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id AND r1.status = r2.status");
    queries.put("1207","select * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id AND r1.positions.size = r2.positions.size");
    queries.put("1208","select * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id AND r1.positions.size > r2.positions.size");
    queries.put("1209","select * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id AND r1.positions.size < r2.positions.size");
    queries.put("1210","select * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id AND r1.positions.size = r2.positions.size AND r2.positions.size > 0");
    queries.put("1211","select * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id AND (r1.positions.size > r2.positions.size OR r2.positions.size > 0)");
    queries.put("1212","select * from /region1 r1, /region2 r2 where" + "r1.ID = r2.id AND (r1.positions.size < r2.positions.size OR r1.positions.size > 0)");
    
    queries.put("1199","select * from /region1 r1, /region2 r2, r2.positions.values pos2 where" +    "r1.ID = pos2.id");
    queries.put("1200","select * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id AND r1.ID > 5");
    queries.put("1201","select * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id AND r1.status = 'active'");
    queries.put("1202","select distinct * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id ORDER BY r1.ID");
    queries.put("1203","select distinct * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id ORDER BY pos2.id");
    queries.put("1204","select distinct * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id ORDER BY r2.status");
    queries.put("1205","select * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id AND r1.status != r2.status");
    queries.put("1206","select * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id AND r1.status = r2.status");
    queries.put("1207","select * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id AND r1.positions.size = r2.positions.size");
    queries.put("1208","select * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id AND r1.positions.size > r2.positions.size");
    queries.put("1209","select * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id AND r1.positions.size < r2.positions.size");
    queries.put("1210","select * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id AND r1.positions.size = r2.positions.size AND r2.positions.size > 0");
    queries.put("1211","select * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id AND (r1.positions.size > r2.positions.size OR r2.positions.size > 0)");
    queries.put("1212","select * from /region1 r1, /region2 r2, r2.positions.values pos2 where" + "r1.ID = pos2.id AND (r1.positions.size < r2.positions.size OR r1.positions.size > 0)");
    
    //BIND QUERIES
    bindQueries.put("1", "$1 AND $2");
    bindQueries.put("2", "$1 IN $2");
    bindQueries.put("3", "$1 IN $3");
    bindQueries.put("4", "$1 IN SET(1, 'a', $2, $3, $4, $5)");
    bindQueries.put("5", "$1 OR $2");
    bindQueries.put("6", "$1[$2]");
    bindQueries.put("7", "$1[0][0]");
    bindQueries.put("8", "$3 IN $2");
    bindQueries.put("9", "(SELECT DISTINCT * FROM /root/exampleRegion WHERE ID < $1).size");
    bindQueries.put("10", "/exampleRegion.containsValue($1)");
    bindQueries.put("11", "IMPORT com.gemstone.gemfire.cache.\"query\".data.Student;IMPORT com.gemstone.gemfire.cache.\"query\".data.Student$Subject;Select distinct * from  $1 as it1 ,  it1.subjects  type Student$Subject  where subject='Hindi'");
    bindQueries.put("12", "IMPORT com.gemstone.gemfire.cache.\"query\".data.Student;IMPORT com.gemstone.gemfire.cache.\"query\".data.Student$Subject;Select distinct * from  $1 as it1 ,  it1.subjects x  type Student$Subject  where x.subject='Hindi'");
    bindQueries.put("13", "IMPORT com.gemstone.gemfire.cache.\"query\".data.Student;IMPORT com.gemstone.gemfire.cache.\"query\".data.Student$Subject;Select distinct * from  $1 as it1 , (list<Student$Subject>) it1.subjects   where subject='Hindi'");
    bindQueries.put("14", "IMPORT com.gemstone.gemfire.cache.\"query\".data.Student;IMPORT com.gemstone.gemfire.cache.\"query\".data.Student$Teacher;Select distinct * from  $1 as it1 ,  it1.teachers  type Student$Teacher  where teacher='Y'");
    bindQueries.put("15", "IMPORT com.gemstone.gemfire.cache.\"query\".data.Student;IMPORT com.gemstone.gemfire.cache.\"query\".data.Student$Teacher;Select distinct * from  $1 as it1 ,  it1.teachers x  type Student$Teacher  where x.teacher='Y'");
    bindQueries.put("16", "IMPORT com.gemstone.gemfire.cache.\"query\".data.Student;IMPORT com.gemstone.gemfire.cache.\"query\".data.Student$Teacher;Select distinct * from  $1 as it1 , (list<Student$Teacher>) it1.teachers  where teacher='Y'");
    bindQueries.put("17", "IS_DEFINED($1)");
    bindQueries.put("18", "IS_UNDEFINED($1)");
    bindQueries.put("19", "NOT $1");
    bindQueries.put("20", "NOT $2");
    bindQueries.put("21", "NOT $3");
    bindQueries.put("22", "NOT $4");
    bindQueries.put("23", "SELECT  distinct ID, description, createTime FROM /exampleRegion1 pf1 where pf1.ID != $1 limit 10");
    bindQueries.put("24", "SELECT  distinct ID, description, createTime FROM /exampleRegion1 pf1 where pf1.ID != $1 order by ID");
    bindQueries.put("25", "SELECT * FROM /exampleRegion pf WHERE pf = $1");
    bindQueries.put("26", "SELECT * FROM /exampleRegion pf WHERE pf > $1");
    bindQueries.put("27", "SELECT * FROM /exampleRegion.keys k WHERE k.ID = $1");
    bindQueries.put("28", "SELECT * FROM /exampleRegion.keys key WHERE key.ID > $1");
    bindQueries.put("29", "SELECT * FROM /exampleRegion.values pf WHERE pf < $1");
    bindQueries.put("30", "SELECT * FROM /root/exampleRegion WHERE ID < $1 and Ticker = $2");
    bindQueries.put("31", "SELECT * FROM /root/exampleRegion WHERE ID = $1 and Ticker = $2");
    bindQueries.put("32", "SELECT DISTINCT * FROM $1 z where z.status = 'active'");
    bindQueries.put("33", "SELECT DISTINCT * FROM /exampleRegion WHERE ID < $1 ORDER BY $2");
    bindQueries.put("34", "SELECT DISTINCT * FROM /exampleRegion where ID = $1");
    bindQueries.put("35", "SELECT DISTINCT * FROM /exampleRegion where status = $1");
    bindQueries.put("36", "SELECT DISTINCT * FROM /exampleRegion where status.equals($1)");
    bindQueries.put("37", "SELECT DISTINCT * FROM /root/exampleRegion WHERE ID < $1 ORDER BY $2");
    bindQueries.put("38", "SELECT DISTINCT * FROM /root/exampleRegion WHERE ID < $1 ORDER BY ID");
    bindQueries.put("39", "SELECT DISTINCT booleanValue from $1 TYPE boolean");
    bindQueries.put("40", "SELECT DISTINCT booleanValue from (set<boolean>) $1");
    bindQueries.put("41", "SELECT DISTINCT byteValue from $1 TYPE byte");
    bindQueries.put("42", "SELECT DISTINCT byteValue from (set<byte>) $1");
    bindQueries.put("43", "SELECT DISTINCT charValue from $1 TYPE char");
    bindQueries.put("44", "SELECT DISTINCT charValue from (set<char>) $1");
    bindQueries.put("45", "SELECT DISTINCT doubleValue from $1 TYPE double");
    bindQueries.put("46", "SELECT DISTINCT doubleValue from (set<double>) $1");
    bindQueries.put("47", "SELECT DISTINCT floatValue from $1 TYPE float");
    bindQueries.put("48", "SELECT DISTINCT floatValue from (set<float>) $1");
    bindQueries.put("49", "SELECT DISTINCT intern from $1 TYPE string");
    bindQueries.put("50", "SELECT DISTINCT intern from (set<string>) $1");
    bindQueries.put("51", "SELECT DISTINCT longValue from $1 TYPE long");
    bindQueries.put("52", "SELECT DISTINCT longValue from (set<long>) $1");
    bindQueries.put("53", "SELECT DISTINCT shortValue from $1 TYPE short");
    bindQueries.put("54", "SELECT DISTINCT shortValue from (set<short>) $1");
    bindQueries.put("55", "SELECT ID, status FROM /exampleRegion.keys WHERE ID = $1");
    bindQueries.put("56", "SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like $1 AND ps.ID > 2 AND ps.ID < 150");
    bindQueries.put("57", "SELECT distinct *  FROM /exampleRegion ps WHERE ps.status like $1");
    bindQueries.put("58", "SELECT e.key, e.value FROM /exampleRegion.entrySet e WHERE e.key IN $1");
    bindQueries.put("59", "SELECT e.value FROM /exampleRegion.entrySet e WHERE e.key IN $1");
    bindQueries.put("60", "SELECT itr.value FROM /root/exampleRegion.entries itr where itr.key = $1");
    bindQueries.put("61", "SELECT k.ID, k.status FROM /exampleRegion.keys k WHERE k.ID = $1 and k.status = $2");
    bindQueries.put("62", "SELECT key.ID FROM /exampleRegion.keys key WHERE key.ID = $1");
    bindQueries.put("63", "Select distinct * from /exampleRegion.keys keys where keys.hashCode >= $1");
    bindQueries.put("64", "Select distinct intValue from $1 TYPE int");
    bindQueries.put("65", "Select distinct intValue from (array<int>) $1");
    bindQueries.put("66", "Select distinct intValue from (list<int>) $1");
    bindQueries.put("67", "Select distinct keys.hashCode  from /exampleRegion.keys() keys where keys.hashCode >= $1");
    bindQueries.put("68", "Select distinct value.secId from /exampleRegion , getPositions($1)");
    bindQueries.put("69", "import com.gemstone.gemfire.cache.\"query\".data.Position;select distinct * from $1 r, r.positions.values pVal TYPE Position where r.status = 'active' AND pVal.mktValue >= 25.00");
    bindQueries.put("70", "import com.gemstone.gemfire.cache.\"query\".data.Position;select distinct r from $1 r, r.positions.values pVal TYPE Position where pVal.mktValue < $2");
    bindQueries.put("71", "import com.gemstone.gemfire.cache.\"query\".data.Position;select distinct r.ID, status, mktValue from $1 r, r.positions.values pVal TYPE Position where r.status = 'active' AND pVal.mktValue >= 25.00");
    bindQueries.put("72", "import java.util.Map$Entry as Entry;select distinct value.secId from /exampleRegion, getPositions(23) type Entry");
    bindQueries.put("73", "select * from $1");
    bindQueries.put("74", "select ALL * from $1");
    bindQueries.put("75", "select distinct * from $1 where status = 'active'");
    bindQueries.put("76", "select distinct * from $1");
    bindQueries.put("77", "select distinct * from /exampleRegion pf where pf.getCW(pf.ID) != $1");
    bindQueries.put("78", "select distinct * from /exampleRegion pf where pf.getCW(pf.ID) < $1");
    bindQueries.put("79", "select distinct * from /exampleRegion pf where pf.getCW(pf.ID) > $1");
    bindQueries.put("80", "select distinct * from /root/exampleRegion where ticker = $1");
    bindQueries.put("81", "select p from /exampleRegion.values p where p like $1");
    bindQueries.put("82", "select p.positions.get('acc') from $1 p");
  }

  public void createServer(VM server, final Properties prop) {
    SerializableRunnable createCacheServer = new CacheSerializableRunnable(
        "Create Cache Server") {
      private static final long serialVersionUID = 1L;

      public void run2() throws CacheException {
        createCache(prop);
      }
    };
    server.invoke(createCacheServer);
  }

  public void createCache(Properties prop) {
    if (null != prop && !prop.isEmpty()) {
      cache = new CacheFactory(prop).create();
    }
    else{
      cache = new CacheFactory().set("mcast-port", "0").create();
    }
  }
  
  public static void setCache(Cache cache) {
    QueryTestUtils.cache = cache;
  }
  
  public void createClient(VM client, final Properties prop) {
    SerializableRunnable createCacheClient = new CacheSerializableRunnable(
        "Create Cache Client") {
      private static final long serialVersionUID = 1L;

      public void run2() throws CacheException {
        createClientCache(prop);
      }
    };
    client.invoke(createCacheClient);
  }

  public void createClientCache(Properties prop) {
    if (null != prop && !prop.isEmpty()) {
      cache = (Cache) new ClientCacheFactory(prop).create();
    }
    else{
      cache = (Cache) new ClientCacheFactory().create();
    }
 }

  public void createPartitionRegion(final String name, final Class constraint, VM vm) {
    vm.invoke(new CacheSerializableRunnable("Create Partition region") {
      private static final long serialVersionUID = 1L;

      public void run2() throws CacheException {
        createPartitionRegion(name, constraint);
      }
    });
  }

  public void createPartitionRegion(String name, Class constraint) {
    ExpirationAttributes expiration = ExpirationAttributes.DEFAULT;
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    RegionFactory factory = cache.createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(paf.create());
    if (constraint != null) {
      factory.setValueConstraint(constraint);
    }
    factory.create(name);
    
  }

  public void createReplicateRegion(final String name, VM vm) {
    vm.invoke(new CacheSerializableRunnable("Create Replicated region") {
      private static final long serialVersionUID = 1L;

      public void run2() throws CacheException {
        getLogger().fine("### Create replicated region. ###");
        createReplicateRegion(name);
      }
    });
  }

  public void createReplicateRegion(String name) {
    cache.createRegionFactory(RegionShortcut.REPLICATE).create(name);
  }
  
  public void createHashIndex(VM vm, final String name, final String field,
      final String region) {
      vm.invoke(new CacheSerializableRunnable("Create Replicated region") {
      public void run2() throws CacheException {
        createHashIndex(name, field, region);
      }
    });
  }

  public Index createHashIndex(String name, String field, String region){
    try {
      return cache.getQueryService().createHashIndex(name, field, region);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Index creation failed for index on region: "
          + region + "." + field, e);
    }
  }

  public void createIndex(VM vm, final String name, final String field,
      final String region) throws CacheException {
      vm.invoke(new CacheSerializableRunnable("Create Index") {
      public void run2() throws CacheException {
        createIndex(name, field, region);
      }
    });
  }
  
  public Index createIndex(String name, String field, String region) throws CacheException {
    try {
      return cache.getQueryService().createIndex(name, field, region);
    } catch (Exception e) {
      throw new RuntimeException("Index creation failed for index on region: "
          + region + "." + field, e);
    }
  }
  
  public void removeIndex(String name, String region){
    try {
      qs = cache.getQueryService();
      qs.removeIndex(qs.getIndex(cache.getRegion(region), name));
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Index removal failed for index: " + name
          + " on region: " + region, e);
    }
  }

  public void createLocalRegion(final String name, VM vm) {
    vm.invoke(new CacheSerializableRunnable("Create Local region") {
      private static final long serialVersionUID = 1L;

      public void run2() throws CacheException {
        getLogger().fine("### Create Local region. ###");
        createLocalRegion(name);
      }
    });
  }

  public void createLocalRegion(String name) {
    cache.createRegionFactory(RegionShortcut.LOCAL).create(name);
  }

  public void createSubRegion(final String rootName, final String name,
      final RegionAttributes regionAttributes, VM vm) {
    vm.invoke(new CacheSerializableRunnable("Creating subregion") {
      private static final long serialVersionUID = 1L;

      public void run2() throws CacheException {
        getLogger().fine("### Create subregion. ###");
        Region root = cache.getRegion(rootName);
        if (root == null) {
          root = cache.createRegionFactory().create(rootName);
        }
        RegionAttributes ra = null;
        if (regionAttributes == null) {
          ra = new AttributesFactory().create();
        }
        root.createSubregion(name, ra);
      }
    });
  }

  public void closeServer(VM server) {
    server.invoke(new CacheSerializableRunnable("Closing Cache Server") {
      private static final long serialVersionUID = 1L;

      public void run2() throws CacheException {
        getLogger().fine("### Close Cache Server. ###");
        closeCache();
      }
    });
  }

  public void closeCache() {
    if (!cache.isClosed()) {
      cache.close();
    }
  }

  /**
   * Places new values in the given region with Object ({@link Portfolio}) Keys
   * @param vm 
   * @param regionName
   * @param size
   */
  public void createValuesObjKeys(VM vm, final String regionName, final int size) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        createValuesObjKeys(regionName, size);
      }
    });
  }

  /**
   * Places new values in the given region with {@link String} Keys
   * @param vm 
   * @param regionName
   * @param size
   */
  public void createValuesStringKeys(VM vm, final String regionName, final int size) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        createValuesStringKeys(regionName, size);
      }
    });
  }

  /**
   * Places new values in the given region with {@link String} Keys 
   * @param regionName
   * @param size
   */
  public void createValuesStringKeys(String regionName, final int size) {
    Region region = cache.getRegion(regionName);
    for (int i = 1; i <= size; i++) {
      region.put("KEY-"+ i, new Portfolio(i));
    }
  }
  
  /**
   * Used to update the values in the given region having {@link String} Keys
   * with modifed values. Should preferably use after the call to createValuesStringKeys
   * @param regionName
   * @param size
   */
  public void createDiffValuesStringKeys(String regionName, final int size) {
    Region region = cache.getRegion(regionName);
    for (int i = 1; i <= size; i++) {
      region.put("KEY-"+ i, new Portfolio(i+1));
    }
  }
  /**
   * Places new values in the given region with Object ({@link Portfolio}) Keys 
   * @param regionName
   * @param size
   */
  public void createValuesObjKeys(String regionName, final int size) {
    Region region = cache.getRegion(regionName);
    for (int i = 1; i <= size; i++) {
      region.put(new Portfolio(i), new Portfolio(i));
    }
  }
  
  /**
   * Places new values in the given region with {@link String} Keys 
   * @param regionName
   * @param size
   */
  public void createNumericValuesStringKeys(String regionName, final int size) {
    Region region = cache.getRegion(regionName);
    for (int i = 1; i <= size; i++) {
      region.put("KEY-"+ i, new Numbers(i));
    }
  }

  /**
   * Destroys entries from the region
   * @param regionName
   * @param size
   * @throws Exception 
   */
  public void destroyRegion(String regionName, int size) throws Exception{
    Region region = cache.getRegion(regionName);
    for (int i = 1; i <= size; i++) {
      try {
        region.destroy("KEY-"+ i);
      } catch (Exception e) {
        throw new Exception(e);
      }
    }
  }
 
  /**
   * Executes queries corresponding to the keys passed using array<br>  
   *    <P><code>
   *    String[] arr = {"1", "2", "3"};<br>
   *    new QueryTestUtils().executeQueries(vm0, arr);
   *    </code></P>
   * @param vm
   *  The VM on which the queries are to be executed
   * @param qarr
   *  Array of keys, queries in the values of which to be executed
   */
  public void executeQueries(VM vm, final String qarr[]) {
    vm.invoke(new CacheSerializableRunnable("Executing query") {
      public void run2() throws CacheException {
        try {
          executeQueries(qarr);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  /**
   * Executes queries corresponding to the keys passed using array<br>  
   *    <P><code>
   *    String[] arr = {"1", "2", "3"};<br>
   *    new QueryTestUtils().executeQueries(arr);
   *    </code></P>
   * @param qarr
   *  Array of keys, queries in the values mapped are executed
   * @return 
   *  Object array containing SelectResults
   */
  public Object[] executeQueries(String qarr[]) throws Exception{
    qs = cache.getQueryService();
    Object[] result = new Object[qarr.length];
    String query = null;
    int j = 0;
    for (int i = 0; i < qarr.length; i++) {
      query = queries.get(qarr[i]);
      result[j++] = qs.newQuery(query).execute();
    }
    return result;
  }

  /**
   * Execute queries by removing any DISTINCT clause if present
   * @param vm
   * @param qarr
   */
  public void executeQueriesWithoutDistinct(VM vm,final String qarr[]) {
    vm.invoke(new CacheSerializableRunnable("Executing query without distinct") {
      public void run2() {
        executeQueriesWithoutDistinct(qarr);
      }
    });  
  }
  
  /**
   * Execute queries by removing any DISTINCT clause if present
   * @param qarr
   */
  public Object[] executeQueriesWithoutDistinct(String qarr[]) {
    qs = cache.getQueryService();
    Object[] result = new Object[qarr.length];
    String query = null;
    int j = 0;
    for (int i = 0; i < qarr.length; i++) {
      query = queries.get(qarr[i]);
      if(query.indexOf("distinct") > -1)
        query = query.replace("distinct", "");
      if(query.indexOf("DISTINCT") > -1)
        query = query.replace("DISTINCT", "");
      
     // hydra.getLogWriter().info("\nExecuting query: " + query);
      try {
        result[j++] = qs.newQuery(query).execute();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }
  
  /**
   * Execute queries by adding a DISTINCT clause if  not present
   * @param vm
   * @param qarr
   */
  public void executeQueriesWithDistinct(VM vm,final String qarr[]) {
    vm.invoke(new CacheSerializableRunnable("Executing query with distinct") {
      public void run2() {
        executeQueriesWithDistinct(qarr);
      }
    });  
  }
  
  /**
   * Execute queries by adding a DISTINCT clause if  not present
   * @param qarr
   */
  public Object[] executeQueriesWithDistinct(String qarr[]) {
    qs = cache.getQueryService();
    Object[] result = new Object[qarr.length];
    String query = null;
    int j = 0;
    for (int i = 0; i < qarr.length; i++) {
      query = queries.get(qarr[i]);
      if(query.indexOf("distinct")== -1)
        query = query.replaceFirst("select", "select distinct");
      else if(query.indexOf("DISTINCT")== -1)
        query = query.replaceFirst("select", "select distinct");
      
     // hydra.getLogWriter().info("\nExecuting query: " + query);
      try {
        result[j++] = qs.newQuery(query).execute();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  /**
   * Execute all the queries in the map
   * @param vm
   */
  public void executeAllQueries(VM vm) {
    vm.invoke(new CacheSerializableRunnable("Execute all queries") {
      public void run2() {
        executeAllQueries();
      }
    });
  }

  /**
   * Execute all the queries in the map
   */
  public Object[] executeAllQueries() {
    qs = cache.getQueryService();
    Object[] result = new Object[queries.size()];

    int j = 0;
    for (Map.Entry<String, String> entry : queries.entrySet()) {
      String queryId = entry.getKey();
      String query = entry.getValue();
      System.out.println("\nExecuting query: " + query);
      try {
        result[j++] = qs.newQuery(query).execute();
        formatQueryResult(result);
      } catch (Exception e) {
        throw new RuntimeException("Query: " + queryId + "-" + query + " failed: ", e);
      }
    }
    return result;
  }

  /**
   * Executes a particular bind query corresponding to the ID passed using array<br>  
   *    <P><code>
   *    String[] arr = {"1", "2", "3"};<br>
   *    new QueryTestUtils().executeBindQuery(vm0, "1", arr);
   *    </code></P>
   * @param vm
   *  The vm on which the query is to be executed   
   */
  public void executeBindQuery(VM vm, final String queryId,
      final Object[] params) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        executeBindQuery(queryId, params);
      }
    });
  }

  /**
   * Executes a particular bind query corresponding to the ID passed using array<br>  
   *    <P><code>
   *    String[] arr = {"1", "2", "3"};<br>
   *    new QueryTestUtils().executeBindQuery("1", arr);
   *    </code></P>
   */
  
  public Object executeBindQuery(final String queryId, final Object[] params) {
    Object result = null;
    qs = cache.getQueryService();
    String query = bindQueries.get(queryId);
    try {
      getLogger().fine("\nExecuting query: " + query);
      result = qs.newQuery(query).execute(params);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public void compareResults(SelectResults[][] r) {
    Set set1 = null;
    Set set2 = null;
    Iterator itert1 = null;
    Iterator itert2 = null;
    ObjectType type1, type2;

    for (int j = 0; j < r.length; j++) {
      CollectionType collType1 = r[j][0].getCollectionType();
      CollectionType collType2 = r[j][1].getCollectionType();
      type1 = collType1.getElementType();
      type2 = collType2.getElementType();

      if (collType1.getSimpleClassName().equals(collType2.getSimpleClassName())) {
        CacheUtils.log("Both SelectResults are of the same Type i.e.--> "
            + collType1);
      }
      else {
        CacheUtils.log("Collection type are : " + collType1 + "and  "
            + collType2);
        Assert
            .fail("FAILED:Select results Collection Type is different in both the cases. CollectionType1="
                + collType1 + " CollectionType2=" + collType2);
      }
      if (type1.equals(type2)) {
        CacheUtils.log("Both SelectResults have same element Type i.e.--> "
            + type1);
      }
      else {
        CacheUtils.log("Classes are :  type1=" + type1.getSimpleClassName()
            + " type2= " + type2.getSimpleClassName());
        Assert
            .fail("FAILED:SelectResult Element Type is different in both the cases. Type1="
                + type1 + " Type2=" + type2);
      }

      if (collType1.equals(collType2)) {
        CacheUtils.log("Both SelectResults are of the same Type i.e.--> "
            + collType1);
      }
      else {
        CacheUtils.log("Collections are : " + collType1 + " " + collType2);
        Assert
            .fail("FAILED:SelectResults Collection Type is different in both the cases. CollType1="
                + collType1 + " CollType2=" + collType2);
      }
      if (r[j][0].size() == r[j][1].size()) {
        CacheUtils.log("Both SelectResults are of Same Size i.e.  Size= "
            + r[j][1].size());
      }
      else {
        Assert
            .fail("FAILED:SelectResults size is different in both the cases. Size1="
                + r[j][0].size() + " Size2 = " + r[j][1].size());
      }
      set2 = ((r[j][1]).asSet());
      set1 = ((r[j][0]).asSet());
      // boolean pass = true;
      itert1 = set1.iterator();
      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        itert2 = set2.iterator();

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          Object p2 = itert2.next();
          if (p1 instanceof Struct) {
            Object[] values1 = ((Struct)p1).getFieldValues();
            Object[] values2 = ((Struct)p2).getFieldValues();
            Assert.assertEquals(values1.length, values2.length);
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              elementEqual = elementEqual
                  && ((values1[i] == values2[i]) || values1[i]
                      .equals(values2[i]));
            }
            exactMatch = elementEqual;
          }
          else {
            exactMatch = (p2 == p1) || p2.equals(p1);
          }
          if (exactMatch) {
            break;
          }
        }
        if (!exactMatch) {
          Assert
              .fail("Atleast one element in the pair of SelectResults supposedly identical, is not equal ");
        }
      }
    }
  }

  public void formatQueryResult(Object[] results) {
    log("\nResults: ");
    for (Object result : results) {
      if (result == null) {
        log("NULL");
      } else if (result == QueryService.UNDEFINED) {
        log("UNDEFINED");
      } else if (result instanceof SelectResults) {
        Collection<?> collection = ((SelectResults<?>) result).asList();
        StringBuffer sb = new StringBuffer();
        for (Object e : collection) {
          sb.append(e + "\n\t");
        }
        log(sb.toString());
      } else {
        log("Unknown");
      }
    }
  }

  public LogWriter getLogger() {
    if (cache == null) {
      return null;
    }
    return cache.getLogger();
  }
  
  public Cache getCache() {
    return cache;
  }

  public void log(Object message) {
    CacheUtils.log(message);
  }
}
