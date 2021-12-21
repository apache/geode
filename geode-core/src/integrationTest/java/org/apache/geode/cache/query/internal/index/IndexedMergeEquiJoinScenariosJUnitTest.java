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
/*
 * Created on Dec 2, 2005
 */
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.City;
import org.apache.geode.cache.query.data.Country;
import org.apache.geode.cache.query.data.District;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.State;
import org.apache.geode.cache.query.data.Village;
import org.apache.geode.cache.query.functional.StructSetOrResultsSet;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexedMergeEquiJoinScenariosJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.log("Creating regions");
    CacheUtils.startCache();
    Region region1 = CacheUtils.createRegion("Portfolios1", Portfolio.class);
    for (int i = 0; i < 5; i++) {
      region1.put("" + i, new Portfolio(i));
    }
    Region region2 = CacheUtils.createRegion("Portfolios2", Portfolio.class);
    for (int i = 0; i < 2; i++) {
      region2.put("" + i, new Portfolio(i));
    }
    Region region3 = CacheUtils.createRegion("Portfolios3", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      region3.put("" + i, new Portfolio(i));
    }
    CacheUtils.log("Portfolio regions created and populated");
    Region region4 = CacheUtils.createRegion("Countries1", Country.class);
    Region region5 = CacheUtils.createRegion("Countries2", Country.class);
    Region region6 = CacheUtils.createRegion("Countries3", Country.class);
    Village v1 = new Village("MAHARASHTRA_VILLAGE1", 123456);
    Village v2 = new Village("PUNJAB_VILLAGE1", 123789);

    Set villages = new HashSet();
    villages.add(v1);
    villages.add(v2); // villages.add(v3); //villages.add(v4); villages.add(v5);

    /* create cities */
    City ct1 = new City("MUMBAI", 123456);
    City ct2 = new City("PUNE", 123789);

    Set cities = new HashSet();
    cities.add(ct1);
    cities.add(ct2); // cities.add(ct3); cities.add(ct4);

    /* create districts */
    District d1 = new District("MUMBAIDIST", cities, villages);
    District d2 = new District("PUNEDIST", cities, villages);

    Set districts = new HashSet();
    districts.add(d1);
    districts.add(d2); // districts.add(d3); districts.add(d4);

    /* create states */
    State s1 = new State("MAHARASHTRA", "west", districts);
    State s2 = new State("PUNJAB", "north", districts);

    Set states = new HashSet();
    states.add(s1);
    states.add(s2); // states.add(s3); //states.add(s4); states.add(s5);

    /* create countries */
    Country c1 = new Country("INDIA", "asia", states);
    Country c2 = new Country("ISRAEL", "africa", states);

    for (int i = 1; i < 3; i++) {
      int temp;
      temp = i % 3;
      switch (temp) {
        case 1:
          region4.put(new Integer(i), c1);
          region5.put(new Integer(i), c1);
          region6.put(new Integer(i), c1);
          break;

        case 2:
          region4.put(new Integer(i), c2);
          region5.put(new Integer(i), c2);
          region6.put(new Integer(i), c2);
          break;


        default:
          CacheUtils.log("Nothing to add in region for: " + temp);
          break;

      }// end of switch
    } // end of for
    CacheUtils.log("Country regions created and populated");
  }

  @Test
  public void testNonNestedQueries() throws Exception {
    CacheUtils.getQueryService();
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    try {
      String[] queries = {
          /*
           * 1*
           * "select distinct * from /Portfolios1 pf1, /Portfolios2 pf2, /Countries1 c1, /Countries2 c2 "
           * + "where pf1.status = pf2.status and c1.name = c2.name", /*2
           */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, pf1.positions.values pos1, "
              + SEPARATOR + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR
              + "Countries2 c2 "
              + "where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM'",
          /* 3 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name",
          /* 4 */ "Select distinct * "
              + "from " + SEPARATOR + "Portfolios1 pfos, pfos.positions.values Pos1, " + SEPARATOR
              + "Countries1 c1, " + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3, "
              + SEPARATOR + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and c1.name = c2.name or c3.name = 'INDIA' and pfo3.status != 'inactive' or pfo3.\"type\" = 'type1' and pfo3.status = pfos.status ",
          /* 5 */ "Select distinct * "
              + "from " + SEPARATOR + "Portfolios1 pfos, pfos.positions.values Pos1, " + SEPARATOR
              + "Countries1 c1, " + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3, "
              + SEPARATOR + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' or c1.name = c2.name or c3.name = 'INDIA' and pfo3.status != 'inactive' or pfo3.\"type\" = 'type1' and pfo3.status = pfos.status ",
          /* 6 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2, " + SEPARATOR
              + "Countries3 c3, " + SEPARATOR + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' or " + "c1.name = c2.name or "
              + "pfo3.status != 'inactive' or " + "pfo3.status = pfos.status ",
          /* 7 */ "Select distinct * "
              + "from " + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists, dists.cities ct1, dists.villages villgs1, "
              + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3, " + SEPARATOR
              + "Portfolios3 pfo3 " + "where "
              + "c1.name = c2.name or " + "ct1.name != 'PUNE' or "
              + "villgs1.name = 'MAHARASHTRA_VILLAGE1'",
          /* 8 */ "Select distinct * "
              + "from " + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists, dists.cities ct1, dists.villages villgs1, "
              + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3 " + "where "
              + "c1.name = c2.name and "
              + "ct1.name != 'PUNE' and " + "villgs1.name = 'MAHARASHTRA_VILLAGE1'",

          /* 9 */ "Select distinct * "
              + "from " + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, c2.states s2, "
              + SEPARATOR
              + "Countries3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 "
              + "where " + "c1.name = c2.name and " + "ct1.name != 'PUNE' and "
              + "villgs1.name = 'MAHARASHTRA_VILLAGE1' or " + "villgs1.name = villgs3.name or "
              + "s2.name = 'PUNJAB' or " + "ct1.name = ct3.name and "
              + "dists3.name = 'MUMBAIDIST'",
          /* 10 */ "Select distinct * "
              + "from " + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, c2.states s2, "
              + SEPARATOR
              + "Countries3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 "
              + "where " + "c1.name = c2.name or " + "ct1.name != 'PUNE' and "
              + "villgs1.name = 'MAHARASHTRA_VILLAGE1' or " + "villgs1.name = villgs3.name or "
              + "s2.name = 'PUNJAB' or " + "ct1.name = ct3.name and "
              + "dists3.name = 'MUMBAIDIST'",
          /* 11 */ "Select distinct * "
              + "from " + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, c2.states s2, "
              + SEPARATOR
              + "Countries3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 "
              + "where " + "c1.name = c2.name and " + "ct1.name != 'PUNE' or "
              + "villgs1.name = 'MAHARASHTRA_VILLAGE1' or " + "villgs1.name = villgs3.name or "
              + "s2.name = 'PUNJAB' or " + "ct1.name = ct3.name and "
              + "dists3.name = 'MUMBAIDIST'",
          /* 12 */ "Select distinct * "
              + "from " + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, c2.states s2, "
              + SEPARATOR
              + "Countries3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 "
              + "where " + "c1.name = c2.name and " + "ct1.name != 'PUNE' or "
              + "villgs1.name = 'MAHARASHTRA_VILLAGE1' and " + "villgs1.name = villgs3.name or "
              + "s2.name = 'PUNJAB' or " + "ct1.name = ct3.name or " + "dists3.name = 'MUMBAIDIST'",
          /* 13 */ "Select distinct * "
              + "from " + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, c2.states s2, "
              + SEPARATOR
              + "Countries3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 "
              + "where " + "c1.name = c2.name and " + "ct1.name != 'PUNE' and "
              + "villgs1.name = 'MAHARASHTRA_VILLAGE1' and " + "villgs1.name = villgs3.name or "
              + "s2.name = 'PUNJAB' and " + "ct1.name = ct3.name and "
              + "dists3.name = 'MUMBAIDIST'",
          /* 14 */ "Select distinct * "
              + "from " + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, c2.states s2, "
              + SEPARATOR
              + "Countries3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 "
              + "where " + "c1.name = c2.name or " + "ct1.name != 'PUNE' or "
              + "villgs1.name = 'MAHARASHTRA_VILLAGE1' or " + "villgs1.name = villgs3.name or "
              + "s2.name = 'PUNJAB' or " + "ct1.name = ct3.name or " + "dists3.name = 'MUMBAIDIST'",
          /* 15 */ "Select distinct * "
              + "from " + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, c2.states s2, "
              + SEPARATOR
              + "Countries3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 "
              + "where " + "c1.name = c2.name and " + "ct1.name != 'PUNE' and "
              + "villgs1.name = 'MAHARASHTRA_VILLAGE1' and " + "villgs1.name = villgs3.name and "
              + "s2.name = 'PUNJAB' and " + "ct1.name = ct3.name and "
              + "dists3.name = 'MUMBAIDIST'",

          /* 16 */ "Select distinct * "
              + "from " + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, c2.states s2, "
              + SEPARATOR
              + "Countries3 c3, c3.states sts3, sts3.districts dists3, dists3.cities ct3, dists3.villages villgs3 "
              + "where " + "c1.name = c2.name and " + "sts1.name != 'PUNJAB' and "
              + "ct1.name != 'PUNE' and " + "villgs1.name = 'MAHARASHTRA_VILLAGE1' and "
              + "villgs1.name = villgs3.name and " + "sts3.name != sts1.name and "
              + "s2.name = 'PUNJAB' and " + "ct1.name = ct3.name and "
              + "dists3.name = 'MUMBAIDIST' and dists3.name != s2.name",
          /* 17 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, " + SEPARATOR + "Countries1 c1, " + SEPARATOR
              + "Countries2 c2, "
              + SEPARATOR + "Countries3 c3, " + SEPARATOR + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and "
              + "c1.name = c2.name or " + "pfo3.status != 'inactive' and "
              + "pfo3.status = pfos.status ",
          /* 18 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, " + SEPARATOR + "Countries1 c1, " + SEPARATOR
              + "Countries2 c2, "
              + SEPARATOR + "Countries3 c3, " + SEPARATOR + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and "
              + "c1.name = c2.name or " + "pfo3.status != 'inactive' and "
              + "pfo3.status = pfos.status ",
          /* 19 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, "
              + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3, " + SEPARATOR
              + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and " + "c1.name = c2.name or "
              + "pfo3.status != 'inactive' and "
              + "pfo3.status = pfos.status and villgs1.name = 'MAHARASHTRA_VILLAGE1' ",
          /* 20 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, "
              + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3, " + SEPARATOR
              + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and " + "c1.name = c2.name or "
              + "pfo3.status != 'inactive' and "
              + "pfo3.status = pfos.status and villgs1.name = 'MAHARASHTRA_VILLAGE1' or pfos.ID != 0",
          /* 21 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, "
              + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3, " + SEPARATOR
              + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and " + "c1.name = c2.name or "
              + "pfo3.status != 'inactive' and "
              + "pfo3.status = pfos.status and villgs1.name = 'MAHARASHTRA_VILLAGE1' or pfos.ID != 0",
          /* 22 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, "
              + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3, " + SEPARATOR
              + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and " + "c1.name = c2.name or "
              + "pfo3.status != 'inactive' and "
              + "pfo3.status = pfos.status and villgs1.name = 'MAHARASHTRA_VILLAGE1' or pfos.ID != 0",



          /* 23 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, " + SEPARATOR + "Countries1 c1, " + SEPARATOR
              + "Countries2 c2, "
              + SEPARATOR + "Countries3 c3, " + SEPARATOR + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and "
              + "(c1.name = c2.name or " + "pfo3.status != 'inactive') and "
              + "pfo3.status = pfos.status ",
          /* 24 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, " + SEPARATOR + "Countries1 c1, c1.states s1, "
              + SEPARATOR + "Countries2 c2, c2.states s2, " + SEPARATOR + "Countries3 c3, "
              + SEPARATOR + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and " + "((c1.name = c2.name or "
              + "pfo3.status != 'inactive') and "
              + "pfo3.status = pfos.status) or s1.name = 'MAHARASHTRA' and s2.name != 'MAHARASHTRA'",
          /* 25 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, "
              + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3, " + SEPARATOR
              + "Portfolios3 pfo3 "
              + "where (Pos1.secId = 'YHOO' and " + "c1.name = c2.name or "
              + "pfo3.status != 'inactive') and "
              + "pfo3.status = pfos.status and villgs1.name = 'MAHARASHTRA_VILLAGE1' ",
          /* 26 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, "
              + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3, " + SEPARATOR
              + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and " + "c1.name = c2.name or "
              + "pfo3.status != 'inactive' and "
              + "pfo3.status = pfos.status and (villgs1.name = 'MAHARASHTRA_VILLAGE1' or pfos.ID != 0)",
          /* 27 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, "
              + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3, " + SEPARATOR
              + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and " + "(c1.name = c2.name or "
              + "pfo3.status != 'inactive') and "
              + "pfo3.status = pfos.status and (villgs1.name = 'MAHARASHTRA_VILLAGE1' or pfos.ID != 0)",
          /* 28 */ "Select distinct * " + "from " + SEPARATOR + "Portfolios1 pfos, "
              + "pfos.positions.values Pos1, "
              + SEPARATOR
              + "Countries1 c1, c1.states sts1, sts1.districts dists1, dists1.cities ct1, dists1.villages villgs1, "
              + SEPARATOR + "Countries2 c2, " + SEPARATOR + "Countries3 c3, " + SEPARATOR
              + "Portfolios3 pfo3 "
              + "where Pos1.secId = 'YHOO' and " + "(c1.name = c2.name or "
              + "pfo3.status != 'inactive' and "
              + "pfo3.status = pfos.status and (villgs1.name = 'MAHARASHTRA_VILLAGE1' or pfos.ID != 0))",
          /* 29 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or false or c1.name = c2.name",
          /* 30 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and pos1.secId = 'IBM' and true",
          /* 31 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and true",
          /* 32 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM'",
          /* 33 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and false",
          /* 34 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or false or c1.name = c2.name or pf2.ID = 1 or c1.name = 'INDIA'",
          /* 35 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and pos1.secId = 'IBM' and true or pf1.ID != 3",
          /* 36 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and true or pf1.ID = pf2.ID",
          /* 37 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM' or false and pf1.ID = pf2.ID",
          /* 38 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and false and pf1.ID = pf2.ID",

          /* 39 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or false or c1.name = c2.name or c1.name = 'INDIA' or pf1.ID = 2",
          /* 40 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and pos1.secId = 'IBM' and true",
          /* 41 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and true or c1.name = 'INDIA' or pf1.ID = 2",
          /* 42 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM' or c1.name = 'INDIA' or pf2.ID = 2",
          /* 43 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and false or c1.name = 'INDIA' or pf2.ID = 2",
          // FAILING /*44*/
          "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or false or c1.name = c2.name or pf2.ID = 1 or c1.name = 'INDIA' or c1.name = 'INDIA' or pf1.ID = 2",
          /* 45 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and pos1.secId = 'IBM' and true or pf1.ID != 3 or c1.name = 'INDIA' or pf1.ID = 2",
          /* 46 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and true or pf1.ID = pf2.ID or c1.name = 'INDIA' or pf2.ID = 2",
          /* 47 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM' or false and pf1.ID = pf2.ID or c1.name = 'INDIA' or pf1.ID = 2",
          /* 48 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and false and pf1.ID = pf2.ID or c1.name = 'INDIA' or pf2.ID = 2",

          /* 49 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or false or c1.name = c2.name and c1.name = 'INDIA' and pf1.ID = 2",
          /* 50 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and pos1.secId = 'IBM' and true and c1.name = 'INDIA' and pf2.ID = 2",
          /* 51 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and true and c1.name = 'INDIA' and pf2.ID = 2",
          /* 52 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM' and c1.name = 'INDIA' and pf2.ID = 2",
          /* 53 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and false and c1.name = 'INDIA' and pf2.ID = 2",
          /* 54 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or false or c1.name = c2.name or pf2.ID = 1 or c1.name = 'INDIA' and c1.name = 'INDIA' and pf2.ID = 2",
          /* 55 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and pos1.secId = 'IBM' and true or pf1.ID != 3 and c1.name = 'INDIA' and pf2.ID = 2",
          /* 56 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and true or pf1.ID = pf2.ID and c1.name = 'INDIA' and pf2.ID = 2",
          /* 57 */ "select distinct * from " + SEPARATOR
              + "Portfolios1 pf1, pf1.positions.values pos1, " + SEPARATOR + "Portfolios2 pf2, "
              + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status and c1.name = c2.name or pos1.secId = 'IBM' or false and pf1.ID = pf2.ID and c1.name = 'INDIA' and pf2.ID = 2",
          /* 58 */ "select distinct * from " + SEPARATOR + "Portfolios1 pf1, " + SEPARATOR
              + "Portfolios2 pf2, " + SEPARATOR + "Countries1 c1, " + SEPARATOR + "Countries2 c2 "
              + "where pf1.status = pf2.status or c1.name = c2.name and false and pf1.ID = pf2.ID and c1.name = 'INDIA' and pf2.ID = 2",};

      SelectResults[][] rs = new SelectResults[queries.length][2];

      for (int i = 0; i < queries.length; i++) {
        CacheUtils.log("Running query number :" + (i + 1) + " without Index");
        Query q = null;
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        rs[i][0] = (SelectResults) q.execute();
      }
      CacheUtils.log("Now creating Indexes");
      createIndex();
      CacheUtils.log("All indexes created ");

      for (int j = 0; j < queries.length; j++) {
        CacheUtils.log("Running query number :" + (j + 1) + " with Index");
        if (j == 4) {
          System.out.print("Hi");
        }
        Query q2 = null;
        q2 = CacheUtils.getQueryService().newQuery(queries[j]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        try {
          rs[j][1] = (SelectResults) q2.execute();
          if (!observer.isIndexesUsed) {
            fail("------------ INDEX IS NOT USED FOR THE QUERY:: " + q2.getQueryString());
          }

        } catch (Exception e) {
          System.out.println(
              "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!CAUGHT EXCPETION AT QUERY NO: " + (j + 1));
          e.printStackTrace();
          fail();
        }
      }
      StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
      ssORrs.CompareQueryResultsWithoutAndWithIndexes(rs, queries.length, queries);
    } finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;
    }
  }

  public void createIndex() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();

    qs.createIndex("Portfolio1secIdIdx", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "Portfolios1 pf, pf.positions.values b");
    qs.createIndex("Portfolio1IdIdx1", IndexType.FUNCTIONAL, "pf.ID",
        SEPARATOR + "Portfolios1 pf, pf.positions.values b");
    qs.createIndex("Portfolio1Idindex2", IndexType.FUNCTIONAL, "pf.ID",
        SEPARATOR + "Portfolios1 pf");
    qs.createIndex("Portfolio1statusIdx1", IndexType.FUNCTIONAL, "pf.status",
        SEPARATOR + "Portfolios1 pf, pf.positions.values b");
    qs.createIndex("Portfolio1statusIdx2", IndexType.FUNCTIONAL, "pf.status",
        SEPARATOR + "Portfolios1 pf");

    qs.createIndex("Portfolio2secIdIdx", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "Portfolios2 pf, pf.positions.values b");
    qs.createIndex("Portfolio2IdIdx1", IndexType.FUNCTIONAL, "pf.ID",
        SEPARATOR + "Portfolios2 pf, pf.positions.values b");
    qs.createIndex("Portfolio2Idindex2", IndexType.FUNCTIONAL, "pf.ID",
        SEPARATOR + "Portfolios2 pf");
    qs.createIndex("Portfolio2statusIdx1", IndexType.FUNCTIONAL, "pf.status",
        SEPARATOR + "Portfolios2 pf, pf.positions.values b");
    qs.createIndex("Portfolio2statusIdx2", IndexType.FUNCTIONAL, "pf.status",
        SEPARATOR + "Portfolios2 pf");

    qs.createIndex("Portfolio3secIdIdx", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "Portfolios3 pf, pf.positions.values b");
    qs.createIndex("Portfolio3IdIdx1", IndexType.FUNCTIONAL, "pf.ID",
        SEPARATOR + "Portfolios3 pf, pf.positions.values b");
    qs.createIndex("Portfolio3Idindex2", IndexType.FUNCTIONAL, "pf.ID",
        SEPARATOR + "Portfolios3 pf");
    qs.createIndex("Portfolio3statusIdx1", IndexType.FUNCTIONAL, "pf.status",
        SEPARATOR + "Portfolios3 pf, pf.positions.values b");
    qs.createIndex("Portfolio3statusIdx2", IndexType.FUNCTIONAL, "pf.status",
        SEPARATOR + "Portfolios3 pf");

    /* Indices on region1 */
    qs.createIndex("villageName1", IndexType.FUNCTIONAL, "v.name",
        SEPARATOR + "Countries1 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("cityName1", IndexType.FUNCTIONAL, "ct.name",
        SEPARATOR + "Countries1 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("countryNameA", IndexType.FUNCTIONAL, "c.name",
        SEPARATOR + "Countries1 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("countryNameB", IndexType.FUNCTIONAL, "c.name", SEPARATOR + "Countries1 c");

    /* Indices on region2 */
    qs.createIndex("stateName2", IndexType.FUNCTIONAL, "s.name",
        SEPARATOR + "Countries2 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("cityName2", IndexType.FUNCTIONAL, "ct.name",
        SEPARATOR + "Countries2 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("countryNameB", IndexType.FUNCTIONAL, "c.name", SEPARATOR + "Countries2 c");

    /* Indices on region3 */
    qs.createIndex("districtName3", IndexType.FUNCTIONAL, "d.name",
        SEPARATOR + "Countries3 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("villageName3", IndexType.FUNCTIONAL, "v.name",
        SEPARATOR + "Countries3 c, c.states s, s.districts d, d.cities ct, d.villages v");
    qs.createIndex("cityName3", IndexType.FUNCTIONAL, "ct.name",
        SEPARATOR + "Countries3 c, c.states s, s.districts d, d.cities ct, d.villages v");

  }// end of createIndex

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }


  class QueryObserverImpl extends QueryObserverAdapter {

    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }
}
