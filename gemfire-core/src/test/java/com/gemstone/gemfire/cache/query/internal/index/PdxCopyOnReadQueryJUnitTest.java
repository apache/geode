package com.gemstone.gemfire.cache.query.internal.index;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;
import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.PortfolioPdx;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class PdxCopyOnReadQueryJUnitTest {
  
  private Cache cache = null;
  
  @Test
  public void testCopyOnReadPdxSerialization() throws Exception {
    List<String> classes = new ArrayList<String>();
    classes.add(PortfolioPdx.class.getCanonicalName());
    ReflectionBasedAutoSerializer serializer = new ReflectionBasedAutoSerializer(classes.toArray(new String[0]));
    
    CacheFactory cf = new CacheFactory();
    cf.setPdxSerializer(serializer);
    cf.setPdxReadSerialized(false);
    cache = cf.create();
    cache.setCopyOnRead(true);
    
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("SimpleObjects");
    Region duplicates = cache.createRegionFactory(RegionShortcut.REPLICATE).create("SimpleObjects_Duplicates");

    for (int i = 0; i < 10; i++) {
      PortfolioPdx t = new PortfolioPdx(i);
      region.put(i,t);
      duplicates.put(i, t);
    }
    
    QueryService qs = cache.getQueryService();
    SelectResults rs = (SelectResults)qs.newQuery("select * from /SimpleObjects").execute();
    assertEquals(10, rs.size());
    Query query = qs.newQuery("select * from /SimpleObjects_Duplicates s where s in ($1)");
    SelectResults finalResults = (SelectResults)query.execute(new Object[]{rs});
    assertEquals(10, finalResults.size());
  }

}
