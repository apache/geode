package com.gemstone.gemfire.security;

import static org.junit.Assert.assertTrue;

import java.security.Principal;
import java.util.Arrays;
import java.util.Properties;

import org.apache.geode.security.PostProcessor;

import com.gemstone.gemfire.pdx.SimpleClass;
import com.gemstone.gemfire.pdx.internal.PdxInstanceImpl;

public class PDXPostProcessor implements PostProcessor{
  public static byte[] BYTES = {1,0};

  private boolean pdx = false;
  private int count = 0;

  public void init(Properties props){
    pdx = Boolean.parseBoolean(props.getProperty("security-pdx"));
    count = 0;
  }
  @Override
  public Object processRegionValue(final Principal principal,
                                   final String regionName,
                                   final Object key,
                                   final Object value) {
    count ++;
    if(value instanceof byte[]){
      assertTrue(Arrays.equals(BYTES, (byte[])value));
    }
    else if(pdx){
      assertTrue(value instanceof PdxInstanceImpl);
    }
    else {
      assertTrue(value instanceof SimpleClass);
    }
    return value;
  }

  public int getCount(){
    return count;
  }
}
