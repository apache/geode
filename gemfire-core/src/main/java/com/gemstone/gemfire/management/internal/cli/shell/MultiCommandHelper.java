package com.gemstone.gemfire.management.internal.cli.shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.management.internal.cli.parser.SyntaxConstants;

/**
 * 
 * @author tushark
 *
 */
public class MultiCommandHelper {
  
  public static List<String> getMultipleCommands(String input) {
    Map<Integer,List<String>> listMap = new HashMap<Integer,List<String>>();
    String as[] = input.split(SyntaxConstants.COMMAND_DELIMITER);
    int splitCount=0;
    for(String a : as) {      
      if(a.endsWith("\\")) {
        String a2 = a.substring(0,a.length()-1);
        updateList(listMap,a2,splitCount);
      } else {
        updateList(listMap,a,splitCount);
        splitCount++;        
      }
    }
    List<String> finalList = new ArrayList<String>();
    for(int i=0;i<listMap.size();i++){
      List<String> list = listMap.get(i);
      StringBuilder sb = new StringBuilder();
      for(int k=0;k<list.size();k++) {
        sb.append(list.get(k));
        if(k<list.size()-1)
          sb.append(";");
      }
      finalList.add(sb.toString());
    }
    return finalList;
  }
  
  private static void updateList(Map<Integer,List<String>> listMap, String a , int splitCount){
    if(listMap.containsKey(splitCount))
      listMap.get(splitCount).add(a);
    else {
      List<String> list = new ArrayList<String>();
      list.add(a);
      listMap.put(splitCount, list);
    }
  }

}
