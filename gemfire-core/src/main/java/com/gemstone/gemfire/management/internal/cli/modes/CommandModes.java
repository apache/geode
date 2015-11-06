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
package com.gemstone.gemfire.management.internal.cli.modes;


import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class CommandModes {
  
  public static final String DEFAULT_MODE = "default";

  public Map<String, Map<String, CommandMode>> getModeMap() {
    return modeMap;
  }

  private static CommandModes _allModes;

  private CommandModes() {
    readAllModes();
  }

  public static CommandModes getInstance() {
    synchronized (CommandModes.class) {
      if (_allModes == null) {
        _allModes = new CommandModes();
      }
      return _allModes;
    }

  }

  private Map<String, Map<String, CommandMode>> modeMap = new HashMap<String, Map<String, CommandMode>>();

  private void readAllModes() {
    try {      
      InputStream stream = CommandModes.class.getResourceAsStream("commands.json");
      byte[] bytes = new byte[50 * 1024];
      int bytesRead = stream.read(bytes);
      String str = new String(bytes, 0, bytesRead);      
      parseModes(str);
      stream.close();
    } catch (IOException e) {
      logException(e);
    } catch (JSONException e) {
      logException(e);
    }
  }

  private void parseModes(String str) throws JSONException {
    JSONObject jsonObject = new JSONObject(str);
    JSONArray array = jsonObject.getJSONArray("commands");
    for (int i = 0; i < array.length(); i++) {
      try{
        addCommandMode(array.getString(i));
      }catch(JSONException e){
        logException(e);
      } catch (IOException e) {        
        logException(e);
      }
    }
  }

  private void logException(Exception e) {
    Cache cache = CacheFactory.getAnyInstance();
    LogWriter logger = cache.getLogger();
    logger.warning("Error parsing command mode descriptor", e);
  }

  private void addCommandMode(String commandName) throws JSONException, IOException {    
    InputStream stream = CommandModes.class.getResourceAsStream(commandName + ".json");
    byte[] bytes = new byte[50 * 1024];
    int bytesRead = stream.read(bytes);
    String str = new String(bytes, 0, bytesRead); 
    JSONObject object = new JSONObject(str);
    String name = object.getString("name");
    JSONArray array = object.getJSONArray("modes");
    Map<String, CommandMode> map = new HashMap<String, CommandMode>();
    for (int i = 0; i < array.length(); i++) {
      CommandMode mode = readMode(array.getJSONObject(i));
      map.put(mode.name, mode);
    }
    modeMap.put(name, map);
  }

  private CommandMode readMode(JSONObject jsonObject) throws JSONException {
    CommandMode mode = new CommandMode();
    mode.name = jsonObject.getString("name");
    mode.text = jsonObject.getString("text");
    mode.leadOption = jsonObject.getString("lead-option");
    mode.options = toStringArray(jsonObject.getJSONArray("options"));
    return mode;
  }

  private String[] toStringArray(JSONArray jsonArray) throws JSONException {
    String[] array = new String[jsonArray.length()];
    for (int i = 0; i < array.length; i++)
      array[i] = jsonArray.getString(i);
    return array;
  }

  public Collection<CommandMode> getCommandModes(String name) {    
    Map<String,CommandMode> commandModes =  modeMap.get(name);
    if(commandModes!=null)
      return commandModes.values();
    else return null;
  }
  
  public CommandMode getCommandMode(String commandName, String modeName) {    
    Map<String,CommandMode> commandModes =  modeMap.get(commandName);
    if(commandModes!=null)
      return commandModes.get(modeName);
    else return null;
  }

  public String toString() {
    return modeMap.toString();
  }

  public static class CommandMode {
    public String name;
    public String leadOption;
    public String[] options;
    public String text;

    public String toString() {
      return "CM: name : " + name + " text:" + text;
    }
  } 

}
