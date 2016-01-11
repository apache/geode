/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.testbed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * 
 * TODO
 * 0. SystemAlerts
 * 1. Operations like member-up/down/crash, region create/destroy [7.5 scope]
 * 2. Read events like member-up/down/crash, region create/destroy [7.5 scope]
 * 3. PropFile Writing
 * 4. Link to other remote systems, topology - multi-cluster [7.5] 
 * 
 * @author tushark
 *
 */
public class GemFireDistributedSystem {
  
  private static final String SERVERS = "servers";
  private static final String LOCATORS = "locators";
  private static final String PEERS = "peers";
  private static final String HOSTS = "hosts";
  private static final String REGIONS = "regions";
  private static final String CLIENTS = "clients";
  private static final String SEP = ".";
  private static final String FUNCTIONS = null;
  private static final String CQS = null;
  
  
  List<Server> servers = new ArrayList<Server>();
  List<Client> clients = new ArrayList<Client>();
  List<Locator> locators = new ArrayList<Locator>();
  List<Peer> peers = new ArrayList<Peer>();
  List<Host> hosts = new ArrayList<Host>();
  List<Region> regions = new ArrayList<Region>();
  List<Function> functions = new ArrayList<Function>();
  List<CQ> cqs = new ArrayList<CQ>();
  String dsName = null;
  
  public GemFireDistributedSystem(String name,Properties pr){
    PropFileHelper propertiesFile = new PropFileHelper(pr);
    this.dsName = name;
    readGemfireDS(propertiesFile);
  }
  
  public GemFireDistributedSystem(String name,String fileName) throws IOException{
    PropFileHelper propertiesFile = new PropFileHelper(fileName);
    this.dsName = name;
    readGemfireDS(propertiesFile);
  }
  
  private void readGemfireDS(PropFileHelper propertiesFile) {
    String serverStrings[] = propertiesFile.readValues(dsName + SEP + SERVERS);
    System.out.println("Servers = " + serverStrings.length);
    for(String serverName : serverStrings){
      Server server = new Server();
      server.init(propertiesFile,dsName,serverName);
      servers.add(server);
    }  
    
    String clientStrings[] = propertiesFile.readValues(dsName + SEP + CLIENTS);
    System.out.println("Clients = " + clientStrings.length);
    for(String clientName : clientStrings){
      Client client = new Client();
      client.init(propertiesFile,dsName,clientName);
      clients.add(client);
    }  
    
    String locatorStrings[] = propertiesFile.readValues(dsName + SEP + LOCATORS);
    System.out.println("Locators = " + locatorStrings.length);
    for(String locatorName : locatorStrings){
      Locator locator = new Locator();
      locator.init(propertiesFile,dsName,locatorName);
      locators.add(locator);
    }
    
    String peerStrings[] = propertiesFile.readValues(dsName + SEP + PEERS);
    System.out.println("Peers = " + peerStrings.length);
    for(String peerName : peerStrings){
      Peer peer = new Peer();
      peer.init(propertiesFile,dsName,peerName);
      peers.add(peer);
    }
    
    String hostsStrings[] = propertiesFile.readValues(dsName + SEP + HOSTS);
    for(String hostName : hostsStrings){
      Host host = new Host();
      host.init(propertiesFile,dsName,hostName);
      hosts.add(host);
    }
    
    String regionsStrings[] = propertiesFile.readValues(dsName + SEP + REGIONS);
    for(String regionName : regionsStrings){
      Region region = new Region();
      region.init(propertiesFile,dsName,regionName);
      regions.add(region);
    }
    
    String functionStrings[] = propertiesFile.readValues(dsName + SEP + FUNCTIONS);
    for(String functionName : functionStrings){
      Function function = new Function();
      function.init(propertiesFile,dsName,functionName);
      functions.add(function);
    }    
    
    String cqStrings[] = propertiesFile.readValues(dsName + SEP + CQS);
    for(String cqName : cqStrings){
      CQ cq = new CQ();
      cq.init(propertiesFile,dsName,cqName);
      cqs.add(cq);
    }
    
  }
  
  public List<Region> getRegions(String memberName) {    
    List<Region> list = new ArrayList<Region>();
    for(Region r : regions){
      if(r.getMembers().contains(memberName))
          list.add(r);
    }
    return list;
   }
  
  public Region getRegion(String regionName) {
    Region r = null;
    for (Region rn : getRegions()) {
      if (rn.getName().equals(regionName)) {
        r = rn;
        break;
      }
    }
    return r;
  }

  public List<Region> getRegions() {
   return regions;
  }
  
  public List<Function> getFunction() {
    return functions;
  }
  
  public List<CQ> getCQs() {
    return cqs;
  }
  
  public List<Server> getServers(){
    return servers;    
  }
  
  public List<Client> getClients(){
    return clients;
  }
  
  public List<Peer> getPeers(){
    return peers;    
  }
  
  public List<Locator> getLocators(){
    return locators;    
  }
  
  public List<Host> getPhysicalHosts(){
    return hosts;    
  }
  
  public static class Base{
    protected Map<String,String> properties=null;
    protected String name;
    
    public void init(PropFileHelper propertiesFile, String dsName, String name) {
      this.name = name;      
      String leadingkey = dsName + SEP + name;
      Map<String,String> map = propertiesFile.readObject(leadingkey);
      map.put("name",name);
      this.properties = map;
    }
    
    public String getName(){
      return properties.get("name");
    }
    
    public String key(String string) {
      return properties.get(string);
    }
    
    public int keyInt(String string) {
      String str = properties.get(string);
      try{
        int index = Integer.parseInt(str);
        return index;
      }catch(Exception e){
        return -1;
      }
    }
    
    public List<String> values(String string) {
      String values= properties.get(string);
      String array[] = values.split(",");
      List<String> list = new ArrayList<String>();
      for(String s:array)
        list.add(s);
      return list;
    }
    
  }
  
  public static class Host extends Base{
    
  }
  
  public static class Server extends Base{
    public String toString(){
      return properties.get("name") + "[on host=" + properties.get("host");
    }
    
    public String getHost(){
      return properties.get("host");
    }
  }
  
  public static class Client extends Base{
    public String toString(){
      return properties.get("name") ;//+ "[on host=" + properties.get("host");
    }
    
    public String getHost(){
      return properties.get("host");
    }
  }
  
  public static class Locator extends Base{
    public String getHost(){
      return properties.get("host");
    }
  }
  
  public static class Peer extends Base{

    public String getHost(){
      return properties.get("host");
    }
  }
  
  public static class Region extends Base{
    public String toString(){
      return properties.get("name") + "[type=" + properties.get("type");
    }
    
    public String getType(){
      return key("type");
    }
    
    public int getEntryCount(){
      return keyInt("entryCount");
    }
    
    public List<String> getWanSenders(){
      return values("wanSenders");
    }
    
    public List<String> getMembers(){
      return values("members");
    }
    
  }
  
  public static class WanSender extends Base{
    
  }

  public static class Function extends Base{
    public String getMemberId(){
      return key("memberId");
    }    
  }
  
  public static class CQ extends Base{
    public String getQuery(){
      return key("query");
    }    
    
    public String getClient(){
      return key("client");
    }
  }
  
  public static class SystemAlert extends Base{
    //TODO
  }
  
  public static void main(String[] args) throws IOException {
    
    GemFireDistributedSystem ds = new GemFireDistributedSystem("t1", "config/testbed.properties");
    System.out.println("Servers = " + ds.getServers());    
    System.out.println("Regions = " + ds.getRegions());
    System.out.println("Clients = " + ds.getClients());
  } 
  
}
