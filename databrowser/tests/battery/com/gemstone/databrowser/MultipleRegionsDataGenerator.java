package com.gemstone.databrowser;

import hydra.CacheHelper;
import hydra.ConfigHashtable;
import hydra.Log;
import hydra.TestConfig;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Set;
import admin.jmx.RecyclePrms;
import com.gemstone.databrowser.objects.CustomerInvoiceHelper;
import com.gemstone.databrowser.objects.SubEmployee;
import com.gemstone.databrowser.objects.Address;
import com.gemstone.databrowser.objects.Customer;
import com.gemstone.databrowser.objects.Employee;
import com.gemstone.databrowser.objects.Order;
import com.gemstone.databrowser.objects.Shipment;
import com.gemstone.databrowser.objects.Work;
import com.gemstone.databrowser.objects.CustomerInvoice;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.databrowser.objects.GemStoneCustomer;

/**
 * Test to pick and choose various methods for starting and creating regions of
 * a VM.
 * 
 * @author vreddy
 * 
 */
public class MultipleRegionsDataGenerator {
  protected static int absoluteRegionCount;

  static ConfigHashtable conftab = TestConfig.tab();

  static Random keys = new Random();

  public static void populateRegions() {
    Cache cache = CacheHelper.getCache();
    populateEmployee(cache);
    populateCustomer(cache);
    populateShipment(cache);
    populateOrder(cache);
  }

  public static void populateEmployeeRegion() {
    Cache cache = CacheHelper.getCache();
    populateEmployee(cache);
  }

  private static void populateEmployee(Cache cache) {
    // Customer Region.
    Region exampleRegion = cache.getRegion("Employee");
    Log.getLogWriter().fine(
        "Example region, " + exampleRegion.getFullPath()
            + ", created in cache. ");
    Work[] works = new Work[] {
        new Work("Software Engineer", (byte)1, "IT Dept"),
        new Work("Sales Executive", (byte)2, "Sales Dept") };

    List<Employee> data1 = new ArrayList<Employee>();
    data1.add(new Employee("John", true, 22, (byte)2, 'a', works[0]));
    data1.add(new Employee("Mat", true, 30, (byte)3, 'b', works[1]));
    data1.add(new SubEmployee("S1", true, 100, (byte)4, 'c', works[1]));
    data1.add(new SubEmployee("S2", true, 100, (byte)5, 'd', works[1]));
    data1.add(new Employee());

    for (int i = 0; i < data1.size(); i++) {
      Log.getLogWriter().fine("Putting Employee object for key " + i);
      exampleRegion.put("key" + i, data1.get(i));
    }
    Set<Region> subRegions = exampleRegion.subregions(false);
    for (Region Temp : subRegions) {
      for (int i = 0; i < data1.size(); i++) {
        Log.getLogWriter()
            .fine("Putting key for Employee object subregion" + Temp);
        Temp.put("key" + i, data1.get(i));
      }
    }
  }

  public static void populateCustomerRegion() {
    Cache cache = CacheHelper.getCache();
    populateCustomer(cache);
  }

  private static void populateCustomer(Cache cache) {
    // Customer Region.
    Region exampleRegion1 = cache.getRegion("Customer");
    Log.getLogWriter().fine(exampleRegion1 +" region is created in cache. ");
    Address add1 = new Address("MG Road", "Camden", 509321);
    Address add2 = new Address("LS Road", "Plainfield", 609221);
    Address add3 = new Address("5 Penn Plaza, 23rd Floor", "New York", 10001);
    Address add4 = new Address("Airport Road", "Beaverton", 97006);
    Address add5 = new Address("444 Castro Street Suite 520", "Mountain View",
        94041);
    Address add6 = new Address("3 Bunhill Row", "London ", 874332);

    int[][] contacts = new int[][] { { 530672222, 356288233 },
        { 453633321, 837495229 }, { 264729739, 337495737 },
        { 363484637, 563383903 }, { 364484637, 563383933 } };
    String[][] roles = new String[][] {
        { "Software Architect", "Product Manager" },
        { "Business Development", "Product Manager" },
        { "Network Administrator", "Software Developer" } };

    List<Customer> data1 = new ArrayList<Customer>();
    data1.add(new Customer("John", add1, contacts[0]));
    data1.add(new Customer("Jack", add2, contacts[1]));
    data1.add(new Customer("Martin", add3, contacts[2]));
    data1.add(new Customer("Ricky", add4, contacts[3]));
    data1.add(new Customer("Mike", add5, contacts[4]));
    data1.add(new Customer("", add6, contacts[2]));
    data1.add(new Customer("Dick", add3, contacts[1]));
    data1.add(new Customer("Russel", add6, contacts[3]));
    data1.add(new Customer("Mick", add2, contacts[0]));
    data1.add(new Customer("Dan", add4, contacts[4]));
    data1.add(new Customer("Darrel", add1, contacts[2]));
    data1.add(new Customer("", add4, contacts[1]));
    data1.add(new GemStoneCustomer("Bruce", add4, contacts[0], roles[0]));
    data1.add(new GemStoneCustomer("Jason", add2, contacts[4], roles[1]));
    data1.add(new GemStoneCustomer("Lance", add1, contacts[3], roles[2]));

    for (int i = 0; i < data1.size(); i++) {
      Log.getLogWriter().fine("Putting Customer object for key " + i);
      exampleRegion1.put("key" + i, data1.get(i));
    }  
    Set<Region> subRegions = exampleRegion1.subregions(false);
    for (Region Temp : subRegions) {
      for (int i = 0; i < data1.size(); i++) {
        Log.getLogWriter().fine(
            "Putting key for Customer object subregion " + Temp);
        Temp.put("key" + i, data1.get(i));
      }
    }
  }

  public static void populateShipmentRegion() {
    Cache cache = CacheHelper.getCache();
    populateShipment(cache);
  }

  private static void populateShipment(Cache cache) {
    // Customer Region.
    Region exampleRegion = cache.getRegion("Shipment");
    Log.getLogWriter().fine(
        "Example region, " + exampleRegion.getFullPath()
            + ", created in cache. ");
    Address add1 = new Address("MG Road", "Camden", 509321);
    Address add2 = new Address("LS Road", "Plainfield", 609221);
    Address add3 = new Address("5 Penn Plaza, 23rd Floor", "New York", 10001);
    Address add4 = new Address("Airport Road", "Beaverton", 97006);
    Address add5 = new Address("444 Castro Street Suite 520", "Mountain View",
        94041);
    Address add6 = new Address("3 Bunhill Row", "London ", 874332);

    List<Shipment> data1 = new ArrayList<Shipment>();
    data1.add(new Shipment(1, add1, 1, "John"));
    data1.add(new Shipment(2, add4, 2, "Ricky"));
    data1.add(new Shipment(3, add3, 3, "Martin"));
    data1.add(new Shipment(1, add1, 3, "Darrel"));
    data1.add(new Shipment(1, add5, 2, "Mike"));

    data1.add(new Shipment(2, add2, 5, "Mick"));
    data1.add(new Shipment(2, add4, 3, "Dan"));
    data1.add(new Shipment(3, add4, 2, "Bruce"));
    data1.add(new Shipment(2, add6, 4, "Russel"));
    data1.add(new Shipment(1, add1, 1, "Lance"));

    for (int i = 0; i < data1.size(); i++) {
      Log.getLogWriter().fine("Putting Shipment object for key " + i);
      exampleRegion.put("key" + i, data1.get(i));
    }
    Set<Region> subRegions = exampleRegion.subregions(false);
    for (Region Temp : subRegions) {
      for (int i = 0; i < data1.size(); i++) {
        Log.getLogWriter().fine(
            "Putting key for Shipment object subregion" + Temp);        
        Temp.put("key" + i, data1.get(i));
      }
    }
  }

  public static void populateOrderRegion() {
    Cache cache = CacheHelper.getCache();
    populateOrder(cache);
  }

  private static void populateOrder(Cache cache) {
    // Customer Region.
    Region exampleRegion = cache.getRegion("Order");
    Log.getLogWriter().fine(
        "Example region, " + exampleRegion.getFullPath()
            + ", created in cache. ");
    List<Order> data1 = new ArrayList<Order>();
    data1.add(new Order(1, new Date(), "Active", "John"));
    data1.add(new Order(2, new Date(), "Active", "Ricky"));
    data1.add(new Order(3, new Date(), "Inactive", "Martin"));
    data1.add(new Order(4, new Date(), "null", "Darrel"));
    data1.add(new Order(5, new Date(), "Active", "Mike"));
    data1.add(new Order(6, new Date(), "Active", "John"));
    data1.add(new Order(7, new Date(), "Active", "Dick"));
    data1.add(new Order(8, new Date(), "Inactive", "Jason"));
    data1.add(new Order(9, new Date(), "", "John"));
    data1.add(new Order(10, new Date(), "Active", "John"));
    data1.add(new Order(11, new Date(), "NotDefined", "Bruce"));
    data1.add(new Order(12, new Date(), "Inactive", "John"));
    data1.add(new Order(13, new Date(), "Active", "John"));
    data1.add(new Order(14, new Date(), "Active", "Dick"));
    data1.add(new Order(15, new Date(), "Active", "Bruce"));
    data1.add(new Order(16, new Date(), "Inactive", "John"));
    data1.add(new Order(17, new Date(), "Active", "John"));
    data1.add(new Order(18, new Date(), "", "Dan"));
    data1.add(new Order(19, new Date(), "Inactive", "John"));
    data1.add(new Order(20, new Date(), "Active", "Dale"));

    for (int i = 0; i < data1.size(); i++) {
      Log.getLogWriter().fine("Putting Order object for key " + i);
      exampleRegion.put("key" + i, data1.get(i));
    }
    Set<Region> subRegions = exampleRegion.subregions(false);
    for (Region Temp : subRegions) {
      for (int i = 0; i < data1.size(); i++) {
        Log.getLogWriter()
            .fine("Putting key for Order object subregion" + Temp);
        Temp.put("key" + i, data1.get(i));
      }
    }
  }

  public static void populateCustomerInvoice() {
    Cache cache = CacheHelper.getCache();
    populateCustInvoice(cache);
  }

  private static void populateCustInvoice(Cache cache) {
    // CustomerInvoice Region.
    Region exampleRegion = cache.getRegion("CustomerInvoice");
    int noOfEntity = conftab.intAt(RecyclePrms.numberOfEntitiesInRegion, 10);
    Log.getLogWriter().info("Total no of Entities are : " + noOfEntity);
    List<CustomerInvoice> result = CustomerInvoiceHelper
        .prepareCustomerInvoiceData(noOfEntity);

    for (int i = 0; i < noOfEntity; i++) {
      Log.getLogWriter().info("Putting CustomerInvoice object for key " + i);
      try {
        Thread.sleep(200);
        String key = "key" + keys.nextInt(noOfEntity * 10);
        exampleRegion.put(key, result.get(i));
      }
      catch (InterruptedException e) {
        Log.getLogWriter().info("The Interrupted Execption is : " + e);
      }
    }    
  }

}
