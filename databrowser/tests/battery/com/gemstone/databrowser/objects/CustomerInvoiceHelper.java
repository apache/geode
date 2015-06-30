package com.gemstone.databrowser.objects;

import hydra.ConfigHashtable;
import hydra.Log;
import hydra.TestConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.gemstone.databrowser.Testutil;

/**
 * CustomerInvoiceHelper.
 * 
 * @author vreddy
 * 
 */
public class CustomerInvoiceHelper {

  static ConfigHashtable conftab = TestConfig.tab();

  private static String[] names;

  private static double[] names_prob;

  private static String[] address;

  private static double[] address_prob;

  private static int id;

  static {
    String nm, ads, nmp, adp;
    nm = Testutil.getProperty("CustomerInvoiceHelper.names");
    String[] allnames = nm.split(":");
    names = new String[allnames.length];
    for (int k = 0; k < allnames.length; k++) {
      names[k] = allnames[k];
      Log.getLogWriter().info("The names are :" + names[k]);
    }

    nmp = Testutil.getProperty("CustomerInvoiceHelper.namesprob");
    String[] allnames_prob = nmp.split(":");
    names_prob = new double[allnames_prob.length];
    for (int i = 0; i < allnames_prob.length; i++) {
      names_prob[i] = Double.valueOf(allnames_prob[i]);
      Log.getLogWriter().info("The names_prob's are :" + names_prob[i]);
    }

    ads = Testutil.getProperty("CustomerInvoiceHelper.address");
    String[] alladdress = ads.split(":");
    address = new String[alladdress.length];
    for (int l = 0; l < alladdress.length; l++) {
      address[l] = alladdress[l];
      Log.getLogWriter().info("The addresses are :" + address[l]);
    }

    adp = Testutil.getProperty("CustomerInvoiceHelper.addressprob");
    String[] alladdress_prob = adp.split(":");
    address_prob = new double[alladdress_prob.length];
    for (int j = 0; j < alladdress_prob.length; j++) {
      address_prob[j] = Double.valueOf(alladdress_prob[j]);
      Log.getLogWriter().info("The address_prob's are  :" + address_prob[j]);
    }
  }

  public static List<CustomerInvoice> prepareCustomerInvoiceData(int count) {
    int[] names_counter = { 0, 0, 0 };
    int[] address_counter = { 0, 0, 0, 0 };

    // List of CustomerInvoice.
    ArrayList<CustomerInvoice> result = new ArrayList<CustomerInvoice>();
    Random rand1 = new Random();
    Random rand2 = new Random();
    Log.getLogWriter().info("Total no of Entities are :" + count);
    for (int i = 0; i < count; i++) {
      CustomerInvoice cust = new CustomerInvoice();
      result.add(cust);
      int index = rand1.nextInt(names.length);
      int expectedCount = (int)(names_prob[index] * count);
      while (names_counter[index] >= expectedCount) {
        index = rand1.nextInt(names.length);
        expectedCount = (int)(names_prob[index] * count);
      }
      Log.getLogWriter().info(
          "The Expected Count for name_counter is :" + expectedCount);
      names_counter[index] += 1;
      cust.setName(names[index]);

      cust.setId(i);

      index = rand2.nextInt(address.length);
      expectedCount = (int)(address_prob[index] * count);
      while (address_counter[index] >= expectedCount) {
        index = rand2.nextInt(address.length);
        expectedCount = (int)(address_prob[index] * count);
      }
      Log.getLogWriter().info(
          "The Expected count for address_counter is : " + expectedCount);
      address_counter[index] += 1;
      cust.setAddress(address[index]);
    }
    return result;
  }
}
