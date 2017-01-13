package com.examples.gemfire.net;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Properties;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.BridgeLoader;
import org.apache.geode.cache.util.BridgeWriter;
import org.apache.geode.distributed.DistributedSystem;


public class ProductBrowser {

	/**
	 * @param args
	 */
	/** This example's connection to the distributed system */
	private DistributedSystem system = null;

	/** Cache <code>Region</code> currently reviewed by this example */
	private Region productRegion;
	
	/** The cache used in the example */
	private Cache cache;
	
	private File xmlFile = new File("product.xml");
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		ProductBrowser client = new ProductBrowser();
		try {
			client.initialize();
			client.go();
		}
		catch (CacheException ce)
		{
			ce.printStackTrace();
		}
		catch (Exception e)
		{
			System.out.println("\nException...");
			e.printStackTrace();
		}

	}
	
	/*
	 * Initialized the cache client.  Gemfire.properties and product.xml are required
	 * for initialization.  No additional cache or region parameters are specified
	 * programmatically
	 */
	private void initialize() throws CacheException, Exception  {	
		Properties props = new Properties();
		props.setProperty("cache-xml-file", this.xmlFile.toString());
		system = DistributedSystem.connect(props);
		cache = CacheFactory.create(system);
		
		productRegion = cache.getRegion("product");
		
		// register the product class for heterogeneous client access.
		Instantiator.register(new Instantiator(Product.class, (byte) 0x07) {
            public DataSerializable newInstance() {
            	return new Product();
        }
        });
		
	}
		
	/*
	 * Allows the user to search and create entries in the cache.
	 */
	private void go() throws CacheException, Exception
	{
		String command;
		String key;
		BufferedReader bin = new BufferedReader(new InputStreamReader(System.in));
				
		do { 
			System.out.print("\n\n");
			System.out.print("Enter the operation to execute.  'Get' to find an object, 'Put' to insert/update an object, or 'Exit' to end:  ");
			
			command = bin.readLine();
			
			if (command.equalsIgnoreCase("get"))
			{
				System.out.print("Enter the Product ID to search for:  ");
				key = bin.readLine();
				Product prod = (Product)productRegion.get(key);
				
				if (prod != null) {
					System.out.println("Product ID =		" + prod.getProductID());
					System.out.println("Product Name =		" + prod.getName());
					System.out.println("Product Number =	" + prod.getProductNumber());
					System.out.println("Color =			" + prod.getColor());
					System.out.println("Stock Level = 		" + prod.getSafetyStockLevel());
					System.out.println("Reorder Point = 	" + prod.getReorderPoint());
					System.out.println("Product Cost =		" + prod.getStandardCost());
					System.out.println("List Price = 		" + prod.getListPrice());
					System.out.println("Available as of = 	" + prod.getSellStartDate());
					System.out.println("Discontinue as of = 	" + prod.getDiscontinuedDate());
				}
				else {
					System.out.println("Product not found in the cache.");
				}
			}
			else if (command.equalsIgnoreCase("put"))
			{
				System.out.print("ProductId:  ");
				String pId = bin.readLine();
				Integer pKey = new Integer(pId);
				
				System.out.print("Product Name:  ");
				String pName = bin.readLine();
				
				System.out.print("Product Number:  ");
				String pNum = bin.readLine();
				
				System.out.print("Color:  ");
				String color = bin.readLine();
				
				System.out.print("Stock Level (int):  ");
				String stockLevel = bin.readLine();
				
				System.out.print("Reorder Point (int):  ");
				String reorderPoint = bin.readLine();
				
				System.out.print("Product Cost (double):  ");
				String cost = bin.readLine();
				
				System.out.print("List Price (double):  ");
				String listPrice = bin.readLine();
				
				System.out.print("Available as of (string):  ");
				String availableAsOf = bin.readLine();
				
				System.out.print("Discontinue as of (string):  ");
				String discAsOf = bin.readLine();
				
				try {
					// Populate the product object with the values specified and insert into the 
					// cache.  Please note that no type checking is performed.  Entering an 
					// invalid type will result in an exception 
					Product prod = new Product();
					prod.setProductID(pKey.intValue());
					prod.setName(pName);
					prod.setProductNumber(pNum);
					prod.setMakeFlag("false");
					prod.setFinishedGoodsFlag("false");
					prod.setColor(color);
					prod.setSafetyStockLevel(new Integer(stockLevel).intValue());
					prod.setReorderPoint(new Integer(reorderPoint).intValue());
					prod.setStandardCost(new Double(cost).doubleValue());
					prod.setListPrice(new Double(listPrice).doubleValue());
					prod.setDaysToManufacture(2);
					prod.setSellStartDate(availableAsOf);
					prod.setDiscontinuedDate(discAsOf);
				
					productRegion.put(pId, prod);
				}
				catch (NumberFormatException nfe)
				{
					System.out.println("\n\nException occurred populating the Product Object.  ");
					System.out.println("------> Stock level, reorder point, list price and product cost must be numeric values");
				}
				catch (Exception re)
				{
					System.out.println("Eception occurred adding the object to the cache.  Exception is:");
					re.printStackTrace();
				}
				
			}
		} while (!command.equalsIgnoreCase("exit"));
	}
	

}
