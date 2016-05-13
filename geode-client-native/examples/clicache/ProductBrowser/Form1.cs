/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using GemStone.GemFire.Cache;

namespace ProductBrowser
{
  public partial class ProductBrowser : Form, ICacheListener
  {
    //Cache cache = null;
    GemStone.GemFire.Cache.Region prodRegion = null;
    private string logLevel = "debug";
    private string logFile = "productBrowserLog";

    public ProductBrowser()
    {
      InitializeComponent();
      Serializable.RegisterType(Product.CreateInstance);

      /*
       * Initialize Cache system properties
       */      
      GemStone.GemFire.Cache.Properties prop = GemStone.GemFire.Cache.Properties.Create();
      prop.Insert("log-file", logFile);
      prop.Insert("log-level", logLevel);
      prop.Insert("name", "ProductCache");
      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(prop);      

      try
      {
        /*
         * Create the GemFire Client cache
         */

        Cache cache = cacheFactory.SetSubscriptionEnabled(true).Create();

        /*
         * Specify required region attributes
         */

        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);

        /*
         * Create the region and register interest in all keys
         */
         prodRegion = regionFactory
          .SetCachingEnabled(true)
          .SetCacheListener(this)
          .Create("product");      

         prodRegion.RegisterAllKeys();
      }
      catch (Exception e)
      {
        MessageBox.Show("Error during client initialization.  " + e.Message);
      }

    }

    private void btnCreate_Click(object sender, EventArgs e)
    {
      Product prd = new Product();
      prd.productId = Convert.ToInt32(txtProdId.Text);
      prd.name = txtProdName.Text;
      prd.productNumber = txtProdNum.Text;
      prd.makeFlag = "false";
      prd.finishedGoodsFlag = "false";
      prd.color = "Black";
      prd.safetyLockLevel = Convert.ToInt32(txtStock.Text);
      prd.reorderPoint = Convert.ToInt32(txtReorder.Text);
      prd.standardCost = Convert.ToDouble(txtCost.Text);
      prd.listPrice = Convert.ToDouble(txtList.Text);
      prd.daysToManufacture = Convert.ToInt32(txtMfgDays.Text);
      prd.sellStartDate = txtSellDate.Text;
      prd.discontinuedDate = txtDiscDate.Text;

      try
      {
        // as the unique identifier is ProductId, we can use this as the key
        CacheableString key = new CacheableString(txtProdId.Text);
        prodRegion.Put(key, prd);
      }
      catch (Exception ex)
      {
        MessageBox.Show("Error creating a new product.  " + ex.Message);
      }
    }

    private void btnSearch_Click(object sender, EventArgs e)
    {
      //  perform a search based upon the ProductId.  If the ProductId is not specified, thrown an exception.
      if (txtProdId.Text == null)
      {
        MessageBox.Show("Searching enabled on Product ID only");
      }
      else
      {
        string prodId = txtProdId.Text;
        clearTxtBoxes();
        Product prod = (Product)prodRegion.Get(prodId);

        if (prod == null)
        {
          MessageBox.Show("Unable to find a product with a ProductId of " + prodId);
        }
        else
        {
          txtProdId.Text = Convert.ToString(prodId);
          txtProdName.Text = prod.name;
          txtProdNum.Text = prod.productNumber;
          txtStock.Text = Convert.ToString(prod.safetyLockLevel);
          txtReorder.Text = Convert.ToString(prod.reorderPoint);
          txtCost.Text = Convert.ToString(prod.standardCost);
          txtList.Text = Convert.ToString(prod.listPrice);
          txtMfgDays.Text = Convert.ToString(prod.daysToManufacture);
          txtSellDate.Text = prod.sellStartDate;
          txtDiscDate.Text = prod.discontinuedDate;
        }
      }
    }

    private void btnClear_Click(object sender, EventArgs e)
    {
      clearTxtBoxes();
    }

    private void clearTxtBoxes()
    {
      txtCost.Clear();
      txtList.Clear();
      txtMfgDays.Clear();
      txtProdId.Clear();
      txtProdName.Clear();
      txtProdNum.Clear();
      txtReorder.Clear();
      txtStock.Clear();
      txtSellDate.ResetText();
      txtDiscDate.ResetText();
      txtEvent.ResetText();
    }


    #region ICacheListener Members

    public void AfterCreate(EntryEvent ev)
    {
      string createMessage;

      ICacheableKey cKey = ev.Key;
      CacheableString cStr = cKey as CacheableString;

      Product newProd = (Product)ev.NewValue;

      createMessage = string.Format("Product ID {1} has been created. " +
        "Product details are:{0}\tProduct Name :  {2}{0}\tProduct Number :  " +
        "{3}{0}\tColor :  {4}{0}\tStock Level :  {5}{0}\tReorder Point :  " +
        "{6}{0}\tProduct Cost :  {7}{0}\tList Price :  " +
        "{8}{0}\tAvailable as of :  {9}{0}\tDiscontinued as of :  {10}",
        Environment.NewLine, cStr.Value, newProd.name, newProd.productNumber,
        newProd.color, newProd.safetyLockLevel, newProd.reorderPoint,
        newProd.standardCost, newProd.listPrice, newProd.sellStartDate,
        newProd.discontinuedDate);
    }

    public void AfterDestroy(EntryEvent ev)
    {
      string destroyMessage;

      ICacheableKey cKey = ev.Key;
      CacheableString cStr = cKey as CacheableString;

      destroyMessage = "Product ID " + cStr.Value + " has been destroyed";
    }

    public void AfterInvalidate(EntryEvent ev)
    {
      string invalidateMessage;

      ICacheableKey cKey = ev.Key;
      CacheableString cStr = cKey as CacheableString;

      invalidateMessage = "Product ID " + cStr.Value + " has been invalidated";
    }

    public void AfterRegionDestroy(RegionEvent ev)
    {
      txtEvent.Text = ev.Region.Name + " has been destroyed";
    }

    public void AfterRegionClear(RegionEvent ev)
    {
      txtEvent.Text = ev.Region.Name + " has been cleared";
    }

    public void AfterRegionInvalidate(RegionEvent ev)
    {
      txtEvent.Text = ev.Region.Name + " has been invalidated";
    }
    /*
     * AfterUpdate is invoked after an entry has been updated.  It provide access to the 
     * old and new values of the entry
     */
    public void AfterUpdate(EntryEvent ev)
    {
      // text string containing the 'delta' between the two objects
      string updateMessage = null;

      try
      {
        ICacheableKey cKey = ev.Key;
        CacheableString cStr = cKey as CacheableString;

        updateMessage = "Product ID " + cStr.Value + " has been updated." +
          Environment.NewLine;

        Product newProduct = (Product)ev.NewValue;
        Product oldProduct = (Product)ev.OldValue;

        if (oldProduct != null)
        {
          updateMessage = updateMessage + Environment.NewLine +
            "\tProduct Name :  " + oldProduct.name + " ---> " +
              newProduct.name + Environment.NewLine +
            "\tProduct Number :  " + oldProduct.productNumber + " ---> " +
              newProduct.productNumber + Environment.NewLine +
            "\tColor :  " + oldProduct.color + " ---> " +
              newProduct.color + Environment.NewLine +
            "\tStock Level :  " + oldProduct.safetyLockLevel + " ---> " +
              newProduct.safetyLockLevel + Environment.NewLine +
            "\tReorder Point :  " + oldProduct.reorderPoint + " ---> " +
              newProduct.reorderPoint + Environment.NewLine +
            "\tProduct Cost :  " + oldProduct.standardCost + " ---> " +
              newProduct.standardCost + Environment.NewLine +
            "\tList Price :  " + oldProduct.listPrice + " ---> " +
              newProduct.listPrice + Environment.NewLine +
            "\tAvailable as of :  " + oldProduct.sellStartDate + " ---> " +
              newProduct.sellStartDate + Environment.NewLine +
            "\tDiscontinued as of :  " + oldProduct.discontinuedDate + " ---> " +
              newProduct.discontinuedDate + Environment.NewLine;

          txtEvent.Text = updateMessage;
        }
        else
        {
          updateMessage = "Product ID " + cStr.Value + " has been updated. " +
            "A prior value does not exist.  Product details are: " + Environment.NewLine + Environment.NewLine +
            "\tProduct Name :  " + newProduct.name + Environment.NewLine +
            "\tProduct Number :  " + newProduct.productNumber + Environment.NewLine +
            "\tColor :  " + newProduct.color + Environment.NewLine +
            "\tStock Level :  " + newProduct.safetyLockLevel + Environment.NewLine +
            "\tReorder Point :  " + newProduct.reorderPoint + Environment.NewLine +
            "\tProduct Cost :  " + newProduct.standardCost + Environment.NewLine +
            "\tList Price :  " + newProduct.listPrice + Environment.NewLine +
            "\tAvailable as of :  " + newProduct.sellStartDate + Environment.NewLine +
            "\tDiscontinued as of :  " + newProduct.discontinuedDate + Environment.NewLine;
        }
      }
      catch (Exception e)
      {
        txtEvent.Text = "AfterUpdate exception." +
          Environment.NewLine + e.Message;
      }
    }

    public void AfterRegionLive(RegionEvent ev)
    {
      txtEvent.Text = ev.Region.Name + " is live";
    }

    public void Close(GemStone.GemFire.Cache.Region region)
    {
      txtEvent.Text = region.Name + " has closed";
    }
    public void AfterRegionDisconnected(GemStone.GemFire.Cache.Region region)
    {
      txtEvent.Text = region.Name + " has disconnected";
    }
    #endregion

    private void btnDestroy_Click(object sender, EventArgs e)
    {
      //  Destroy an entry based upon the ProductId (key).  If the ProductId is not specified, notify the user.
      if (txtProdId.Text.Length == 0)
      {
        MessageBox.Show("Please specify a Product Id to destroy.");
      }
      else
      {
        string prodId = txtProdId.Text;

        try
        {
          prodRegion.Destroy(prodId);
          clearTxtBoxes();
        }
        catch (Exception re)
        {
          MessageBox.Show("Unable to destroy entry." +
            Environment.NewLine + re.Message);
        }
      }
    }

    private void btnInvalidate_Click(object sender, EventArgs e)
    {
      //  Invalidate an entry based upon the ProductId (key).  If the ProductId is not specified, notify the user.
      if (txtProdId.Text.Length == 0)
      {
        MessageBox.Show("Please specify a Product Id to invalidate.");
      }
      else
      {
        string prodId = txtProdId.Text;

        try
        {
          prodRegion.Invalidate(prodId);
          clearTxtBoxes();
        }
        catch (Exception re)
        {
          MessageBox.Show("Unable to invalidate entry." +
            Environment.NewLine + re.Message);
        }
      }

    }

  }
}
