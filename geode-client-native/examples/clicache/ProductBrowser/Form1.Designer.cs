/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

namespace ProductBrowser
{
  partial class ProductBrowser
  {
    /// <summary>
    /// Required designer variable.
    /// </summary>
    private System.ComponentModel.IContainer components = null;

    /// <summary>
    /// Clean up any resources being used.
    /// </summary>
    /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
    protected override void Dispose(bool disposing)
    {
      if (disposing && (components != null))
      {
        components.Dispose();
      }
      base.Dispose(disposing);
    }

    #region Windows Form Designer generated code

    /// <summary>
    /// Required method for Designer support - do not modify
    /// the contents of this method with the code editor.
    /// </summary>
    private void InitializeComponent()
    {
      this.lblProdId = new System.Windows.Forms.Label();
      this.txtProdId = new System.Windows.Forms.TextBox();
      this.txtProdName = new System.Windows.Forms.TextBox();
      this.lblProdName = new System.Windows.Forms.Label();
      this.txtProdNum = new System.Windows.Forms.TextBox();
      this.lblProdNum = new System.Windows.Forms.Label();
      this.txtStock = new System.Windows.Forms.TextBox();
      this.label1 = new System.Windows.Forms.Label();
      this.txtReorder = new System.Windows.Forms.TextBox();
      this.label2 = new System.Windows.Forms.Label();
      this.txtCost = new System.Windows.Forms.TextBox();
      this.label3 = new System.Windows.Forms.Label();
      this.txtList = new System.Windows.Forms.TextBox();
      this.label4 = new System.Windows.Forms.Label();
      this.txtMfgDays = new System.Windows.Forms.TextBox();
      this.label5 = new System.Windows.Forms.Label();
      this.label6 = new System.Windows.Forms.Label();
      this.label7 = new System.Windows.Forms.Label();
      this.btnCreate = new System.Windows.Forms.Button();
      this.btnSearch = new System.Windows.Forms.Button();
      this.txtSellDate = new System.Windows.Forms.TextBox();
      this.txtDiscDate = new System.Windows.Forms.TextBox();
      this.btnClear = new System.Windows.Forms.Button();
      this.txtEvent = new System.Windows.Forms.TextBox();
      this.btnDestroy = new System.Windows.Forms.Button();
      this.btnInvalidate = new System.Windows.Forms.Button();
      this.label8 = new System.Windows.Forms.Label();
      this.SuspendLayout();
      // 
      // lblProdId
      // 
      this.lblProdId.AutoSize = true;
      this.lblProdId.Location = new System.Drawing.Point(44, 52);
      this.lblProdId.Name = "lblProdId";
      this.lblProdId.Size = new System.Drawing.Size(56, 13);
      this.lblProdId.TabIndex = 0;
      this.lblProdId.Text = "Product Id";
      // 
      // txtProdId
      // 
      this.txtProdId.Location = new System.Drawing.Point(134, 45);
      this.txtProdId.Name = "txtProdId";
      this.txtProdId.Size = new System.Drawing.Size(100, 20);
      this.txtProdId.TabIndex = 1;
      // 
      // txtProdName
      // 
      this.txtProdName.Location = new System.Drawing.Point(134, 86);
      this.txtProdName.Name = "txtProdName";
      this.txtProdName.Size = new System.Drawing.Size(100, 20);
      this.txtProdName.TabIndex = 3;
      // 
      // lblProdName
      // 
      this.lblProdName.AutoSize = true;
      this.lblProdName.Location = new System.Drawing.Point(44, 93);
      this.lblProdName.Name = "lblProdName";
      this.lblProdName.Size = new System.Drawing.Size(75, 13);
      this.lblProdName.TabIndex = 2;
      this.lblProdName.Text = "Product Name";
      // 
      // txtProdNum
      // 
      this.txtProdNum.Location = new System.Drawing.Point(134, 124);
      this.txtProdNum.Name = "txtProdNum";
      this.txtProdNum.Size = new System.Drawing.Size(100, 20);
      this.txtProdNum.TabIndex = 5;
      // 
      // lblProdNum
      // 
      this.lblProdNum.AutoSize = true;
      this.lblProdNum.Location = new System.Drawing.Point(44, 131);
      this.lblProdNum.Name = "lblProdNum";
      this.lblProdNum.Size = new System.Drawing.Size(84, 13);
      this.lblProdNum.TabIndex = 4;
      this.lblProdNum.Text = "Product Number";
      // 
      // txtStock
      // 
      this.txtStock.Location = new System.Drawing.Point(134, 168);
      this.txtStock.Name = "txtStock";
      this.txtStock.Size = new System.Drawing.Size(100, 20);
      this.txtStock.TabIndex = 7;
      // 
      // label1
      // 
      this.label1.AutoSize = true;
      this.label1.Location = new System.Drawing.Point(44, 175);
      this.label1.Name = "label1";
      this.label1.Size = new System.Drawing.Size(56, 13);
      this.label1.TabIndex = 6;
      this.label1.Text = "Stock Amt";
      // 
      // txtReorder
      // 
      this.txtReorder.Location = new System.Drawing.Point(134, 201);
      this.txtReorder.Name = "txtReorder";
      this.txtReorder.Size = new System.Drawing.Size(100, 20);
      this.txtReorder.TabIndex = 9;
      // 
      // label2
      // 
      this.label2.AutoSize = true;
      this.label2.Location = new System.Drawing.Point(44, 208);
      this.label2.Name = "label2";
      this.label2.Size = new System.Drawing.Size(66, 13);
      this.label2.TabIndex = 8;
      this.label2.Text = "Reorder Amt";
      // 
      // txtCost
      // 
      this.txtCost.Location = new System.Drawing.Point(134, 237);
      this.txtCost.Name = "txtCost";
      this.txtCost.Size = new System.Drawing.Size(100, 20);
      this.txtCost.TabIndex = 11;
      // 
      // label3
      // 
      this.label3.AutoSize = true;
      this.label3.Location = new System.Drawing.Point(44, 244);
      this.label3.Name = "label3";
      this.label3.Size = new System.Drawing.Size(28, 13);
      this.label3.TabIndex = 10;
      this.label3.Text = "Cost";
      // 
      // txtList
      // 
      this.txtList.Location = new System.Drawing.Point(134, 279);
      this.txtList.Name = "txtList";
      this.txtList.Size = new System.Drawing.Size(100, 20);
      this.txtList.TabIndex = 13;
      // 
      // label4
      // 
      this.label4.AutoSize = true;
      this.label4.Location = new System.Drawing.Point(44, 286);
      this.label4.Name = "label4";
      this.label4.Size = new System.Drawing.Size(50, 13);
      this.label4.TabIndex = 12;
      this.label4.Text = "List Price";
      // 
      // txtMfgDays
      // 
      this.txtMfgDays.Location = new System.Drawing.Point(134, 318);
      this.txtMfgDays.Name = "txtMfgDays";
      this.txtMfgDays.Size = new System.Drawing.Size(100, 20);
      this.txtMfgDays.TabIndex = 15;
      // 
      // label5
      // 
      this.label5.AutoSize = true;
      this.label5.Location = new System.Drawing.Point(44, 325);
      this.label5.Name = "label5";
      this.label5.Size = new System.Drawing.Size(64, 13);
      this.label5.TabIndex = 14;
      this.label5.Text = "Days to Mfg";
      // 
      // label6
      // 
      this.label6.AutoSize = true;
      this.label6.Location = new System.Drawing.Point(44, 360);
      this.label6.Name = "label6";
      this.label6.Size = new System.Drawing.Size(50, 13);
      this.label6.TabIndex = 16;
      this.label6.Text = "Sell Date";
      // 
      // label7
      // 
      this.label7.AutoSize = true;
      this.label7.Location = new System.Drawing.Point(44, 396);
      this.label7.Name = "label7";
      this.label7.Size = new System.Drawing.Size(89, 13);
      this.label7.TabIndex = 18;
      this.label7.Text = "Discontinue Date";
      // 
      // btnCreate
      // 
      this.btnCreate.Location = new System.Drawing.Point(319, 447);
      this.btnCreate.Name = "btnCreate";
      this.btnCreate.Size = new System.Drawing.Size(75, 23);
      this.btnCreate.TabIndex = 20;
      this.btnCreate.Text = "Put";
      this.btnCreate.UseVisualStyleBackColor = true;
      this.btnCreate.Click += new System.EventHandler(this.btnCreate_Click);
      // 
      // btnSearch
      // 
      this.btnSearch.Location = new System.Drawing.Point(238, 447);
      this.btnSearch.Name = "btnSearch";
      this.btnSearch.Size = new System.Drawing.Size(75, 23);
      this.btnSearch.TabIndex = 23;
      this.btnSearch.Text = "Get";
      this.btnSearch.UseVisualStyleBackColor = true;
      this.btnSearch.Click += new System.EventHandler(this.btnSearch_Click);
      // 
      // txtSellDate
      // 
      this.txtSellDate.Location = new System.Drawing.Point(134, 360);
      this.txtSellDate.Name = "txtSellDate";
      this.txtSellDate.Size = new System.Drawing.Size(100, 20);
      this.txtSellDate.TabIndex = 24;
      // 
      // txtDiscDate
      // 
      this.txtDiscDate.Location = new System.Drawing.Point(134, 396);
      this.txtDiscDate.Name = "txtDiscDate";
      this.txtDiscDate.Size = new System.Drawing.Size(100, 20);
      this.txtDiscDate.TabIndex = 25;
      // 
      // btnClear
      // 
      this.btnClear.Location = new System.Drawing.Point(562, 447);
      this.btnClear.Name = "btnClear";
      this.btnClear.Size = new System.Drawing.Size(75, 23);
      this.btnClear.TabIndex = 26;
      this.btnClear.Text = "Clear";
      this.btnClear.UseVisualStyleBackColor = true;
      this.btnClear.Click += new System.EventHandler(this.btnClear_Click);
      // 
      // txtEvent
      // 
      this.txtEvent.Location = new System.Drawing.Point(316, 45);
      this.txtEvent.Multiline = true;
      this.txtEvent.Name = "txtEvent";
      this.txtEvent.Size = new System.Drawing.Size(321, 364);
      this.txtEvent.TabIndex = 27;
      // 
      // btnDestroy
      // 
      this.btnDestroy.Location = new System.Drawing.Point(400, 447);
      this.btnDestroy.Name = "btnDestroy";
      this.btnDestroy.Size = new System.Drawing.Size(75, 23);
      this.btnDestroy.TabIndex = 28;
      this.btnDestroy.Text = "Destroy";
      this.btnDestroy.UseVisualStyleBackColor = true;
      this.btnDestroy.Click += new System.EventHandler(this.btnDestroy_Click);
      // 
      // btnInvalidate
      // 
      this.btnInvalidate.Location = new System.Drawing.Point(481, 447);
      this.btnInvalidate.Name = "btnInvalidate";
      this.btnInvalidate.Size = new System.Drawing.Size(75, 23);
      this.btnInvalidate.TabIndex = 29;
      this.btnInvalidate.Text = "Invalidate";
      this.btnInvalidate.UseVisualStyleBackColor = true;
      this.btnInvalidate.Click += new System.EventHandler(this.btnInvalidate_Click);
      // 
      // label8
      // 
      this.label8.AutoSize = true;
      this.label8.Location = new System.Drawing.Point(316, 26);
      this.label8.Name = "label8";
      this.label8.Size = new System.Drawing.Size(81, 13);
      this.label8.TabIndex = 30;
      this.label8.Text = "Event Message";
      // 
      // ProductBrowser
      // 
      this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
      this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
      this.ClientSize = new System.Drawing.Size(673, 501);
      this.Controls.Add(this.label8);
      this.Controls.Add(this.btnInvalidate);
      this.Controls.Add(this.btnDestroy);
      this.Controls.Add(this.txtEvent);
      this.Controls.Add(this.btnClear);
      this.Controls.Add(this.txtDiscDate);
      this.Controls.Add(this.txtSellDate);
      this.Controls.Add(this.btnSearch);
      this.Controls.Add(this.btnCreate);
      this.Controls.Add(this.label7);
      this.Controls.Add(this.label6);
      this.Controls.Add(this.txtMfgDays);
      this.Controls.Add(this.label5);
      this.Controls.Add(this.txtList);
      this.Controls.Add(this.label4);
      this.Controls.Add(this.txtCost);
      this.Controls.Add(this.label3);
      this.Controls.Add(this.txtReorder);
      this.Controls.Add(this.label2);
      this.Controls.Add(this.txtStock);
      this.Controls.Add(this.label1);
      this.Controls.Add(this.txtProdNum);
      this.Controls.Add(this.lblProdNum);
      this.Controls.Add(this.txtProdName);
      this.Controls.Add(this.lblProdName);
      this.Controls.Add(this.txtProdId);
      this.Controls.Add(this.lblProdId);
      this.Name = "ProductBrowser";
      this.Text = "Product Browser";
      this.ResumeLayout(false);
      this.PerformLayout();

    }

    #endregion

    private System.Windows.Forms.Label lblProdId;
    private System.Windows.Forms.TextBox txtProdId;
    private System.Windows.Forms.TextBox txtProdName;
    private System.Windows.Forms.Label lblProdName;
    private System.Windows.Forms.TextBox txtProdNum;
    private System.Windows.Forms.Label lblProdNum;
    private System.Windows.Forms.TextBox txtStock;
    private System.Windows.Forms.Label label1;
    private System.Windows.Forms.TextBox txtReorder;
    private System.Windows.Forms.Label label2;
    private System.Windows.Forms.TextBox txtCost;
    private System.Windows.Forms.Label label3;
    private System.Windows.Forms.TextBox txtList;
    private System.Windows.Forms.Label label4;
    private System.Windows.Forms.TextBox txtMfgDays;
    private System.Windows.Forms.Label label5;
    private System.Windows.Forms.Label label6;
    private System.Windows.Forms.Label label7;
    private System.Windows.Forms.Button btnCreate;
    private System.Windows.Forms.Button btnSearch;
    private System.Windows.Forms.TextBox txtSellDate;
    private System.Windows.Forms.TextBox txtDiscDate;
    private System.Windows.Forms.Button btnClear;
    private System.Windows.Forms.TextBox txtEvent;
    private System.Windows.Forms.Button btnDestroy;
    private System.Windows.Forms.Button btnInvalidate;
    private System.Windows.Forms.Label label8;
  }
}
