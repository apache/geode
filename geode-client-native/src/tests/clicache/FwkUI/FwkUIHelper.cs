using System;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel;
using System.Windows.Forms;
using System.Drawing;
using System.Threading;
namespace GemStone.GemFire.Cache.FwkDriver
{
 
 partial class FwkUI 
  {
    public void InitializeEvent()
    {
      rb1.Click += new EventHandler(rb1_Click);
      rb2.Click += new EventHandler(rb2_Click);
      ok1b.Click += new EventHandler(ok1b_Click);
      execute.Click += new EventHandler(execute_Click);
      LogDirb.Click += new EventHandler(LogDirb_Click);
      GfeDirb.Click += new EventHandler(GfeDirb_Click);
      ClassPathb.Click += new EventHandler(ClassPathb_Click);
      ClassPathb1.Click += new EventHandler(ClassPathb1_Click);
      GfJavab.Click += new EventHandler(GfJavab_Click);
      clearb.Click += new EventHandler(clearb_Click);
      resetb.Click += new EventHandler(resetb_Click);
      clear2b.Click += new EventHandler(clear2b_Click);
      buildb.Click += new EventHandler(buildb_Click);
      ok2b.Click += new EventHandler(okb2_Click);
      listb.Click += new EventHandler(listb_Click);
      xmlb.Click += new EventHandler(xmlb_Click);
      optionchk.ItemCheck += new ItemCheckEventHandler(optionchk_ItemCheck);
      windowschk.ItemCheck += new ItemCheckEventHandler(windowschk_ItemCheck);
      linuxchk.ItemCheck += new ItemCheckEventHandler(linuxchk_ItemCheck);
      windowsb.Click += new EventHandler(windowsb_Click);
      linuxb.Click += new EventHandler(linuxb_Click);
      clientb.Click += new EventHandler(clientb_Click);
      workingdirb.Click += new EventHandler(workingdirb_Click);
      LogDirb1.Click += new EventHandler(LogDirb1_Click);
      GfeDirb1.Click += new EventHandler(GfeDirb1_Click);
      Lin_ClassPathb.Click += new EventHandler(Lin_ClassPathb_Click);
      Lin_ClassPathb1.Click += new EventHandler(Lin_ClassPathb1_Click);
      GfJavab1.Click += new EventHandler(GfJavab1_Click);
      stopb.Click += new EventHandler(stopb_Click);
      delete1b.Click += new EventHandler(delete1b_Click);
      delete2b.Click += new EventHandler(delete2b_Click);
      delete3b.Click += new EventHandler(delete3b_Click);
    }

    public void InitializeComponent()
    {
      this.SuspendLayout();
      this.ResumeLayout(false);
      this.toolTip = new System.Windows.Forms.ToolTip();
      //Tab Pages//
      this.textOptionsTabControl = new System.Windows.Forms.TabControl();
      this.IntTabPage = new System.Windows.Forms.TabPage("Introduction");
      this.EnvTabPage = new System.Windows.Forms.TabPage("EnvSetting");
      this.TestTabPage = new System.Windows.Forms.TabPage("TestSetting");
      this.ResultTabPage = new System.Windows.Forms.TabPage("Results");

      // Group Box //
      this.CsharpBox = new System.Windows.Forms.GroupBox();
      this.MachineBox = new System.Windows.Forms.GroupBox();
      this.ListGBox = new System.Windows.Forms.GroupBox();
      this.LinuxBox = new System.Windows.Forms.GroupBox();

      //ComboBox//
      this.cb = new System.Windows.Forms.ComboBox();
      this.poolcb = new System.Windows.Forms.ComboBox();
      this.perfTest = new System.Windows.Forms.CheckBox();

      //Radio Buttons//
      this.rb1 = new System.Windows.Forms.RadioButton();
      this.rb2 = new System.Windows.Forms.RadioButton();
      this.windowsrb = new System.Windows.Forms.RadioButton();
      this.linuxrb = new System.Windows.Forms.RadioButton();

      //CheckedlistBox//
      this.optionchk = new System.Windows.Forms.CheckedListBox();
      this.windowschk = new System.Windows.Forms.CheckedListBox();
      this.linuxchk = new System.Windows.Forms.CheckedListBox();

      //ListBox//
      this.windowslbx = new System.Windows.Forms.ListBox();
      this.linuxlbx = new System.Windows.Forms.ListBox();
      this.clientlbx = new System.Windows.Forms.ListBox();
      this.classpathlbx = new System.Windows.Forms.ListBox();
      this.Lin_classpathlbx = new System.Windows.Forms.ListBox();
      this.lstbx = new System.Windows.Forms.ComboBox();
      
      //Test Box//
      this.LogDirtx = new System.Windows.Forms.TextBox();
      this.GfeDirtx = new System.Windows.Forms.TextBox();
      this.GfJavatx = new System.Windows.Forms.TextBox();
      this.ClassPathtx = new System.Windows.Forms.TextBox();
      this.ClassPathtx1 = new System.Windows.Forms.TextBox();
      this.LogDirtx1 = new System.Windows.Forms.TextBox();
      this.GfeDirtx1= new System.Windows.Forms.TextBox();
      this.GfJavatx1 = new System.Windows.Forms.TextBox();
      this.Lin_ClassPathtx1 = new System.Windows.Forms.TextBox();
      this.Lin_ClassPathtx = new System.Windows.Forms.TextBox();
      this.Cmdtx = new System.Windows.Forms.TextBox();
      this.buildtx = new System.Windows.Forms.TextBox();
      this.listtx = new System.Windows.Forms.TextBox();
      this.xmltx = new System.Windows.Forms.TextBox();
      this.windowstxt = new System.Windows.Forms.TextBox();
      this.linuxtxt = new System.Windows.Forms.TextBox();
      this.clienttxt = new System.Windows.Forms.TextBox();
      this.workingdirtx = new System.Windows.Forms.TextBox();
      this.psswdtx = new System.Windows.Forms.TextBox();
      this.richtxt = new System.Windows.Forms.RichTextBox();
      this.Resultrhtxt = new System.Windows.Forms.RichTextBox();


      // Buttons //
      this.LogDirb = new System.Windows.Forms.Button();
      this.GfeDirb = new System.Windows.Forms.Button();
      this.GfJavab = new System.Windows.Forms.Button();
      this.ClassPathb = new System.Windows.Forms.Button();
      this.ClassPathb1 = new System.Windows.Forms.Button();
      this.LogDirb1 = new System.Windows.Forms.Button();
      this.GfeDirb1 = new System.Windows.Forms.Button();
      this.GfJavab1 = new System.Windows.Forms.Button();
      this.Lin_ClassPathb1 = new System.Windows.Forms.Button();
      this.Lin_ClassPathb = new System.Windows.Forms.Button();
      this.execute = new System.Windows.Forms.Button();
      this.clearb = new System.Windows.Forms.Button();
      this.clear2b = new System.Windows.Forms.Button();
      this.ok1b = new System.Windows.Forms.Button();
      this.ok2b = new System.Windows.Forms.Button();
      this.buildb = new System.Windows.Forms.Button();
      this.listb = new System.Windows.Forms.Button();
      this.xmlb = new System.Windows.Forms.Button();
      this.ShowLb = new System.Windows.Forms.Label();
      this.windowsb = new System.Windows.Forms.Button();
      this.linuxb = new System.Windows.Forms.Button();
      this.clientb = new System.Windows.Forms.Button();
      this.delete1b = new System.Windows.Forms.Button();
      this.delete2b = new System.Windows.Forms.Button();
      this.delete3b = new System.Windows.Forms.Button();
      this.resetb = new System.Windows.Forms.Button();
      this.workingdirb = new System.Windows.Forms.Button();
      this.stopb = new System.Windows.Forms.Button();

      // Labels //
      this.LogDirLb = new System.Windows.Forms.Label();
      this.GfeDirLb = new System.Windows.Forms.Label();
      this.GfJavaLb = new System.Windows.Forms.Label();
      this.ClassPathLb = new System.Windows.Forms.Label();
      this.LogDirLb1= new System.Windows.Forms.Label();
      this.GfeDirLb1 = new System.Windows.Forms.Label();
      this.GfJavaLb1 = new System.Windows.Forms.Label();
      this.Lin_ClassPathLb = new System.Windows.Forms.Label();
      this.GfcppP = new System.Windows.Forms.Label();
      this.LoglevelLb = new System.Windows.Forms.Label();
      this.CmdLb = new System.Windows.Forms.Label();
      this.buildLb = new System.Windows.Forms.Label();
      this.optionLb = new System.Windows.Forms.Label();
      this.poolLb = new System.Windows.Forms.Label();
      this.listLb = new System.Windows.Forms.Label();
      this.xmlLb = new System.Windows.Forms.Label();
      this.windowsLb = new System.Windows.Forms.Label();
      this.linuxLb = new System.Windows.Forms.Label();
      this.clientLb = new System.Windows.Forms.Label();
      this.workingdirLb = new System.Windows.Forms.Label();
      this.psswdLb = new System.Windows.Forms.Label();
      this.picBox = new PictureBox();
      this.fileD = new System.Windows.Forms.OpenFileDialog();
      this.folderBr = new System.Windows.Forms.FolderBrowserDialog();
      this.winlb = new Label();
      this.linlb = new Label();

      //Adding Tab controls//
      this.Controls.Add(this.textOptionsTabControl);
      this.textOptionsTabControl.Controls.Add(this.IntTabPage);
      this.textOptionsTabControl.Controls.Add(this.EnvTabPage);
      this.textOptionsTabControl.Controls.Add(this.TestTabPage);
      this.textOptionsTabControl.Controls.Add(this.ResultTabPage);

      // Adding Controls to EnvTabPage //
      this.rb1.Text = "CSharp Testing";
      this.rb1.AutoSize = true;
      this.rb1.SetBounds(10, 50, 10, 10);
      rb1.Name = "rb1";
      this.EnvTabPage.Controls.Add(rb1);
      this.rb2.Text = "GFCPP Testing";
      this.rb2.AutoSize = true;
      this.rb2.SetBounds(200, 50, 10, 10);
      rb2.Enabled = false;
      rb2.Name = "rb2";
      this.EnvTabPage.Controls.Add(rb2);

      this.CsharpBox.Text = "gfcsharp.env/gfcpp.properties";
      this.CsharpBox.SetBounds(10, 100, 570, 600);
      CsharpBox.Name = "csharpbx";
      this.EnvTabPage.Controls.Add(CsharpBox);

      this.winlb.Text = "Windows Setting";
      this.winlb.SetBounds(10,40,10,10);
      this.winlb.AutoSize = true;
      this.CsharpBox.Controls.Add(winlb);

      this.LogDirLb.Text = "Log Dir";
      this.LogDirLb.SetBounds(10, 80, 10, 10);
      this.LogDirLb.AutoSize = true;
      this.CsharpBox.Controls.Add(LogDirLb);
      this.LogDirb.Text = "Browse";
      this.LogDirb.SetBounds(70, 80, 10, 10);
      this.LogDirb.AutoSize = true;
      this.LogDirb.Enabled = false;
      this.CsharpBox.Controls.Add(LogDirb);
      SetControlToolTips(LogDirb, "Select directory for storing the logs");
      this.LogDirtx.SetBounds(130, 80, 150, 10);
      LogDirtx.Name = "logdirtx";
      this.LogDirtx.Enabled = false;
      this.CsharpBox.Controls.Add(LogDirtx);
      this.GfeDirLb.Text = "GFE_DIR";
      this.GfeDirLb.AutoSize = true;
      this.GfeDirLb.SetBounds(10, 120, 10, 10);
      this.CsharpBox.Controls.Add(GfeDirLb);
      this.GfeDirb.Text = "Browse";
      this.GfeDirb.Enabled = false;
      this.GfeDirb.AutoSize = true;
      this.GfeDirb.SetBounds(70, 120, 10, 10);
      this.CsharpBox.Controls.Add(GfeDirb);
      this.GfeDirtx.SetBounds(130, 120, 150, 10);
      this.GfeDirtx.Enabled = false;
      GfeDirtx.Name = "GfeDirtx";
      this.CsharpBox.Controls.Add(GfeDirtx);

      this.ClassPathLb.Text = "ClassPath";
      this.ClassPathLb.AutoSize = true;
      this.ClassPathLb.SetBounds(10,160,10,10);
      this.CsharpBox.Controls.Add(ClassPathLb);
      this.ClassPathb.Text = "Browse";
      this.ClassPathb.AutoSize = true;
      this.ClassPathb.Enabled = false;
      this.ClassPathb.SetBounds(70, 160, 10, 10);
      SetControlToolTips(ClassPathb, "Select the class path and then click Add to set the path");
      this.CsharpBox.Controls.Add(ClassPathb);
      ClassPathtx.Name = "ClassPathtx";
      this.ClassPathtx.SetBounds(130, 160, 150, 10);
      this.ClassPathtx.Enabled = false;
      this.CsharpBox.Controls.Add(ClassPathtx);

      this.ClassPathb1.Text = "Add";
      this.ClassPathb1.AutoSize = true;
      this.ClassPathb1.Enabled = false;
      this.ClassPathb1.SetBounds(290, 160, 10, 10);
      SetControlToolTips(ClassPathb1, "Click Add to set the Classpath,after browsing the path");
      this.CsharpBox.Controls.Add(ClassPathb1);
      this.classpathlbx.SetBounds(350, 160, 200, 50);
      this.classpathlbx.Enabled = false;
      this.CsharpBox.Controls.Add(classpathlbx);

      this.workingdirLb.Text = "WorkingDir";
      this.workingdirLb.AutoSize = true;
      this.workingdirLb.SetBounds(290, 80, 10, 10);
      this.CsharpBox.Controls.Add(workingdirLb);
      this.workingdirb.Text = "Browse";
      this.workingdirb.AutoSize = true;
      this.workingdirb.Enabled = false;
      this.workingdirb.SetBounds(350, 80, 10, 10);
      SetControlToolTips(workingdirb, "Select a working directory");
      this.CsharpBox.Controls.Add(workingdirb);
      this.workingdirtx.SetBounds(410, 80, 150, 10);
      workingdirtx.Name = "workingdirtx";
      this.workingdirtx.Enabled = false;
      this.CsharpBox.Controls.Add(workingdirtx);

      this.GfJavaLb.Text = "GF_JAVA";
      this.GfJavaLb.AutoSize = true;
      this.GfJavaLb.SetBounds(290, 120, 10, 10);
      this.CsharpBox.Controls.Add(GfJavaLb);
      this.GfJavab.Text = "Browse";
      this.GfJavab.AutoSize = true;
      this.GfJavab.Enabled = false;
      this.GfJavab.SetBounds(350, 120, 10, 10);
      this.CsharpBox.Controls.Add(GfJavab);
      this.GfJavatx.SetBounds(410, 120, 150, 10);
      GfJavatx.Name = "GfJavatx";
      this.GfJavatx.Enabled = false;
      this.CsharpBox.Controls.Add(GfJavatx);

      this.linlb.Text = "Linux Setting";
      this.linlb.SetBounds(10, 220, 10, 10);
      this.linlb.AutoSize = true;
      this.CsharpBox.Controls.Add(linlb);

      this.LogDirLb1.Text = "LogDir";
      this.LogDirLb1.SetBounds(10, 260, 10, 10);
      this.LogDirLb1.AutoSize = true;
      this.CsharpBox.Controls.Add(LogDirLb1);
      this.LogDirb1.Text = "Browse";
      this.LogDirb1.SetBounds(70, 260, 10, 10);
      this.LogDirb1.AutoSize = true;
      this.LogDirb1.Enabled = false;
      this.CsharpBox.Controls.Add(LogDirb1);
      SetControlToolTips(LogDirb1, "Select directory for storing the logs");
      this.LogDirtx1.SetBounds(130, 260, 150, 10);
      this.LogDirtx1.Enabled = false;
      LogDirtx1.Name = "Log_lin";
      this.CsharpBox.Controls.Add(LogDirtx1);

      this.GfeDirLb1.Text = "GFE_DIR";
      this.GfeDirLb1.AutoSize = true;
      this.GfeDirLb1.SetBounds(10, 300, 10, 10);
      this.CsharpBox.Controls.Add(GfeDirLb1);
      this.GfeDirb1.Text = "Browse";
      this.GfeDirb1.Enabled = false;
      this.GfeDirb1.AutoSize = true;
      this.GfeDirb1.SetBounds(70, 300, 10, 10);
      this.CsharpBox.Controls.Add(GfeDirb1);
      this.GfeDirtx1.SetBounds(130, 300, 150, 10);
      this.GfeDirtx1.Enabled = false;
      GfeDirtx1.Name = "Gfe_Lin";
      this.CsharpBox.Controls.Add(GfeDirtx1);

      this.GfJavaLb1.Text = "GF_JAVA";
      this.GfJavaLb1.AutoSize = true;
      this.GfJavaLb1.SetBounds(10, 340, 10, 10);
      this.CsharpBox.Controls.Add(GfJavaLb1);
      this.GfJavab1.Text = "Browse";
      this.GfJavab1.AutoSize = true;
      this.GfJavab1.Enabled = false;
      this.GfJavab1.SetBounds(70, 340, 10, 10);
      this.CsharpBox.Controls.Add(GfJavab1);
      this.GfJavatx1.SetBounds(130, 340, 150, 10);
      this.GfJavatx1.Enabled = false;
      GfJavatx1.Name = "gfJava_lin";
      this.CsharpBox.Controls.Add(GfJavatx1);

      this.Lin_ClassPathLb.Text = "ClassPath";
      this.Lin_ClassPathLb.AutoSize = true;
      this.Lin_ClassPathLb.SetBounds(10, 380, 10, 10);
      this.CsharpBox.Controls.Add(Lin_ClassPathLb);
      this.Lin_ClassPathb.Text = "Browse";
      this.Lin_ClassPathb.AutoSize = true;
      this.Lin_ClassPathb.Enabled = false;
      this.Lin_ClassPathb.SetBounds(70, 380, 10, 10);
      SetControlToolTips(Lin_ClassPathb, "Select the class path and then click Add to set the path");
      this.CsharpBox.Controls.Add(Lin_ClassPathb);
      this.Lin_ClassPathtx.SetBounds(130, 380, 150, 10);
      this.Lin_ClassPathtx.Enabled = false;
      Lin_ClassPathtx.Name = "ClassPath_Lin";
      this.CsharpBox.Controls.Add(Lin_ClassPathtx);

      this.Lin_ClassPathb1.Text = "Add";
      this.Lin_ClassPathb1.AutoSize = true;
      this.Lin_ClassPathb1.Enabled = false;
      this.Lin_ClassPathb1.SetBounds(290, 380, 10, 10);
      SetControlToolTips(Lin_ClassPathb1, "Click Add to set the Classpath,after browsing the path");
      this.CsharpBox.Controls.Add(Lin_ClassPathb1);
      this.Lin_classpathlbx.SetBounds(350, 380, 200, 50);
      this.Lin_classpathlbx.Enabled = false;
      this.CsharpBox.Controls.Add(Lin_classpathlbx);

      this.perfTest.Text = "PerfTest";
      this.perfTest.AutoSize = true;
      this.perfTest.SetBounds(10, 420, 10, 10);
      this.CsharpBox.Controls.Add(perfTest);

      this.ok1b.Text = "OK";
      this.ok1b.SetBounds(190, 460, 30, 10);
      this.ok1b.AutoSize = true;
      this.CsharpBox.Controls.Add(ok1b);

      this.clearb.Text = "Reset";
      this.clearb.SetBounds(250, 460, 10, 10);
      this.clearb.AutoSize = true;
      this.CsharpBox.Controls.Add(clearb);
   
      this.LoglevelLb.Text = "log_level";
      this.LoglevelLb.AutoSize = true;
      this.LoglevelLb.SetBounds(290, 260, 500, 200);
      this.CsharpBox.Controls.Add(LoglevelLb);
      this.cb.SetBounds(350, 260, 100, 200);
      this.cb.Text = "Select log-level";
      this.cb.Items.Add("info");
      this.cb.Items.Add("config");
      this.cb.Items.Add("fine");
      this.cb.Items.Add("debug");
      this.cb.Enabled = false;
      this.CsharpBox.Controls.Add(cb);

      this.ShowLb.Text = "Your gfcsharp.env file is";
      this.ShowLb.SetBounds(600, 100, 500, 200);
      this.ShowLb.AutoSize = true;
      this.EnvTabPage.Controls.Add(ShowLb);
      this.richtxt.SetBounds(600, 150, 500, 250);
      this.richtxt.WordWrap = false;
      this.richtxt.ScrollBars = RichTextBoxScrollBars.Horizontal;
      this.EnvTabPage.Controls.Add(richtxt);

      // Adding Controls to TestTabPage //
      this.MachineBox.Text = "Select Machines";
      this.MachineBox.SetBounds(10, 10, 800, 250);
      this.MachineBox.AutoSize = true;
      this.TestTabPage.Controls.Add(MachineBox);
      this.windowsLb.Text = "Add Windows machines for servers";
      this.windowsLb.SetBounds(30, 30, 30, 10);
      this.windowsLb.AutoSize = true;
      this.MachineBox.Controls.Add(windowsLb);
      this.windowstxt.SetBounds(30, 60, 50, 150);
      this.MachineBox.Controls.Add(windowstxt);
      this.windowsb.Text = "Add";
      this.windowsb.SetBounds(30,90,10,10);
      this.windowsb.AutoSize = true;
      this.windowsb.Enabled = false;
      this.MachineBox.Controls.Add(windowsb);
      this.delete1b.Text = "Remove";
      this.delete1b.SetBounds(30, 120, 10, 10);
      this.delete1b.AutoSize = true;
      this.delete1b.Enabled = false;
      this.MachineBox.Controls.Add(delete1b);
      this.windowslbx.SetBounds(90, 90, 80, 140);
      this.MachineBox.Controls.Add(windowslbx);

      this.clientLb.Text = "Add client (windows) machines:";
      this.clientLb.SetBounds(300,30,50,10);
      this.clientLb.AutoSize = true;
      this.MachineBox.Controls.Add(clientLb);
      this.clienttxt.SetBounds(300,60,50,150);
      this.MachineBox.Controls.Add(clienttxt);
      this.clientb.Text = "Add";
      this.clientb.SetBounds(300,90,10,10);
      this.clientb.AutoSize = true;
      this.MachineBox.Controls.Add(clientb);
      this.delete2b.Text = "Remove";
      this.delete2b.SetBounds(300, 120, 10, 10);
      this.delete2b.AutoSize = true;
      this.MachineBox.Controls.Add(delete2b);
      this.clientlbx.SetBounds(365,90,80,140);
      this.MachineBox.Controls.Add(clientlbx);

      this.linuxLb.Text = "Add Linux machines for servers";
      this.linuxLb.SetBounds(520, 30, 30, 10);
      this.linuxLb.AutoSize = true;
      this.MachineBox.Controls.Add(linuxLb);
      this.linuxtxt.SetBounds(520, 60, 50, 150);
      this.MachineBox.Controls.Add(linuxtxt);
      this.linuxb.Text = "Add";
      this.linuxb.SetBounds(520,90,10,10);
      this.linuxb.AutoSize = true;
      this.linuxb.Enabled = false;
      this.MachineBox.Controls.Add(linuxb);
      this.delete3b.Text = "Remove";
      this.delete3b.SetBounds(520, 120, 10, 10);
      this.delete3b.AutoSize = true;
      this.delete3b.Enabled = false;
      this.MachineBox.Controls.Add(delete3b);
      this.linuxlbx.SetBounds(585, 90, 80, 140);
      this.MachineBox.Controls.Add(linuxlbx);

      this.ListGBox.Text = "Select your test options";
      this.ListGBox.SetBounds(10, 300, 800, 200);
      this.ListGBox.AutoSize = true;
      this.TestTabPage.Controls.Add(ListGBox);
      this.buildLb.Text = "BuildDir";
      this.buildLb.SetBounds(30, 30, 10, 10);
      this.buildLb.AutoSize = true;
      this.ListGBox.Controls.Add(buildLb);
      this.buildb.Text = "Browse";
      this.buildb.SetBounds(75, 30, 10, 10);
      this.buildb.AutoSize = true;
      this.ListGBox.Controls.Add(buildb);
      this.buildtx.Name = "buildtx";
      this.buildtx.SetBounds(140, 30, 200, 10);
      this.ListGBox.Controls.Add(xmltx);
      this.optionLb.Text = "Choose your options:";
      this.optionLb.SetBounds(30, 90, 10, 10);
      this.optionLb.AutoSize = true;
      this.ListGBox.Controls.Add(optionLb);
      this.optionchk.SetBounds(30, 120, 150, 100);
      this.ListGBox.Controls.Add(optionchk); 
      this.optionchk.Items.AddRange(new object[]
     {
       "--xml","--list","--pool","--database","--at","--auto-ssh","--bbServer","--driverPort"
     });
      this.listLb.Text = "List :";
      this.listLb.AutoSize = true;
      this.listLb.SetBounds(30, 270, 10, 10);
      this.ListGBox.Controls.Add(listLb);
      this.listb.Text = "Browse";
      this.listb.SetBounds(60, 270, 10, 20);
      this.listb.AutoSize = true;
      this.listb.Enabled = false;
      this.ListGBox.Controls.Add(listb);
      this.listtx.Name = "listtx";
      this.listtx.SetBounds(120, 270, 150, 20);
      this.listtx.Enabled = false;
      this.ListGBox.Controls.Add(listtx);
      this.xmlLb.Text = "xml :";
      this.xmlLb.SetBounds(310, 270, 10, 10);
      this.xmlLb.AutoSize = true;
      this.ListGBox.Controls.Add(xmlLb);
      this.xmlb.Text = "Browse";
      this.xmlb.SetBounds(340, 270, 10, 20);
      this.xmlb.AutoSize = true;
      this.xmlb.Enabled = false;
      this.ListGBox.Controls.Add(xmlb);
      this.xmltx.Name = "xmltx";
      this.xmltx.SetBounds(400, 270, 150, 20);
      this.xmltx.Enabled = false;
      this.ListGBox.Controls.Add(buildtx);
      this.poolLb.Text = "poolOption:";
      this.poolLb.SetBounds(600, 270, 10, 20);
      this.poolLb.AutoSize = true;
      this.ListGBox.Controls.Add(poolLb);
      this.poolcb.SetBounds(665, 270, 100, 200);
      this.poolcb.Items.Add("poolwithendpoints");
      this.poolcb.Items.Add("poolwithlocator");
      this.poolcb.Enabled = false;
      this.ListGBox.Controls.Add(poolcb);

      this.ok2b.Text = "OK";
      this.ok2b.SetBounds(420, 300, 20, 10);
      this.ok2b.AutoSize = true;
      this.ListGBox.Controls.Add(ok2b);
      this.CmdLb.Text = "Final Command Line :";
      this.CmdLb.SetBounds(30, 350, 20, 10);
      this.CmdLb.AutoSize = true;
      this.ListGBox.Controls.Add(CmdLb);
      this.Cmdtx.SetBounds(150, 350, 300, 10);
      this.ListGBox.Controls.Add(Cmdtx);

      this.execute.Text = "Execute";
      this.execute.AutoSize = true;
      this.execute.SetBounds(360, 380, 20, 10);
      this.ListGBox.Controls.Add(execute);
      this.clear2b.Text = "Clear";
      this.clear2b.AutoSize = true;
      this.clear2b.SetBounds(470, 380, 20, 10);
      this.ListGBox.Controls.Add(clear2b);

      this.stopb.Text = "STOP";
      this.stopb.SetBounds(520, 380, 20, 10);
      this.stopb.AutoSize = true;
      SetControlToolTips(stopb,"Terminates,execution of the process");
      this.ListGBox.Controls.Add(stopb);

      this.SizeChanged += new EventHandler(MainForm_SizeChanged);

      //Add controls to Result Page:
      this.Resultrhtxt.SetBounds(30,10,1000,700);
      this.ResultTabPage.Controls.Add(Resultrhtxt);
    }

    #region Declaration for the windows form controls

    public TabControl textOptionsTabControl;
    public TabPage IntTabPage;
    public TabPage EnvTabPage;
    public TabPage TestTabPage;
    public TabPage ResultTabPage;
    public GroupBox CsharpBox;
    public GroupBox LinuxBox;
    public GroupBox MachineBox;
    public GroupBox ListGBox;
    public Button LogDirb;
    public Button GfeDirb;
    public Button ClassPathb;
    public Button ClassPathb1;
    public Button GfJavab1;
    public Button LogDirb1;
    public Button GfeDirb1;
    public Button Lin_ClassPathb;
    public Button Lin_ClassPathb1;
    public Button GfJavab;
    public Button execute;
    public Button clearb;
    public Button ok1b;
    public Button ok2b;
    public Button listb;
    public Button xmlb;
    public Button clear2b;
    public Button buildb;
    public Button windowsb;
    public Button linuxb;
    public Button clientb;
    public Button delete1b;
    public Button delete2b;
    public Button delete3b;
    public Button resetb;
    public Button stopb;
    public Button workingdirb;
    public Label ShowLb;
    public Label LogDirLb;
    public Label GfeDirLb;
    public Label ClassPathLb;
    public Label GfJavaLb;
    public Label LogDirLb1;
    public Label GfeDirLb1;
    public Label Lin_ClassPathLb;
    public Label GfJavaLb1;
    public Label GfcppP;
    public Label LoglevelLb;
    public Label CmdLb;
    public Label buildLb;
    public Label optionLb;
    public Label poolLb;
    public Label listLb;
    public Label xmlLb;
    public Label windowsLb;
    public Label linuxLb;
    public Label clientLb;
    public Label workingdirLb;
    public Label psswdLb;
    public Label winlb;
    public Label linlb;
    public ComboBox cb;
    public ComboBox poolcb;
    public RadioButton rb1;
    public RadioButton rb2;
    public RadioButton windowsrb;
    public RadioButton linuxrb;
    public OpenFileDialog fileD;
    public TextBox LogDirtx;
    public TextBox GfeDirtx;
    public TextBox GfJavatx;
    public TextBox ClassPathtx;
    public TextBox LogDirtx1;
    public TextBox GfeDirtx1;
    public TextBox GfJavatx1;
    public TextBox ClassPathtx1;
    public TextBox Lin_ClassPathtx;
    public TextBox Lin_ClassPathtx1;
    public TextBox Cmdtx;
    public TextBox buildtx;
    public TextBox listtx;
    public TextBox xmltx;
    public TextBox windowstxt;
    public TextBox linuxtxt;
    public TextBox clienttxt;
    public TextBox workingdirtx;
    public TextBox psswdtx;
    public RichTextBox richtxt;
    public RichTextBox Resultrhtxt;
    public CheckedListBox optionchk;
    public CheckedListBox windowschk;
    public CheckedListBox linuxchk;
    public ListBox windowslbx;
    public ListBox linuxlbx;
    public ListBox clientlbx;
    public ListBox classpathlbx;
    public ListBox Lin_classpathlbx;
    public ComboBox lstbx;
    public CheckBox perfTest;
    public FolderBrowserDialog folderBr;
    public ToolTip toolTip;
    private PictureBox picBox;
    //private static Thread[] thread=new Thread[10];
    private static Thread thread ;
    //private static int count;
   #endregion
 }

}