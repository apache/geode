using System;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel;
using System.Windows.Forms;
using System.IO;
using System.Diagnostics;
using System.Security;
using System.Web;
using System.Drawing;
using System.Threading;
using System.Collections.ObjectModel;

namespace GemStone.GemFire.Cache.FwkDriver
{
  public partial class FwkUI : Form
  {
    public FwkUI()
    {
      InitializeComponent();
      InitializeEvent();
      this.SetBounds(100,100,1300,1300);
      this.CenterToScreen();
      this.TopMost = true;
      getTextData();
    }

    [STAThread]
    public static void Main()
    {
      Application.EnableVisualStyles();
      Application.Run(new FwkUI());
    }

    public void SetControlToolTips(Control ctrl,string txt)
    {
      toolTip.ShowAlways = true;
      toolTip.IsBalloon = true;
      toolTip.SetToolTip(ctrl,txt);
    }

    public void setTextData()
    {
      if (!Directory.Exists("C:\\FwkTxtData"))
        Directory.CreateDirectory("C:\\FwkTxtData");
      //string wrkpath = @"C:\FwkTxtData\" + "wrkpath.txt";
      string bpath = @"C:\FwkTxtData\" + "bpath.txt";
      //string gpath = @"C:\FwkTxtData\" + "gfcsharp.env"; ;
      string mylist = listtx.Text.ToString();
      string myxml = xmltx.Text.ToString();
      string mybuild = buildtx.Text.ToString();
      string mywrkdir = workingdirtx.Text.ToString();
      string mycmd = Cmdtx.Text.ToString();
      StreamWriter sw = null;
      sw = new StreamWriter(bpath);
      sw.WriteLine(myxml);
      sw.WriteLine(mylist);
      sw.WriteLine(mybuild);
      sw.WriteLine(mycmd);
      sw.Close();
    }

    public void getTextData()
    {
      //string fpath = @"C:\" + "suppi.txt";
      //char[] c = new char[] { '=' };
      this.ShowLb.Text = "Your Previous gfcsharp.evn file is";
      ShowLb.Font = new Font(ShowLb.Font.FontFamily, ShowLb.Font.Size + 5);
      this.richtxt.BackColor = Color.Black ;
      string dirpath = "C:\\FwkTxtData";
      string basicPath = @"C:\FwkTxtData\";
      if (Directory.Exists(dirpath))
      {
        string gfpath = basicPath + "abc.env";
        string bpath = basicPath + "bpath.txt";
        string wrkpath = basicPath + "wrkpath.txt";
        string fpath = basicPath + "gfcsharp.env";
        if (File.Exists(gfpath))
        {
          //string fpath = @"C:\FwkTxtData\" + "gfcsharp.env".Replace("/", "\\");
          if (File.Exists(fpath))
          {
            fpath = fpath.Replace("/", "\\");
            StreamReader sr = new StreamReader(fpath);
            richtxt.Text = sr.ReadToEnd();
            richtxt.ForeColor = Color.White;
            richtxt.Font = new Font(richtxt.Font.FontFamily, richtxt.Font.Size + 3);
            sr.Close();
          }
        }
        if (File.Exists(wrkpath))
        {
          StreamReader sr2 = new StreamReader(wrkpath);
          workingdirtx.Text = sr2.ReadLine();
          sr2.Close();
        }
        //string[] strarr ={ "buildtx", "listtx", "xmltx" };
        //StreamReader sr3 = new StreamReader(@"C:\" + "bpath.txt");
        //buildtx.Text = sr3.ReadLine();
        //sr3.Close();
        //Console.WriteLine(fpath);
        //if (File.Exists(fpath))
        //{
        //StreamReader sr3 = new StreamReader(@"C:\" + "bpath.txt");
        //while (!sr3.EndOfStream)
        //{
        //string a = sr3.ReadLine();
        //int i = a.IndexOf('=');
        //string s = a.Substring(i).Remove(0, 2);
        if(File.Exists(bpath))
        {
          StreamReader sr3 = new StreamReader(bpath);
          while (!sr3.EndOfStream)
          {
            foreach (Control ctrl in ListGBox.Controls)
            {
              if (ctrl.GetType() == typeof(TextBox))
              {
                //Console.WriteLine("{0}",ctrl.Name);
                string a = sr3.ReadLine();
                //Console.WriteLine("{0}={1}", ctrl.GetType(), a);
                ctrl.Text = a;
              }
            }
          }
          sr3.Close();
        }
        if (File.Exists(gfpath))
        {
          StreamReader sr4 = new StreamReader(gfpath);
          while (!sr4.EndOfStream)
          {
            foreach (Control ctrl in CsharpBox.Controls)
            {
              if (ctrl.GetType() == typeof(TextBox))
              {
                string a = sr4.ReadLine();
                Console.WriteLine("{0}={1}", ctrl.Name, a);
                ctrl.Text = a;
              }
            }
          }
          sr4.Close();
        }

      }
      else
        Console.Write("No such dir {0}",dirpath);
    }

    public void ClassPathb1_Click(object sender, EventArgs e)
    {
      classpathlbx.Items.Add(ClassPathtx.Text.ToString());
      ClassPathtx.Clear();
    }

    public void Lin_ClassPathb1_Click(object sender, EventArgs e)
    {
      Lin_classpathlbx.Items.Add(Lin_ClassPathtx.Text.ToString());
      Lin_ClassPathtx.Clear();
    }

    public string FileBrowser(TextBox txt,Boolean trim,int flag)
    {
      if ((flag == 0) && (trim==false))
      {
        folderBr.Description = "Select a folder/Directory";
      
          if (folderBr.ShowDialog() == DialogResult.OK)
          {
            txt.Text = folderBr.SelectedPath.Replace("\\", "/");
          }
          return txt.Text.ToString();
      }
      else
      { 
        fileD.Title = "Select a file";
        fileD.InitialDirectory = @"c:\";
        fileD.FilterIndex = 2;
        fileD.RestoreDirectory = true;
        fileD.Multiselect = true;
        if (fileD.ShowDialog() == DialogResult.OK)
        {
          if (!trim)
          {
            string filepath = fileD.FileName.Replace("\\", "/");
            txt.Text = filepath;
            //Console.Write(txt.Controls.GetType().Name);
          }
          else
          {
            if (xmltx.Enabled)
            {
             // Console.WriteLine("Inside xmltxt");
              string filepath =fileD.FileName;
              int i = filepath.IndexOf('x');
              string s = filepath.Substring(i).Remove(0, 4).Replace("\\", "/");
              //Console.WriteLine("the xml path is {0}",s);
              txt.Text = s;
            }
            else
            {
             // Console.WriteLine("Inside listtxt");
              string listname = "";
              char[] character = new char[] { ',' };
              foreach (String file in fileD.FileNames)
              {
                string filepath = file.Substring(fileD.FileName.LastIndexOf("\\") + 1);
                if (!File.Exists(workingdirtx.Text.ToString() + "\\" + filepath))
                  File.Copy(file,workingdirtx.Text.ToString()+"\\"+filepath);
                //Console.WriteLine("trimed file name is  {0}", filepath);
                listname = listname + filepath + ",";
              }
              txt.Text = listname.TrimEnd(character);
            }
          }
        }
      }
      return txt.Text.ToString();
    }

    //Code to resize the tab panes,when resizing the form.
    public void MainForm_SizeChanged(object sender, EventArgs e)
    {
      this.textOptionsTabControl.Width = this.Width;
      this.textOptionsTabControl.Height = this.Height;
    }
  
    public void rb2_Click(object sender, EventArgs e)
    {
      if (rb2.Checked)
      {
        //Console.WriteLine("GFCPP clicked");
        ClassPathb.Enabled = false;
        ClassPathtx.Enabled = false;
        LogDirb.Enabled = false;
        LogDirtx.Enabled = false;
        cb.Enabled = true;
        GfeDirb.Enabled = true;
        GfeDirtx.Enabled = true;
        GfJavab.Enabled = true;
        GfJavatx.Enabled = true;
      }
    }
    public void rb1_Click(object sender, EventArgs e)
    {
      if (rb1.Checked)
      {
        //Console.WriteLine("Csharp checked");
        cb.Enabled = true;
        GfeDirb.Enabled = true;
        GfeDirtx.Enabled = true;
        GfJavab.Enabled = true;
        GfJavatx.Enabled = true;
        ClassPathb.Enabled = true;
        ClassPathtx.Enabled = true;
        ClassPathb1.Enabled = true;
        ClassPathtx1.Enabled = true;
        classpathlbx.Enabled = true;
        LogDirb.Enabled = true;
        LogDirtx.Enabled = true;
        workingdirb.Enabled = true;
        workingdirtx.Enabled = true;
        DialogResult result=MessageBox.Show("DO you want to run servers on windows? if no then the servers will run on Linux:","",MessageBoxButtons.YesNo,MessageBoxIcon.Question);
        if (result == DialogResult.Yes)
        {
          windowsb.Enabled = true;
          delete1b.Enabled = true;
          linuxb.Enabled = false;
          linuxlbx.Items.Clear();
          GfeDirb1.Enabled = false;
          GfeDirtx1.Enabled = false;
          GfJavab1.Enabled = false;
          GfJavatx1.Enabled = false;
          Lin_ClassPathb.Enabled = false;
          Lin_ClassPathtx.Enabled = false;
          Lin_ClassPathb1.Enabled = false;
          Lin_ClassPathtx1.Enabled = false;
          Lin_classpathlbx.Enabled = false;
          LogDirb1.Enabled = false;
          LogDirtx1.Enabled = false;
          delete3b.Enabled = false;
        }
        else
        {
          linuxb.Enabled = true;
          windowsb.Enabled = false;
          windowslbx.Items.Clear();
          GfeDirb1.Enabled = true;
          GfeDirtx1.Enabled = true;
          GfJavab1.Enabled = true;
          GfJavatx1.Enabled = true;
          Lin_ClassPathb.Enabled = true;
          Lin_ClassPathtx.Enabled = true;
          Lin_ClassPathb1.Enabled = true;
          Lin_ClassPathtx1.Enabled = true;
          Lin_classpathlbx.Enabled = true;
          LogDirb1.Enabled = true;
          LogDirtx1.Enabled = true;
          delete3b.Enabled = true;
        }
      }
    }
    
    //Code for executing the command on the cmd propmt.
    public void execute_Click(object sender, EventArgs e)
    {
      //count++;
      //Console.WriteLine("COUNT={0}", count);
      //thread[count] = new Thread(new ThreadStart(this.provoke));
      //Console.WriteLine("Started Thread {0}",thread[count].Name);
      //thread[count].Start();
      thread = new Thread(new ThreadStart(this.provoke));
      thread.Start();

    }

    public void provoke()
    {
      //lock (this)
      //{
        string passwd = string.Empty;
        string pathExe = buildtx.Text.ToString() + "/framework/csharp/bin/FwkDriver.exe";
        //Console.WriteLine("The exe path is {0}", pathExe);
        Console.Write("Please Enter The Password: ");
        Process start = new Process();
        start.StartInfo.WorkingDirectory = workingdirtx.Text.ToString();
        start.StartInfo.FileName = @"" + pathExe;
        start.StartInfo.Arguments = Cmdtx.Text.ToString();
        start.StartInfo.UseShellExecute = false;
        start.StartInfo.RedirectStandardOutput = true;
        start.Start();
     // }    
        //Console.WriteLine("The process name is {0}",start.ProcessName);
        string output = start.StandardOutput.ReadToEnd();
        Console.WriteLine(output);
        
    }

    public void okb2_Click(object sender, EventArgs e)
    {
      string hostname = System.Environment.MachineName.ToString().ToLower();
      string servercmd="CS:" + hostname;
      string clientcmd = "";
      if (linuxb.Enabled)
      {
        foreach (string s in linuxlbx.Items)
        {
          servercmd = servercmd + " " + "LIN:CS:" + s.ToString();
        }
      }
      else
      {
        foreach (string s in windowslbx.Items)
        {
          servercmd = servercmd + " " + "CS:" + s.ToString();
          //Console.WriteLine("Server string is {0}", servercmd);
        }
      }
      foreach (string c in clientlbx.Items)
      {
        clientcmd = clientcmd + " " + c.ToString();
      }
      string list = listtx.Text.ToString();
      string[] words = list.Split(',');
      string list1="";
      string options = "";
      foreach (string word in words)
        list1=list1 + " " + "--list=" + word.ToString();
      bool pool=true;
      if (poolcb.SelectedIndex > -1)
        pool = true;
      else
      {
        MessageBox.Show("Please select a pooloption","WARNING",MessageBoxButtons.OK,MessageBoxIcon.Warning);
        pool = false;
      }
      if (optionchk.CheckedItems.Count != 0)
      {
        for (int x = 0; x <= optionchk.CheckedItems.Count - 1; x++)
        {
          if (optionchk.CheckedItems[x].ToString() == "--list" || optionchk.CheckedItems[x].ToString() == "--xml" || optionchk.CheckedItems[x].ToString()== "--pool")
            Console.WriteLine("list/xml");
          else
            options = options + " " + optionchk.CheckedItems[x].ToString();
        }
      }
      try
      {
        string path = buildtx.Text.ToString().Replace(buildtx.Text.ToString().Substring(0, 2), "/cygdrive/" + buildtx.Text.ToString().Substring(0, 1).ToLower());
        //Console.WriteLine("path is {0}", path);
        //Console.WriteLine("The pool value is {0}", pool.ToString());
        if ((listtx.Text.Length == 0) && (xmltx.Text.Length != 0) && (!pool))
          Cmdtx.Text = options + " --xml=" + xmltx.Text.ToString() + " " + servercmd + " " + clientcmd;
        if ((xmltx.Text.Length == 0) && (listtx.Text.Length != 0) && (!pool))
          Cmdtx.Text = options + list1 + " " + servercmd + " " + clientcmd;
        if ((listtx.Text.Length != 0) && (xmltx.Text.Length != 0) && (!pool))
          Cmdtx.Text = options + list1 + " --xml=" + xmltx.Text.ToString() + " " + servercmd + " " + clientcmd;
        if ((listtx.Text.Length == 0) && (xmltx.Text.Length != 0) && pool)
          Cmdtx.Text = options + " --xml=" + xmltx.Text.ToString() + " " + "--pool=" + poolcb.SelectedItem.ToString() + " " + servercmd + " " + clientcmd;
        if ((xmltx.Text.Length == 0) && (listtx.Text.Length != 0) && pool)
          Cmdtx.Text = options + list1 + " " + "--pool=" + poolcb.SelectedItem.ToString() + " " + servercmd + " " + clientcmd;
        if ((listtx.Text.Length != 0) && (xmltx.Text.Length != 0) && pool)
          Cmdtx.Text = options + list1 + " --xml=" + xmltx.Text.ToString() + " " + "--pool=" + poolcb.SelectedItem.ToString() + " " + servercmd + " " + clientcmd;
      }
      catch (Exception)
      {
        MessageBox.Show("You cannot leave builddir empty", "WARNING", MessageBoxButtons.OK, MessageBoxIcon.Warning);
      }
      setTextData();
    }

    public void LogDirb_Click(object sender, EventArgs e)
    {
      string path = FileBrowser(LogDirtx, false, 0);
    }
    public void GfeDirb_Click(object sender, EventArgs e)
    {
      string path = FileBrowser(GfeDirtx, false, 0);
    }
    public void GfJavab_Click(object sender, EventArgs e) 
    {
      string path = FileBrowser(GfJavatx, false, 1);
    }
    public void ClassPathb_Click(object sender, EventArgs e)
    {
      string path = FileBrowser(ClassPathtx, false, 1);
    }
    public void LogDirb1_Click(object sender, EventArgs e)
    {
      string path = FileBrowser(LogDirtx1, false, 0);
    }
    public void GfeDirb1_Click(object sender, EventArgs e)
    {
      string path = FileBrowser(GfeDirtx1, false, 0);
    }
    public void GfJavab1_Click(object sender, EventArgs e)
    {
      string path = FileBrowser(GfJavatx1, false, 1);
    }
    public void Lin_ClassPathb_Click(object sender, EventArgs e)
    {
      string path = FileBrowser(Lin_ClassPathtx, false, 1);
    }
    public void buildb_Click(object sender, EventArgs e)
    {
      string path = FileBrowser(buildtx, false, 0);
      setTextData();
    }
    public void listb_Click(object sender, EventArgs e)
    {
      string path = FileBrowser(listtx, true, 1);
      setTextData();
    }
    public void xmlb_Click(object sender, EventArgs e)
    {
       string path = FileBrowser(xmltx,true,1);
       setTextData();
    }
    public void workingdirb_Click(object sender, EventArgs e)
    {
      string path = FileBrowser(workingdirtx,false,0);
      //setTextData();
    }

    public void optionchk_ItemCheck(object sender, ItemCheckEventArgs e)
    {
       string item = optionchk.SelectedItem.ToString();
       if ( e.NewValue == CheckState.Checked )
         {
           if (item.ToString() == "--list")
           {
             listb.Enabled = true;
             listtx.Enabled = true;
           }
           if (item.ToString() == "--xml")
           {
             xmlb.Enabled = true;
             xmltx.Enabled = true;
           }
           if (item.ToString() == "--pool")
             poolcb.Enabled = true;
         }
         else if (e.NewValue == CheckState.Unchecked)
         {
           if (item.ToString() == "--list")
           {
             listb.Enabled = false;
             listtx.Enabled = false;
             listtx.Clear();
           }
           if (item.ToString() == "--xml")
           {
             xmlb.Enabled = false;
             xmltx.Enabled = false;
             xmltx.Clear();
           }
           if (item.ToString() == "--pool")
             poolcb.Enabled = false;
       }
    }

    public void linuxchk_ItemCheck(object sender, ItemCheckEventArgs e)
    {
      string item = linuxchk.SelectedItem.ToString();
      if (e.NewValue == CheckState.Checked)
        linuxlbx.Items.Add(item);
      else
        linuxlbx.Items.Remove(item);
    }

    public void windowschk_ItemCheck(object sender, ItemCheckEventArgs e)
    {
      string item = windowschk.SelectedItem.ToString();
      if (e.NewValue == CheckState.Checked)
        windowslbx.Items.Add(item);
      else
        windowslbx.Items.Remove(item);
    }

    public void clearb_Click(object sender, EventArgs e)
    {
      LogDirtx.Clear();
      GfeDirtx.Clear();
      ClassPathtx.Clear();
      classpathlbx.ClearSelected();
      GfJavatx.Clear();
      classpathlbx.Items.Clear();
      workingdirtx.Clear();
      richtxt.Clear();
    }

    public void resetb_Click(object sender, EventArgs e)
    {
      LogDirtx1.Clear();
      GfeDirtx1.Clear();
      Lin_ClassPathtx.Clear();
      Lin_classpathlbx.ClearSelected();
      GfJavatx1.Clear();
      Lin_classpathlbx.Items.Clear();
    }

    public void clear2b_Click(object sender, EventArgs e)
    {
      buildtx.Clear();
      listtx.Clear();
      xmltx.Clear();
      Cmdtx.Clear();
      optionchk.ClearSelected();
    }

    public void ok1b_Click(object sender, EventArgs e)
    {
      if (!Directory.Exists("C:\\FwkTxtData"))
        Directory.CreateDirectory("C:\\FwkTxtData");
      this.ShowLb.Text = "Your gfcsharp.env file is";
      richtxt.ForeColor = Color.Black;
      richtxt.BackColor = Color.White;
      richtxt.Font = new Font(richtxt.Font.FontFamily, richtxt.Font.Size);
      string wrkdirgf = "";
      if (workingdirtx.Text.Length == 0)
        MessageBox.Show("You cannot proceed future unless you specify a working directory", "WARNING", MessageBoxButtons.OK, MessageBoxIcon.Warning);
      else
      {
         char[] charsL = new char[] { ':' };
         char[] charsW = new char[] { ';' };
         string classpathL = "";
         string classpathW = "";
         foreach (string c in classpathlbx.Items)
         {
           classpathW = classpathW + c.ToString() + ";";
         }
         foreach (string c in Lin_classpathlbx.Items)
         {
           classpathL = classpathL + c.ToString() + ":";
         }
         if (windowsb.Enabled)
           classpathW = classpathW.TrimEnd(charsW);
         else
         {
           classpathL = classpathL.TrimEnd(charsL);
           classpathW = classpathW.TrimEnd(charsW);
         }
         wrkdirgf = workingdirtx.Text.ToString() + "/gfcsharp.env".Replace("/", "\\");
         if (windowsb.Enabled)
         {
           StreamWriter sw1 = new StreamWriter(wrkdirgf);
           sw1.WriteLine("FWK_LOGDIR.WIN={0}", '"' + LogDirtx.Text.ToString() + '"');
           sw1.WriteLine("GFE_DIR.WIN={0}", '"' + GfeDirtx.Text.ToString() + '"');
           sw1.WriteLine("CLASSPATH.WIN={0}", '"' + classpathW + '"');
           sw1.WriteLine("GF_JAVA.WIN={0}", '"' + GfJavatx.Text.ToString() + '"');
           if (perfTest.Checked)
             sw1.WriteLine("PERFTEST=true");
           else
             sw1.WriteLine("");
           sw1.Close();
           
           StreamWriter sw3 = new StreamWriter("abc.env");
           sw3.WriteLine(LogDirtx.Text.ToString());
           sw3.WriteLine(GfeDirtx.Text.ToString());
           sw3.WriteLine(classpathW);
           sw3.WriteLine(workingdirtx.Text.ToString());
           sw3.WriteLine(GfJavatx.Text.ToString());
           sw3.Close();
           FileCopy("abc.env");
         }
         else
         {
           StreamWriter sw1 = new StreamWriter(wrkdirgf);
           sw1.WriteLine("FWK_LOGDIR.WIN={0}", '"' + LogDirtx.Text.ToString() + '"');
           sw1.WriteLine("FWK_LOGDIR.LIN={0}", '"' + LogDirtx1.Text.ToString() + '"');
           sw1.WriteLine("GFE_DIR.WIN={0}", '"' + GfeDirtx.Text.ToString() + '"');
           sw1.WriteLine("GFE_DIR.LIN={0}", '"' + GfeDirtx1.Text.ToString() + '"');
           sw1.WriteLine("CLASSPATH.WIN={0}", '"' + classpathW + '"');
           sw1.WriteLine("CLASSPATH.LIN={0}", '"' + classpathL + '"');
           sw1.WriteLine("GF_JAVA.WIN={0}", '"' + GfJavatx.Text.ToString() + '"');
           sw1.WriteLine("GF_JAVA.LIN={0}", '"' + GfJavatx1.Text.ToString() + '"');
           if (perfTest.Checked)
             sw1.WriteLine("PERFTEST=true");
           else
             sw1.WriteLine("");
           sw1.Close();
           
           StreamWriter sw3 = new StreamWriter("abc.env");
           sw3.WriteLine(LogDirtx.Text.ToString());
           sw3.WriteLine(GfeDirtx.Text.ToString());
           sw3.WriteLine(classpathW);
           sw3.WriteLine(workingdirtx.Text.ToString());
           sw3.WriteLine(GfJavatx.Text.ToString());
           sw3.WriteLine(LogDirtx1.Text.ToString());
           sw3.WriteLine(GfeDirtx1.Text.ToString());
           sw3.WriteLine(GfJavatx1.Text.ToString());
           sw3.WriteLine(classpathL);
           sw3.Close();
           FileCopy("abc.env");
         }

         string wrkdirgp = workingdirtx.Text.ToString() + "/gfcpp.properties".Replace("/", "\\");
         StreamWriter sw2 = new StreamWriter(wrkdirgp);
         if (cb.SelectedIndex > -1)
           sw2.WriteLine("log-level={0}", cb.SelectedItem.ToString());
         else
         {
           //Console.WriteLine("The default property is info:");
           sw2.WriteLine("log-level=info");
         }
         sw2.Close();
         StreamReader sr = new StreamReader(wrkdirgf);
         richtxt.Text = sr.ReadToEnd();
         sr.Close();
       }
    }

    public void FileCopy(string fileName)
    {
      string newpath = @"C:\FwkTxtData\"+fileName;
      string gpath_temp = @"C:\FwkTxtData\gfcsharp.env";
      string gpath_wrk = workingdirtx.Text.ToString() + "/gfcsharp.env";
      if (!File.Exists(newpath))
      {
        File.Copy(fileName, newpath);
      }
      else
      {
        File.Delete(newpath);
        File.Copy(fileName, newpath);
      }
      //Console.WriteLine("Souurce ={0}", gpath);
      if (!File.Exists(gpath_temp))
        File.Copy(gpath_wrk, gpath_temp);
      else
      {
        //Console.Write("Hello suppi");
        File.Delete(gpath_temp);
        File.Copy(gpath_wrk, gpath_temp);
      }
      
    }
    
    public void Showb_Click(object sender, EventArgs e)
    {
      string wrkdir = workingdirtx.Text.ToString() + "/gfcsharp.env".Replace("/", "\\");
      StreamReader sr = new StreamReader(wrkdir);
      richtxt.Text = sr.ReadToEnd();
      sr.Close();
    }

    public void windowsb_Click(object sender, EventArgs e)
    {
      windowslbx.Items.Add(windowstxt.Text.ToString());
    }

    public void linuxb_Click(object sender, EventArgs e)
    {
      linuxlbx.Items.Add(linuxtxt.Text.ToString());
    }

    public void clientb_Click(object sender, EventArgs e)
    {
      clientlbx.Items.Add(clienttxt.Text.ToString());
    }

    public void stopb_Click(object sender, EventArgs e)
    {
      //Process[] myProcesses;
      //myProcesses =Process.GetProcessesByName("FwkDriver");
      //foreach (Process p in myProcesses)
      //{
      //  Console.WriteLine("Terminating the process {0} as STOP button was clicked.",p.ProcessName);
      //  p.Kill();
      //}
      Process[] processlist = Process.GetProcesses();
      foreach (Process p in processlist)
      {
        if (p.ProcessName.Equals("FwkLauncher"))
        {
          Console.WriteLine("Killing process {0}",p.ProcessName);
          p.Kill();
        }
        if (p.ProcessName.Equals("FwkClient"))
        {
          Console.WriteLine("Killing process {0}", p.ProcessName);
          p.Kill();
        }
        if (p.ProcessName.Equals("FwkDriver"))
        {
          Console.WriteLine("Killing process {0}", p.ProcessName);
          p.Kill();
        }
        p.Close();
      }
      
       // Console.WriteLine("The process is {0}",p.ProcessName);
     
    }

    public void delete1b_Click(object sender, EventArgs e)
    {
      while (windowslbx.SelectedItems.Count > 0)
        windowslbx.Items.Remove(windowslbx.SelectedItems[0]);
    }
    public void delete2b_Click(object sender, EventArgs e)
    {
      while (clientlbx.SelectedItems.Count > 0)
        clientlbx.Items.Remove(clientlbx.SelectedItems[0]);
    }
    public void delete3b_Click(object sender, EventArgs e)
    {
      while (linuxlbx.SelectedItems.Count > 0)
        linuxlbx.Items.Remove(linuxlbx.SelectedItems[0]);
    }


  }

}
