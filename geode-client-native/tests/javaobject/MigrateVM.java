/*================================================================================
 * Copyright (c) 2008 VMware, Inc. All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice, 
 *  this list of conditions and the following disclaimer.
 *
 *   * Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution.
 *
 *    * Neither the name of Pivotal Software, Inc. nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without specific prior 
 *    written permission.
 *
 *    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 *    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 *    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 *    IN NO EVENT SHALL PIVOTAL SOFTWARE, INC. OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
 *    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
 *    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR 
 *    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 *    WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 *    ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 *    POSSIBILITY OF SUCH DAMAGE.
 *    ================================================================================*/

package javaobject;

import java.net.URL;

import com.vmware.vim25.HostVMotionCompatibility;
import com.vmware.vim25.TaskInfo;
import com.vmware.vim25.VirtualMachineMovePriority;
import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.mo.ComputeResource;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.Task;
import com.vmware.vim25.mo.VirtualMachine;
//import com.gemstone.gemfire.internal.cache.GemFireCacheImpl; 

public class MigrateVM
{
  public static void main(String[] args) throws Exception
  {
    if(args.length!=5)
   {
      System.out.println("Usage: java MigrateVM <url> " +
      //GemFireCacheImpl.getInstance().getLogger().info("Usage: java MigrateVM <url> " +
      "<username> <password> <vmname> <newhost>");
      System.exit(0);
    }

    String vmname = args[3];
    //String[] vmname = vmnames.split(",");
    String newHostName = args[4];
    //String[] newHostName = newHostNames.split(",");

    //GemFireCacheImpl.getInstance().getLogger().info("rjk: args[3] " + vmname + " and args[4] " + newHostName );
    ServiceInstance si = new ServiceInstance(
        new URL(args[0]), args[1], args[2], true);

    Folder rootFolder = si.getRootFolder();
VirtualMachine vm = (VirtualMachine) new InventoryNavigator(
        rootFolder).searchManagedEntity(
            "VirtualMachine", vmname);
    HostSystem newHost = (HostSystem) new InventoryNavigator(
        rootFolder).searchManagedEntity(
            "HostSystem", newHostName);
    ComputeResource cr = (ComputeResource) newHost.getParent();

    String[] checks = new String[] {"cpu", "software"};
    HostVMotionCompatibility[] vmcs =
      si.queryVMotionCompatibility(vm, new HostSystem[]
         {newHost},checks );

    String[] comps = vmcs[0].getCompatibility();
    if(checks.length != comps.length)
    {
      System.out.println("CPU/software NOT compatible. Exit.");
      si.getServerConnection().logout();
      return;
    }

    Task task = vm.migrateVM_Task(cr.getResourcePool(), newHost,
        VirtualMachineMovePriority.highPriority,
        VirtualMachinePowerState.poweredOn);

    if(task.waitForMe()==Task.SUCCESS)
    {
      System.out.println("VMotioned!");
      //GemFireCacheImpl.getInstance().getLogger().info("VMotioned!");
    }
    else
    {
      System.out.println("VMotion failed!");
      //GemFireCacheImpl.getInstance().getLogger().info("VMotion failed!");
      TaskInfo info = task.getTaskInfo();
      System.out.println(info.getError().getFault());
    }
    si.getServerConnection().logout();
  }
}
