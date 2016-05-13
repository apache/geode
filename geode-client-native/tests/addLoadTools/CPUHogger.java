/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
  */
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.event.*;
import java.net.*;
class SimpleRunnable implements Runnable {
    static double x=1256e10;
    static double y=1245e10;
    static int runLoop=10;
    int delay = 10;
    static volatile boolean stopped = false;
    public static void stop(){
      stopped = true;
    }
    public SimpleRunnable(int loop)
    {
      runLoop = loop;
    }
    public void setDelay(int d)
    {
      delay = d;
      System.out.println("CPUHogger: delay now is " + delay); 
    }
  public void run() {
    System.out.println("Thread '"
	+ Thread.currentThread().getName() + "' started");
    int i=0;
    for(i=0; i< runLoop; i++) {
      if(stopped)
      {
         System.out.println("thread '"
	+ Thread.currentThread().getName() + "'" + "stopping");
	break;
      }
       crunch(x,y);
       if(i%10 == 0){
	    try
	         {
		      // Sleep at least delay milliseconds.
		      // 1 millisecond = 1/1000 of a second.
		   if(delay > 0)
		      Thread.sleep( delay );
	     	      //Thread.yield();
		         }
	    catch ( InterruptedException e )
	         {
		      System.out.println( "awakened prematurely" );

		  }
       } 
//       if(i%5000==0)
//	  System.out.println( "loop " + i );
    }
//    System.out.println( "done crunching" );
  }
  public synchronized void crunch(double x, double y) {
    x = x* y;
//    System.out.println("in thread named '"
//	+ Thread.currentThread().getName() + "'" + "x=" +x + ",y="+y);
    x= x/y;
  }
}
  class CPUHogger extends JPanel
                        implements ActionListener,
                                   WindowListener,
                                   ChangeListener {
        static SimpleRunnable simpleRunnable=null;
     static JFrame frame = null;
    //Set up animation parameters.
    static final int SCALE_MIN = 0;
    static final int SCALE_MAX = 100;
    static final int SCALE_INIT = 50;    //initial frames per second
    public CPUHogger() {
        setLayout(new BoxLayout(this, BoxLayout.PAGE_AXIS));
        //Create the label.
        JLabel sliderLabel = new JLabel("Thread Delays in 10's Milliseconds", JLabel.CENTER);
        sliderLabel.setAlignmentX(Component.CENTER_ALIGNMENT);

        //Create the slider.
        JSlider delayScale = new JSlider(JSlider.HORIZONTAL,
                                              SCALE_MIN, SCALE_MAX, SCALE_INIT);
        

        delayScale.addChangeListener(this);

        //Turn on labels at major tick marks.

        delayScale.setMajorTickSpacing(10);
        delayScale.setMinorTickSpacing(2);
        delayScale.setPaintTicks(true);
        delayScale.setPaintLabels(true);
        delayScale.setBorder(
               BorderFactory.createEmptyBorder(0,0,10,0));
        //Font font = new Font("Serif", Font.ITALIC, 15);
        //delayScale.setFont(font);

        //Put everything together.
        add(sliderLabel);
        add(delayScale);
        setBorder(BorderFactory.createEmptyBorder(10,10,10,10));
    }

    /** Add a listener for window events. */
    void addWindowListener(Window w) {
        w.addWindowListener(this);
    }
    //React to window events.
    public void windowIconified(WindowEvent e) {}
    public void windowDeiconified(WindowEvent e){}
    public void actionPerformed(ActionEvent e) {}
    public void windowOpened(WindowEvent e) {}
    public void windowClosing(WindowEvent e) {}
    public void windowClosed(WindowEvent e) {
      SimpleRunnable.stop();
    }
    public void windowActivated(WindowEvent e) {}
    public void windowDeactivated(WindowEvent e) {}

    /** Listen to the slider. */
    public void stateChanged(ChangeEvent e) {
        JSlider source = (JSlider)e.getSource();
        if (!source.getValueIsAdjusting()) {
            int fps = (int)source.getValue();
                int delay = fps*10;
		if(simpleRunnable != null)
	           simpleRunnable.setDelay(delay);
        }
    }

        public static void main(String[] args) {
	  if(args.length !=2)
	  {
	     System.out.println("Usage: CPUHogger <loop> <number of threads>" ); 
	     System.exit(-1);
	  }
	  int i=0;
	  int loop = Integer.valueOf(args[0]).intValue();
	  int n = Integer.valueOf(args[1]).intValue();
	  simpleRunnable = new SimpleRunnable(loop);
	  runSlider(args);
	  System.out.println("CPUHogger: loop=" + loop + ",sleep=" + n); 
	  Thread[] t= new Thread[n];
	  for (i=0; i<n; i++)
	  {
	    t[i] = new Thread(simpleRunnable);
//	    System.out.println("new Thread() " + (t[i]==null?
//		  "fail" : "Succeed") + "ed.");
	    t[i].start();
	  }
//	  System.out.println("Done starting Threads ");
	  for (i=0; i<n; i++)
	  {
	    
	    try { t[i].join();}catch (InterruptedException ignored){}
//	    System.out.println("Done joining the " + i + "-th Thread");
	  }
//	  System.out.println("Done joining Threads ");
          JOptionPane.showMessageDialog(frame, "We're not hogging the CPUs anymore!");
          frame.dispose();
      }
    /**
     * Create the GUI and show it.  For thread safety,
     * this method should be invoked from the
     * event-dispatching thread.
     */
    private static void createAndShowGUI() {
        //Create and set up the window.
        String hostname="Dummy";
        try {
          InetAddress addr = InetAddress.getLocalHost();
		      
          // Get IP Address
          //byte[] ipAddr = addr.getAddress();
			      
          // Get hostname
          hostname = addr.getHostName();
	} catch (UnknownHostException e) {
	}
        frame = new JFrame("CPU Hogger on " + hostname);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        CPUHogger hogger = new CPUHogger();
                
        //Add content to the window.
        frame.getContentPane().add(hogger, BorderLayout.CENTER);

        //Display the window.
        frame.pack();
        frame.setVisible(true);
    }

    public static void runSlider(String[] args) {
        /* Turn off metal's use of bold fonts */
        UIManager.put("swing.boldMetal", Boolean.FALSE);
        
        
        //Schedule a job for the event-dispatching thread:
        //creating and showing this application's GUI.
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                createAndShowGUI();
            }
        });
    }
  }


