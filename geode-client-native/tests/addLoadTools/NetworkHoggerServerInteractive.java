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
import java.io.*;
class MultiServerThread extends Thread {
  static int len = 1024;
  static int reset = 0;
  private Socket clientSocket = null;
  public MultiServerThread(Socket socket) {
    super("MultiServerThread");
    this.clientSocket = socket;
  }
  public static void setLen(int l) {
      if(len > 0){
	len = l;
	reset = 1;
      }
  System.out.println("MultiServerThread: package length=" + len); 
  }
  public void run() {
    try {
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(
				new InputStreamReader(
				clientSocket.getInputStream()));
        String inputLine, outputLine;

        outputLine = "Let's get started";
        out.println(outputLine);

        while ((inputLine = in.readLine()) != null) {
	  if(reset == 1){
             outputLine = "Let's get started";
	     reset  = 0;
	  }
             
	  if(outputLine.length() < len)
	    outputLine += inputLine;
             out.println(outputLine);
             if (outputLine.equals("Bye."))
                break;
        }
        out.close();
        in.close();
        clientSocket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

class NetworkHoggerServer {
    static int len = 1024;
    static int reset = 0;
    static volatile boolean listening = true;
    public static void setLen(int l) {
      if(len > 0){
	len = l;
	reset = 1;
	MultiServerThread.setLen(l);
      }
  System.out.println("NetworkHoggerServerInteractive: package length=" + len); 

    }
    public static void stop() {
      listening = false;
    }
    public static void runServer(String[] args)  throws IOException {

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(4444);
        } catch (IOException e) {
            System.err.println("Could not listen on port: 4444.");
            System.exit(1);
        }

        while(listening) {
            new MultiServerThread(serverSocket.accept()).start();
  System.out.println("NetworkHoggerServerInteractive: a new client came on"); 
	}

        serverSocket.close();
    }
}
class NetworkHoggerServerInteractive extends JPanel
                        implements ActionListener,
                                   WindowListener,
                                   ChangeListener {
    static SimpleRunnable simpleRunnable=null;
    //Set up animation parameters.
    static final int SCALE_MIN = 0;
    static final int SCALE_MAX = 100;
    static final int SCALE_INIT = 50;    //initial frames per second
    private int multiplier = 10240;
    public NetworkHoggerServerInteractive(String[] args) {
    multiplier = 1024* Integer.valueOf(args[0]).intValue();
        setLayout(new BoxLayout(this, BoxLayout.PAGE_AXIS));
        //Create the label.
        JLabel sliderLabel = new JLabel("Package Size in " + args[0] + "KBytes", JLabel.CENTER);
        sliderLabel.setAlignmentX(Component.CENTER_ALIGNMENT);

        //Create the slider.
        JSlider delayScale = new JSlider(JSlider.HORIZONTAL,
                                              SCALE_MIN, SCALE_MAX, SCALE_INIT);
        

        delayScale.addChangeListener(this);

        //Turn on labels at major tick marks.

        delayScale.setMajorTickSpacing(10);
        delayScale.setMinorTickSpacing(1);
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
	    NetworkHoggerServer.stop();
    }
    public void windowActivated(WindowEvent e) {}
    public void windowDeactivated(WindowEvent e) {}

    /** Listen to the slider. */
    public void stateChanged(ChangeEvent e) {
        JSlider source = (JSlider)e.getSource();
        if (!source.getValueIsAdjusting()) {
            int fps = (int)source.getValue();
            int len = fps*multiplier;
	    NetworkHoggerServer.setLen(len);
        }
    }

        public static void main(String[] args) {
	  if(args.length !=1)
	  {
	     System.out.println("Usage: NetworkHoggerServerInteractive <increment multiplier(e.g. 100, 1000)>" ); 
	     System.exit(-1);
	  }
	  runSlider(args);
	  try {
	    NetworkHoggerServer.runServer(args);
          } catch (IOException e) {
              System.err.println("main failed.");
              System.exit(1);
          }
      }
    /**
     * Create the GUI and show it.  For thread safety,
     * this method should be invoked from the
     * event-dispatching thread.
     */
    private static void createAndShowGUI(String[] args) {
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
        JFrame frame = new JFrame("Network Hogger Server on " + hostname);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        NetworkHoggerServerInteractive hogger = new NetworkHoggerServerInteractive(args);
                
        //Add content to the window.
        frame.getContentPane().add(hogger, BorderLayout.CENTER);

        //Display the window.
        frame.pack();
        frame.setVisible(true);
    }

    public static void runSlider(String[] args) {
         final String[] args1 = args;
        /* Turn off metal's use of bold fonts */
        UIManager.put("swing.boldMetal", Boolean.FALSE);
        
        
        //Schedule a job for the event-dispatching thread:
        //creating and showing this application's GUI.
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                createAndShowGUI(args1);
            }
        });
    }
  }


