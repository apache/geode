#Geode Network Configuration Best Practices##IntroductionGeode is a data management platform that provides real-time, consistent access to data-intensive applications throughout widely distributed cloud architectures. Geode pools memory, CPU, network resources, and optionally disk storage across multiple processes to manage application objects and behavior. It uses dynamic replication and data partitioning techniques to implement high availability, improved performance, scalability, and fault tolerance. In addition to being a distributed data container, Geode is an in-memory data management system that provides reliable asynchronous event notifications and guaranteed message delivery.
Due to Geode’s distributed nature, network resources can have a significant impact on system performance and availability.  Geode is designed to be fault tolerant and to handle network disruptions gracefully. However, proper network design and tuning are essential to achieving optimum performance and High Availability with Geode.
##Purpose
The purpose of this paper is to provide best practice recommendations for configuring the network resources in a Geode solution. The recommendations in this paper are not intended to provide a comprehensive, one-size-fits-all guide to network design and implementation. However, they should serve to provide a working foundation to help guide Geode implementations. 
##Scope
This paper covers topics related to the design and configuration of network components used as part of a Geode solution. This paper covers the following topics:+	Network architectural goals+	Network Interface Card (NIC) selection and configuration+	Switch configuration considerations+	General network infrastructure considerations+	TCP vs. UDP protocol considerations+	Socket communications and socket buffer settings+	TCP settings for congestion control, window scaling, etc.
##Target Audience
This paper assumes a basic knowledge and understanding of Geode, virtualization concepts and networking. Its primary audience consists of:+	Architects: who can use this paper to inform key decisions and design choices surrounding a Geode solution+	System Engineers and Administrators: who can use this paper as a guide for system configuration
##Geode: A Quick Review
 ###Overview
A Geode distributed system is comprised of members distributed over a network to provide in-memory speed along with high availability, scalability, and fault tolerance. Each member consists of a Java virtual machine (JVM) that hosts data and/or compute logic and is connected to other Geode members over a network. Members hosting data maintain a cache consisting of one or more Regions that can be replicated or partitioned across the distributed system.  Compute logic is deployed to members as needed by adding the appropriate Java JAR files to the member’s class path. 
####Companies using Geode have:+	Reduced risk analysis time from 6 hours to 20 minutes, allowing for record profits in the flash crash of 2008 that other firms were not able to monetize.+	Improved end-user response time from 3 seconds to 50 ms, worth 8 figures a year in new revenue from a project delivered in fewer than 6 months.+	Tracked assets in real time to coordinate all the right persons and machinery into the right place at the right time to take advantage of immediate high-value opportunities.+	Created end-user reservation systems that handle over a billion requests daily with no downtime.
###Geode Communications
Geode members use a combination of TCP, UDP unicast and UDP multicast for communications between members. Geode members maintain constant communications with other members for the purposes of distributing data and managing the distributed system. 
####Member Discovery Communications
Peer member discovery is what defines a distributed system. All applications and cache servers that use the same settings for peer discovery are members of the same distributed system. Each system member has a unique identity and knows the identities of the other members. A member can belong to only one distributed system at a time. Once they have found each other, members communicate directly, independent of the discovery mechanism. In peer discovery, Geode uses a membership coordinator to manage member joins and departures. There are two discovery options: using multicast or using locators.
+	**UDP/IP Multicast** New members broadcast their connection information over the multicast address and port to all running members. Existing members respond to establish communication with the new member. +	**Geode Locators Using TCP/IP** Peer locators manage a dynamic list of distributed system members. New members connect to one of the locators to retrieve the member list, which it uses to join the system.
####General Messaging and Region Operation CommunicationsGeode supports the use of TCP, UDP Unicast or UDP Multicast for general messaging and for region operations distribution.  The default is TCP. However, Geode may be configured to use UDP if desired.
###Geode Topologies
Geode members can be configured in multiple topologies to provide a flexible solution for enterprise system needs. The following sections summarize these topologies.####Peer-to-Peer Topology
The peer-to-peer topology is the basic building block for Geode installations. In this configuration, each member directly communicates with every other member in the distributed system.  New members broadcast their connection information to all running members. Existing members respond to establish communication with the new member.  A typical example of this configuration is an application server cluster in which an application instance and a Geode server are co-located in the same JVM.  This configuration is illustrated in the diagram below. 

****link to network1.png ####Client Server Topology
The Client Server topology is the most commonly used topology for Geode installations. In this configuration, applications communicate with the Geode servers using a Geode client. The Geode client consists of a set of code that executes in the same process as the application.  The client defines a connection pool for managing connectivity to Geode servers and may also provide a local cache to keep selected Geode data in process with the application.  New Geode servers starting up will contact a locator to join the distributed system and be added to the membership view. The locators in a Geode system server to coordinate the membership of the system and provide load balancing for Geode clients. This configuration is illustrated in the following diagram.NOTE: this paper focuses on network configuration in the context of this topology. ****link to network2.png###Geode Network Characteristics 
Geode is a distributed, in-memory data platform designed to provide extreme performance and high levels of availability. In its most common deployment configurations, Geode makes extensive use of network resources for data distribution, system management and client request processing. As a result, network performance and reliability can have a significant impact on Geode. To obtain optimal Geode performance, the network needs to exhibit the following characteristics. ####Low Latency
The term latency refers to any of several kinds of delays typically incurred in the processing of network data. These delays include:+	Propagation delays – these are the result of the distance that must be covered in order for data moving across the network to reach its destination and the medium through which the signal travels. This can range from a few nanoseconds or microseconds in local area networks (LANs) up to about 0.25 seconds in geostationary-satellite communications systems. +	Transmission delays – these delays are the result of the time required to push all the packet’s bits into the link, which is a function of the packet’s length and the data rate of the link. For example, to transmit a 10 Mb file over a 1 Mbps link would require 10 seconds while the same transmission over a 100 Mbps link would take only 0.1 seconds. +	Processing delays – these delays are the result of the time it takes to process the packet header, check for bit-level errors and determine the packet’s destination. Processing delays in high-speed routers are often minimal. However, for networks performing complex encryption or Deep Packet Inspection (DPI), processing delays can be quite large. In addition, routers performing Network Address Translation (NAT) also have higher than normal processing delays because those routers need to examine and modify both incoming and outgoing packets. +	Queuing delays – these delays are the result of time spent by packets in routing queues. The practical reality of network design is that some queuing delays will occur.  Effective queue management techniques are critical to ensuring that the high-priority traffic experiences smaller delays while lower priority packets see longer delays. 
####Best Practices
It should be noted that latency, not bandwidth, is the most common performance bottleneck for network dependent systems like websites. Therefore, one of the key design goals in architecting a Geode solution is to minimize network latency. Best practices for achieving this goal include:+	Keep Geode members and clients on the same LANKeep all members of a Geode distributed system and their clients on the same LAN and preferably on the same LAN segment. The goal is to place all Geode cluster members and clients in close proximity to each other on the network. This not only minimizes propagation delays, it also serves to minimize other delays resulting from routing and traffic management. Geode members are in constant communication and so even relatively small changes in network delays can multiply, impacting overall performance. +	Use network traffic encryption prudentlyDistributed systems like Geode generate high volumes of network traffic, including a fair amount of system management traffic. Encrypting network traffic between the members of a Geode cluster will add processing delays even when the traffic contains no sensitive data. As an alternative, consider encrypting only the sensitive data itself. Or, if it is necessary to restrict access to data on the wire between Geode members, consider placing the Geode members in a separate network security zone that cordons off the Geode cluster from other systems. +	Use the fastest link possibleAlthough bandwidth alone does not determine throughput - all things being equal, a higher speed link will transmit more data in the same amount of time than a slower one. Distributed systems like Geode move high volumes of traffic through the network and can benefit from having the highest speed link available. While some Geode customers with exacting performance requirements make use of InfiniBand network technology that is capable of link speeds up to 40Gbps, 10GbE is sufficient for most applications and is generally recommended for production and performance/system testing environments. For development environments and less critical applications, 1GbE is often sufficient. 
####High Throughput
In addition to low latency, the network underlying a Geode system needs to have high throughput. ISPs and the FCC often use the terms 'bandwidth' and 'speed' interchangeably although they are not the same thing. In fact, bandwidth is only one of several factors that affect the perceived speed of a network. Therefore, it is more accurate to say that bandwidth describes a network’s capacity, most often expressed in bits per second. Specifically, bandwidth refers to the data transfer rate (in bits per second) supported by a network connection or interface. Throughput, on the other hand, can often be significantly less than the network’s full capacity. Throughput, the useable link bandwidth, may be impacted by a number of factors including:
+	Protocol inefficiency – TCP is an adaptive protocol that seeks to balance the demands placed on network resources from all network peers while making efficient use of the underlying network infrastructure. TCP detects and responds to current network conditions using a variety of feedback mechanisms and algorithms. The mechanisms and algorithms have evolved over the years but the core principles remain the same:++	All TCP connections begin with a three-way handshake that introduces latency and makes TCP connection creation expensive++	TCP slow-start is applied to every new connection by default. This means that connections can’t immediately use the full capacity of the link. The time required to reach a specific throughput target is a function of both the round trip time between the client and server and the initial congestion window size.++	TCP flow control and congestion control regulate the throughput of all TCP connections.++	TCP throughput is regulated by the current congestion window size.+	Congestion – this occurs when a link or node is loaded to the point that its quality of service degrades. Typical effects include queuing delay, packet loss or blocking of new connections. As a result, an incremental increase in offered load on a congested network may result in an actual reduction in network throughput.  In extreme cases, networks may experience a congestion collapse where reduced throughput continues well after the congestion-inducing load has been eliminated and renders the network unusable.  This condition was first documented by John Nagle in 1984 and by 1986 had become a reality for the Department of Defense’s ARPANET – the precursor to the modern Internet and the world’s first operational packet-switched network. These incidents saw sustained reductions in capacity, in some cases capacity dropped by a factor of 1,000! Modern networks use flow control, congestion control and congestion avoidance techniques to avoid congestion collapse. These techniques include: exponential backoff, TCP Window reduction and fair queuing in devices like routers. Packet prioritization is another method used to minimize the effects of congestion. 
####Best Practices
Geode systems are often called upon to handle extremely high transaction volumes and as a consequence move large amounts of traffic through the network.  As a result, one of the primary design goals in architecting a Geode solution is to maximize network throughput.

Best practices for achieving this goal include:+	Increasing TCP’s Initial Congestion WindowA larger starting congestion window allows TCP transfers more data in the first round trip and significantly accelerates the window growth – an especially critical optimization for bursty and short-lived connections. +	Disabling TCP Slow-Start After Idle Disabling slow-start after idle will improve performance of long-lived TCP connections, which transfer data in bursts. +	Enabling Window Scaling (RFC 1323) Enabling window scaling increases the maximum receive window size and allows high-latency connections to achieve better throughput.+	Enabling TCP Low Latency Enabling TCP Low Latency effectively tells the operating system to sacrifice throughput for lower latency. For latency sensitive workloads like Geode, this is an acceptable tradeoff than can improve performance. +	Enabling TCP Fast Open Enabling TCP Fast Open (TFO), allows application data to be sent in the initial SYN packet in certain situations. TFO is a new optimization, which requires support on both clients and servers and may not be available on all operating systems.###Fault Tolerance
Another network characteristic that is key to optimal Geode performance is fault tolerance. Geode operations are dependent on network services and network failures can have a significant impact on Geode system operations and performance.  While fault tolerant network design is beyond the scope of this paper, there are some important considerations to bear in mind when designing Geode Solutions. For the purposes of this paper, these considerations are organized along the lines of the Cisco Hierarchical Network Design Model as illustrated below.

***link to network3.png This model uses a layered approach to network design, representing the network as a set of scalable building blocks, or layers. In designing Geode systems, network fault tolerance considerations include:
+	Access layer redundancy – The access layer is the first point of entry into the network for edge devices and end stations such as Geode servers. For Geode systems, this network layer should have attributes that support high availability including:++	Operating system high-availability features, such as Link Aggregation (EtherChannel or 802.3ad), which provide higher effective bandwidth and resilience while reducing complexity.++	Default gateway redundancy using dual connections to redundant systems (distribution layer switches) that use Gateway Load Balancing Protocol (GLBP), Hot Standby Router Protocol (HSRP), or Virtual Router Redundancy Protocol (VRRP). This provides fast failover from one switch to the backup switch at the distribution layer.++	Switch redundancy using some form of Split Multi-Link Trunking (SMLT). The use of SMLT not only allows traffic to be load-balanced across all the links in an aggregation group but also allows traffic to be redistributed very quickly in the event of link or switch failure. In general the failure of any one component results in a traffic disruption lasting less than half a second (normal less than 100 milliseconds).+	Distribution layer redundancy – The distribution layer aggregates access layer nodes and creates a fault boundary providing a logical isolation point in the event of a failure in the access layer. High availability for this layer comes from dual equal-cost paths from the distribution layer to the core and from the access layer to the distribution layer. This network layer is usually designed for high availability and doesn’t typically require changes for Geode systems.+	Core layer redundancy – The core layer serves as the backbone for the network. The core needs to be fast and extremely resilient because everything depends on it for connectivity. This network layer is typically built as a high-speed, Layer 3 switching environment using only hardware-accelerated services and redundant point-to-point Layer 3 interconnections in the core. This layer is designed for high availability and doesn’t typically require changes for Geode systems.####Best Practices
Geode systems depend on network services and network failures can have a significant impact on Geode operations and performance.   As a result, network fault tolerance is an important design goal for Geode solutions. Best practices for achieving this goal include:+	Use Mode 6 Network Interface Card (NIC) Bonding – NIC bonding involves combining multiple network connections in parallel in order to increase throughput and provide redundancy should one of the links fail. Linux supports six modes of link aggregation:++	Mode 1 (active-backup) in this mode only one slave in the bond is active. A different slave becomes active if and only if the active slave fails. ++	Mode 2 (balance-xor) in this mode a slave is selected to transmit based on a simple XOR calculation that determines which slave to use. This mode provides both load balancing and fault tolerance.++	Mode 3 (broadcast) this mode transmits everything on all slave interfaces. This mode provides fault tolerance.++	Mode 4 (IEEE 802.3ad) this mode creates aggregation groups that share the same speed and duplex settings and utilizes all slaves in the active aggregator according to the 802.3ad specification. ++	Mode 5 (balance-tlb) this mode distributes outgoing traffic according to the load on each slave. One slave receives incoming traffic. If that slave fails, another slave takes over the MAC address of the failed receiving slave.++	Mode 6 (balance-alb) this mode includes balance-tlb plus receive load balancing (rlb) for IPV4 traffic, and does not require any special switch support. The receive load balancing is achieved by ARP negotiation. The bonding driver intercepts the ARP Replies sent by the local system on their way out and overwrites the source hardware address with the unique hardware address of one of the slaves in the bond such that different peers use different hardware addresses for the server.
For Geode systems, Mode 6 is recommended. Mode 6 NIC Bonding (Adaptive Load Balancing) provides both link aggregation and fault tolerance. Mode 1 only provides fault tolerance while modes 2, 3 and 4 require that the link aggregation group reside on the same logical switch and this could introduce a single point of failure when the physical switch to which the links are connected goes offline.+	Use SMLT for switch redundancy – the Split Multi-link Trunking (SMLT) protocol allows multiple Ethernet links to be split across multiple switches in a stack, preventing any single point of failure, and allowing switches to be load balanced across multiple aggregation switches from the single access stack. SMLT provides enhanced resiliency with sub-second failover and sub-second recovery for all speed trunks while operating transparently to end-devices. This allows for the creation of Active load sharing high availability network designs that meet five nines availability requirements. ###Geode Network SettingsTo achieve the goals of low latency, high throughput and fault tolerance, network settings in the operating system and Geode will need to be configured appropriately. The following sections outline recommended settings.####IPv4 vs. IPv6
By default, Geode uses Internet Protocol version 4 (IPv4).Testing with Geode has shown that IPv4 provides better performance than IPv6. Therefore, the general recommendation is to use IPv4 with Geode.  However, Geode can be configured to use IPv6 if desired. If IPv6 is used, make sure that all Geode processes use IPv6. Do not mix IPv4 and IPv6 addresses.  Note: to use IPv6 for Geode addresses, set the following Java property:java.net.preferIPv6Addresses=true
####TCP vs. UDP
Geode supports the use of both TCP and UDP for communications. Depending on the size and nature of the Geode system as well as the types of regions employed, either TCP or UDP may be more appropriate. 
#####TCP CommunicationsTCP (Transmission Control Protocol) provides reliable in-order delivery of system messages. Geode uses TCP by default for inter-cache point-to-point messaging. TCP is generally more appropriate than UDP in the following situations:
+	Partitioned DataFor distributed systems that make extensive use of partitioned regions, TCP is generally a better choice as TCP provides more reliable communications and better performance that UDP. +	Smaller Distributed SystemsTCP is preferable to UDP unicast in smaller distributed systems because it implements more reliable communications at the operating system level than UDP and its performance can be substantially faster than UDP.+	Unpredictable Network LoadsTCP provides higher levels of fault tolerance and reliability than UDP. While Geode implements retransmission protocols to ensure proper delivery of messages over UDP, it cannot fully compensate for heavy congestion and unpredictable spikes in network loading.Note: Geode always uses TCP communications in member failure detection. In this situation, Geode will attempt to establish a TCP/IP connection with the suspect member in order to determine if the member has failed. #####UDP Communications
UDP (User Datagram Protocol) is a connectionless protocol, which uses far fewer resources than TCP.  However, UDP has some important limitations that should be factored into a design, namely:

+ 64K byte message size limit (including overhead for message headers)+ Markedly slower performance on congested networks + Limited reliability (Geode compensates through retransmission protocols)
If a Geode system can operate within the limitations of UDP, then it may be a more appropriate choice than TCP in the following situations:+	Replicated DataIn systems where most or all of the members use the same replicated regions, UDP multicast may be the most appropriate choice. UDP multicast provides an efficient means of distributing all events for a region. However, when multicast is enabled for a region, all processes in the distributed system receive all events for the region. Therefore, multicast is only suitable when most or all members have the region defined and the members are interested in most or all of the events for the region.Note: Even when UDP multicast is used for a region, Geode will send unicast messages in some situations. Also, partitioned regions will use UDP unicast for almost all purposes.
+	Larger Distributed SystemsAs the size of a distributed system increases, the relatively small overhead of UDP makes it the better choice. TCP adds new threads and sockets to every member, causing more overhead as the system grows. Note: to configure Geode to use UDP for inter-cache point-to-point messaging set the following Geode property:disable-tcp=true####TCP Settings
The following sections provide guidance on TCP settings recommended for Geode.#####Geode Settings for TCP/IP Communications+	Socket Buffer SizeIn determining buffer size settings, the goal is to strike a balance between communication needs and other processing. Larger socket buffers allow Geode members to distribute data and events more quickly, but also reduce the memory available for other tasks. In some cases, particularly when storing very large data objects, finding the right socket buffer size can become critical to system performance.Ideally, socket buffers should be large enough for the distribution of any single data object. This will avoid message fragmentation, which lowers performance. The socket buffers should be at least as large as the largest stored objects with their keys plus some overhead for message headers - 100 bytes should be sufficient. If possible, the TCP/IP socket buffer settings should match across the Geode installation. At a minimum, follow the guidelines listed below.++	Peer-to-peer. The socket-buffer-size setting in gemfire.properties should be the same throughout the distributed system.++	Client/server. The client’s pool socket-buffer size-should match the setting for the servers that the pool uses.++	Server. The server socket-buffer size in the server’s cache configuration (e.g. cache.xml file) should match the values defined for the server’s clients. ++	Multisite (WAN). If the link between sites isn’t optimized for throughput, it can cause messages to back up in the queues. If a receiving queue buffer overflows, it will get out of sync with the sender and the receiver won’t know it. A gateway sender's socket-buffer-size should match the gateway receiver’s socket-buffer-size for all receivers that the sender connects to.Note: OS TCP buffer size limits must be large enough to accommodate Geode socket buffer settings. If not, the Geode value will be set to the OS limit – not the requested value.+	TCP/IP Keep Alive 
Geode supports TCP KeepAlive to prevent socket connections from being timed out.
The gemfire.enableTcpKeepAlive system property prevents connections that appear idle from being timed out (for example, by a firewall.) When configured to true, Geode enables the SO_KEEPALIVE option for individual sockets. This operating system-level setting allows the socket to send verification checks (ACK requests) to remote systems in order to determine whether or not to keep the socket connection alive.Note: The time intervals for sending the first ACK KeepAlive request, the subsequent ACK requests and the number of requests to send before closing the socket is configured on the operating system level. See By default, this system property is set to true.+	TCP/IP Peer-to-Peer Handshake TimeoutsThis property governs the amount of time a peer will wait to complete the TCP/IP handshake process. You can change the connection handshake timeouts for TCP/IP connections with the system property p2p.handshakeTimeoutMs.The default setting is 59,000 milliseconds (59 seconds).This sets the handshake timeout to 75,000 milliseconds for a Java application:-Dp2p.handshakeTimeoutMs=75000The properties are passed to the cache server on the gfsh command line:<pre><code>gfsh>start server --name=server1 --J=-Dp2p.handshakeTimeoutMs=75000</code></pre>#####Linux Settings for TCP/IP Communications
The following table summarizes the recommended TCP/IP settings for Linux. These settings are in the /etc/sysctl.conf file

<table>
<tr>
<td>Setting</td>
<td>Recommended Value</td>
<td>Rationale</td>
</tr>
<tr><td>net.core.netdev_max_backlog</td>
<td>30000</td>
<td>Set maximum number of packets, queued on the INPUT side, when the interface receives packets faster than kernel can process them. Recommended setting is for 10GbE links. For 1GbE links use 8000.</td>
</tr>
<tr><td>net.core.wmem_max</td>
<td>67108864</td>
<td>Set max to 16MB (16777216) for 1GbE links and 64MB (67108864) for 10GbE links.</td>
</tr>
<tr>
<td>net.core.rmem_max</td>
<td>67108864</td>
<td>Set max to 16MB (16777216) for 1GbE links and 64MB (67108864) for 10GbE links.</td>
</tr>
<tr> <td>net.ipv4.tcp_congestion_control</td>
<td>htcp</td>
<td>There seem to be bugs in both bic and cubic (the default) for a number of versions of the Linux  kernel up to version 2.6.33. The kernel version for Redhat 5.x is 2.6.18-x and 2.6.32-x for Redhat 6.x
</td>
</tr>
<tr>
<td>net.ipv4.tcp_congestion_window</td>
<td>10</td>
<td>This is the default for Linux operating systems based on Linux kernel 2.6.39 or later.</td>
</tr>
<tr><td>net.ipv4.tcp_fin_timeout</td>
<td>10</td>
<td>This setting determines the time that must elapse before TCP/IP can release a closed connection and reuse its resources. During this TIME_WAIT state, reopening the connection to the client costs less than establishing a new connection. By reducing the value of this entry, TCP/IP can release closed connections faster, making more resources available for new connections. The default value is 60. The recommened setting lowers its to 10. You can lower this even further, but too low, and you can run into socket close errors in networks with lots of jitter.</td>
</tr>
<tr>
<td>net.ipv4.tcp_keepalive_interval</td>
<td>30</td>
<td>This determines the wait time between isAlive interval probes. Default value is 75. Recommended value reduces this in keeping with the reduction of the overall keepalive time.</td>
</tr>
<tr><td>net.ipv4.tcp_keepalive_probes</td>
<td>5</td>
<td>How many keepalive probes to send out before the socket is timed out. Default value is 9. Recommended value reduces this to 5 so that retry attempts will take 2.5 minutes.</td>
</tr>
<tr><td>net.ipv4.tcp_keepalive_time</td>
<td>600</td>	
<td>Set the TCP Socket timeout value to 10 minutes instead of 2 hour default. With an idle socket, the system will wait tcp_keepalive_time seconds, and after that try tcp_keepalive_probes times to send a TCP KEEPALIVE in intervals of tcp_keepalive_intvl seconds. If the retry attempts fail, the socket times out.</td>
</tr>
<tr>
<td>net.ipv4.tcp_low_latency</td>
<td>1</td>
<td>Configure TCP for low latency, favoring low latency over throughput</td>
</tr>
<tr>
<td>net.ipv4.tcp_max_orphans</td>
<td>16384</td>
<td>Limit number of orphans, each orphan can eat up to 16M (max wmem) of unswappable memory</td>
</tr>
<tr>
<td>net.ipv4.tcp_max_tw_buckets</td>
<td>1440000</td>
<td>Maximal number of timewait sockets held by system simultaneously. If this number is exceeded time-wait socket is immediately destroyed and warning is printed. This limit exists to help prevent simple DoS attacks.</td>
</tr>
<tr>
<td>net.ipv4.tcp_no_metrics_save</td>
<td>1</td>
<td>Disable caching TCP metrics on connection close</td>
</tr>
<tr>
<td>net.ipv4.tcp_orphan_retries</td>
<td>0</td>
<td>Limit number of orphans, each orphan can eat up to 16M (max wmem) of unswappable memory</td>
</tr>
<tr>
<td>net.ipv4.tcp_rfc1337</td>
<td>1</td>
<td>Enable a fix for RFC1337 - time-wait assassination hazards in TCP</td>
</tr>
<tr>
<td>net.ipv4.tcp_rmem</td>
<td>10240 131072 33554432</td>
<td>Setting is min/default/max. Recommed increasing the Linux autotuning TCP buffer limit to 32MB</td>
</tr>
<tr>
<td> net.ipv4.tcp_wmem</td>
<td>10240 131072 33554432</td>
<td>Setting is min/default/max. Recommed increasing the Linux autotuning TCP buffer limit to 32MB</td>
</tr>
<tr>
<td>net.ipv4.tcp_sack</td>
<td>1</td>
<td>Enable select acknowledgments</td>
</tr>
<tr>
<td>net.ipv4.tcp_slow_start_after_idle</td>
<td>0</td>
<td>By default, TCP starts with a single small segment, gradually increasing it by one each time. This results in unnecessary slowness that impacts the start of every request.</td>
</tr>
<tr>
<td>net.ipv4.tcp_syncookies</td>
<td>0</td>	
<td>Many default Linux installations use SYN cookies to protect the system against malicious attacks that flood TCP SYN packets. The use of SYN cookies dramatically reduces network bandwidth, and can be triggered by a running Geode cluster.If your Geode cluster is otherwise protected against such attacks, disable SYN cookies to ensure that Geode network throughput is not affected.<br>NOTE: if SYN floods are an issue and SYN cookies can’t be disabled, try the following:<br>net.ipv4.tcp_max_syn_backlog="16384"<br> net.ipv4.tcp_synack_retries="1" <br>net.ipv4.tcp_max_orphans="400000"<br>
</td>
</tr>
<tr><td>net.ipv4.tcp_timestamps</td>
<td>1</td>
<td>Enable timestamps as defined in RFC1323:</td>
</tr>
<tr>
<td>net.ipv4.tcp_tw_recycle</td>
<td>1</td>
<td>This enables fast recycling of TIME_WAIT sockets. The default value is 0 (disabled). Should be used with caution with load balancers.</td>
</tr>
<tr>
<td>net.ipv4.tcp_tw_reuse</td>
<td>1</td>
<td>This allows reusing sockets in TIME_WAIT state for new connections when it is safe from protocol viewpoint. Default value is 0 (disabled). It is generally a safer alternative to tcp_tw_recycle. The tcp_tw_reuse setting is particularly useful in environments where numerous short connections are open and left in TIME_WAIT state, such as web servers and loadbalancers.</td>
</tr>
<tr>
<td>net.ipv4.tcp_window_scaling</td>
<td>1</td>	
<td>Turn on window scaling which can be an option to enlarge the transfer window:</td>
</tr>
</table>In addition, increasing the size of transmit queue can also help TCP throughput. Add the following command to /etc/rc.local to accomplish this./sbin/ifconfig eth0 txqueuelen 10000 NOTE: substitute the appropriate adapter name for eth0 in the above example.