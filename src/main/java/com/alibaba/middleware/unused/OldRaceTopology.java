//package com.alibaba.middleware.unused;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.jstorm.PlatformDistinguish;
//import com.alibaba.middleware.race.jstorm.RatioWriter;
//import com.alibaba.middleware.race.jstorm.TBCounterWriter;
//import com.alibaba.middleware.race.jstorm.TMCounterWriter;
//import com.alibaba.middleware.unused.NewTBMinuteCounter;
//import com.alibaba.middleware.unused.NewTMMinuteCounter;
//
//import backtype.storm.Config;
//import backtype.storm.StormSubmitter;
//import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.tuple.Fields;
//
///**
// * 这是一个很简单的例子
// * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
// * 我们会将选手拓扑杀掉。
// */
//
///**
// * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
// * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
// * 所以这个主类路径一定要正确
// */
//public class OldRaceTopology {
//    private static Logger LOG = LoggerFactory.getLogger(OldRaceTopology.class);
//    /** Spout **/
//    private static final int AllSpoutParallelism = 1;
//    public static final String ALLSPOUT = "AllSpout";    
//    public static final String PAYMENTSTREAM = "PaymentStream";
//    public static final String TMTRADESTREAM = "TMTradeStream";
//    public static final String TBTRADESTREAM = "TBTradeStream";
//    
//    /** Platform Distinguish **/
//    private static final int PlatformParallelism = 1;
//    public static final String PLATFORMBOLT = "PlatformBolt";  
//    public static final String TMPAYSTREAM = "TMPayStream";
//    public static final String TBPAYSTREAM = "TBPayStream";
//    
//    /** Counter Bolt **/      
//    private static final int TMMinuteCounterParallelism = 1;
//    public static final String TMMINUTECOUNTERBOLT = "TMMinuteCounterBolt";    
//    public static final String TMPCCOUNTERSTREAM = "TMPCCounterStream";
//    public static final String TMWIRELESSSTREAM = "TMWirelessStream"; 
//    
//    private static final int TBMinuteCounterParallelism = 1;
//    public static final String TBMINUTECOUNTERBOLT = "TBMinuteCounterBolt";
//    public static final String TBPCCOUNTERSTREAM = "TBPCCounterStream";
//    public static final String TBWIRELESSSTREAM = "TBWirelessStream";
//    
////    private static final int PCSumCounterParallelism = 3;
////    public static final String PCSUMCOUNTERRBOLT = "PCSumWriterBolt";
//    
////    private static final int WirelessSumCounterParallelism = 3;
////    public static final String WIRELESSSUMCOUNTERBOLT = "WirelessSumBolt";
//    
//    /** Writer Bolt **/
//    private static final int TMCounterWriterParallelism = 1;
//    public static final String TMCOUNTERWRITERBOLT = "TMCounterWriter";
//    
//    private static final int TBCounterWriterParallelism = 1;
//    public static final String TBCOUNTERWRITERBOLT = "TBCounterWriter";
//    
//    private static final int RationCounterParallelism = 1;
//    public static final String RATIONWRITERBOLT = "RatioWriter";
//    
//    
//    
//    public static void main(String[] args){
//
//        TopologyBuilder builder = new TopologyBuilder();
//
//        /** Spout **/        
//        builder.setSpout(ALLSPOUT, new NewAllSpout(), AllSpoutParallelism);
//        
//        /** Platform Bolt **/
//        builder.setBolt(PLATFORMBOLT, new PlatformDistinguish(), PlatformParallelism)
//        	   .fieldsGrouping(ALLSPOUT, PAYMENTSTREAM, new Fields("orderID"))
//        	   .fieldsGrouping(ALLSPOUT, TMTRADESTREAM, new Fields("orderID"))
//        	   .fieldsGrouping(ALLSPOUT, TBTRADESTREAM, new Fields("orderID"));
//        
//        /** Counter Bolt **/
//        builder.setBolt(TMMINUTECOUNTERBOLT, new TMTerminalDistinguish(), TMMinuteCounterParallelism)
//        	   .shuffleGrouping(PLATFORMBOLT, TMPAYSTREAM);
//        builder.setBolt(TBMINUTECOUNTERBOLT, new TBTerminalDistinguish(), TBMinuteCounterParallelism)
//        	   .shuffleGrouping(PLATFORMBOLT, TBPAYSTREAM);
//        
////        builder.setBolt(PCSUMCOUNTERRBOLT, new NewPCSumCounter(), PCSumCounterParallelism)
////        	   .shuffleGrouping(TMMINUTECOUNTERBOLT, TMPCCOUNTERSTREAM)
////        	   .shuffleGrouping(TBMINUTECOUNTERBOLT, TBPCCOUNTERSTREAM);
////        builder.setBolt(WIRELESSSUMCOUNTERBOLT, new NewWirelessSumCounter(), WirelessSumCounterParallelism)
////        	   .shuffleGrouping(TMMINUTECOUNTERBOLT, TMWIRELESSSTREAM)
////        	   .shuffleGrouping(TBMINUTECOUNTERBOLT, TBWIRELESSSTREAM);
//        
//        /** Writer Bolt **/
//        builder.setBolt(TMCOUNTERWRITERBOLT, new TMCounterWriter(), TMCounterWriterParallelism)
// 	   		   .fieldsGrouping(TMMINUTECOUNTERBOLT, TMPCCOUNTERSTREAM, new Fields("key"))
// 	   		   .fieldsGrouping(TMMINUTECOUNTERBOLT, TMWIRELESSSTREAM, new Fields("key"));
//        builder.setBolt(TBCOUNTERWRITERBOLT, new TBCounterWriter(), TBCounterWriterParallelism)
// 	           .fieldsGrouping(TBMINUTECOUNTERBOLT, TBPCCOUNTERSTREAM, new Fields("key"))
// 	           .fieldsGrouping(TBMINUTECOUNTERBOLT, TBWIRELESSSTREAM, new Fields("key"));
//        
//        builder.setBolt(RATIONWRITERBOLT, new RatioWriter(), RationCounterParallelism)
//               .fieldsGrouping(TMMINUTECOUNTERBOLT, TMPCCOUNTERSTREAM, new Fields("key"))
//               .fieldsGrouping(TMMINUTECOUNTERBOLT, TMWIRELESSSTREAM, new Fields("key"))
//               .fieldsGrouping(TBMINUTECOUNTERBOLT, TBPCCOUNTERSTREAM, new Fields("key"))
//               .fieldsGrouping(TBMINUTECOUNTERBOLT, TBWIRELESSSTREAM, new Fields("key"));
//
//        
//        String topologyName = RaceConfig.JstormTopologyName;
//
//        Config conf = new Config();
//        conf.setNumWorkers(4);
//        conf.setNumAckers(0);
//        
////      conf.setMessageTimeoutSecs(90);
////        conf.setMaxSpoutPending(RaceConfig.SpoutMaxPending);
//        
//        try {
//            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }
//}