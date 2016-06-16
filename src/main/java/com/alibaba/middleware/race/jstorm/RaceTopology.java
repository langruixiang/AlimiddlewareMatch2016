package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.bolt.TMCounterWriter;
import com.alibaba.middleware.race.bolt.PCSumCounter;
import com.alibaba.middleware.race.bolt.RatioWriter;
import com.alibaba.middleware.race.bolt.TBCounterWriter;
import com.alibaba.middleware.race.bolt.TBMinuteCounter;
import com.alibaba.middleware.race.bolt.TMMinuteCounter;
import com.alibaba.middleware.race.bolt.WirelessSumCounter;
import com.alibaba.middleware.race.spout.PaySpout;
import com.alibaba.middleware.race.spout.TBTradeSpout;
import com.alibaba.middleware.race.spout.TMTradeSpout;

import org.codehaus.plexus.archiver.tar.TarArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {
    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);
    /** Spout **/
    private static final int TMTradeSpoutParallelism = 1;
    public static final String TMTRADESPOUT = "TMTradeSpout";
    
    private static final int TBTreadeSpoutParallelism = 1;
    public static final String TBTRADESPOUT = "TBTradeSpout";
    
    private static final int PaySpoutParallelism = 1;
    public static final String PAYSPOUT = "PaySpout";
    
    /** Counter Bolt **/
    private static final int TMMinuteCounterParallelism = 2;
    public static final String TMMINUTECOUNTERBOLT = "TMMinuteCounterBolt";    
    public static final String TMPCCOUNTERSTREAM = "TMPCCounterStream";
    public static final String TMWIRELESSSTREAM = "TMWirelessStream"; 
    
    private static final int TBMinuteCounterParallelism = 2;
    public static final String TBMINUTECOUNTERBOLT = "TBMinuteCounterBolt";
    public static final String TBPCCOUNTERSTREAM = "TBPCCounterStream";
    public static final String TBWIRELESSSTREAM = "TBWirelessStream";
    
    private static final int PCSumCounterParallelism = 1;
    public static final String PCSUMCOUNTERRBOLT = "PCSumWriterBolt";
    
    private static final int WirelessSumCounterParallelism = 1;
    public static final String WIRELESSSUMCOUNTERBOLT = "WirelessSumBolt";
    
    /** Writer Bolt **/
    private static final int TMCounterWriterParallelism = 1;
    public static final String TMCOUNTERWRITERBOLT = "TMCounterWriter";
    
    private static final int TBCounterWriterParallelism = 1;
    public static final String TBCOUNTERWRITERBOLT = "TBCounterWriter";
    
    private static final int RationCounterParallelism = 1;
    public static final String RATIONWRITERBOLT = "RatioWriter";
    
    
    
    public static void main(String[] args){

        TopologyBuilder builder = new TopologyBuilder();

        /** Spout **/
        builder.setSpout(TMTRADESPOUT, new TMTradeSpout(), TMTradeSpoutParallelism);
        builder.setSpout(TBTRADESPOUT, new TBTradeSpout(), TBTreadeSpoutParallelism);
        builder.setSpout(PAYSPOUT, new PaySpout(), PaySpoutParallelism);
        
        /** Counter Bolt **/
        builder.setBolt(TMMINUTECOUNTERBOLT, new TMMinuteCounter(), TMMinuteCounterParallelism)
        	   .fieldsGrouping(TMTRADESPOUT, new Fields("orderID"))
        	   .fieldsGrouping(PAYSPOUT, new Fields("orderID"));
        builder.setBolt(TBMINUTECOUNTERBOLT, new TBMinuteCounter(), TBMinuteCounterParallelism)
        	   .fieldsGrouping(TBTRADESPOUT, new Fields("orderID"))
        	   .fieldsGrouping(PAYSPOUT, new Fields("orderID"));
        
        builder.setBolt(PCSUMCOUNTERRBOLT, new PCSumCounter(), PCSumCounterParallelism)
        	   .globalGrouping(TMMINUTECOUNTERBOLT, TMPCCOUNTERSTREAM)
        	   .globalGrouping(TBMINUTECOUNTERBOLT, TBPCCOUNTERSTREAM);
        builder.setBolt(WIRELESSSUMCOUNTERBOLT, new WirelessSumCounter(), WirelessSumCounterParallelism)
        	   .globalGrouping(TMMINUTECOUNTERBOLT, TMWIRELESSSTREAM)
        	   .globalGrouping(TBMINUTECOUNTERBOLT, TBWIRELESSSTREAM);
        /** Writer Bolt **/
        builder.setBolt(TMCOUNTERWRITERBOLT, new TMCounterWriter(), TMCounterWriterParallelism)
 	   		   .globalGrouping(TMMINUTECOUNTERBOLT, TMPCCOUNTERSTREAM)
 	   		   .globalGrouping(TMMINUTECOUNTERBOLT, TMWIRELESSSTREAM);
        builder.setBolt(TBCOUNTERWRITERBOLT, new TBCounterWriter(), TBCounterWriterParallelism)
 	           .globalGrouping(TBMINUTECOUNTERBOLT, TBPCCOUNTERSTREAM)
 	           .globalGrouping(TBMINUTECOUNTERBOLT, TBWIRELESSSTREAM);
        
        builder.setBolt(RATIONWRITERBOLT, new RatioWriter(), RationCounterParallelism)
        	   .globalGrouping(PCSUMCOUNTERRBOLT)
        	   .globalGrouping(WIRELESSSUMCOUNTERBOLT);
        
        String topologyName = RaceConfig.JstormTopologyName;

        Config conf = new Config();
        conf.setNumWorkers(4);
        
        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}