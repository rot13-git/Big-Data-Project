import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class DisasterMainTopology {
    static final String topologyName = "Disaster";

    public static void main(String[] args){
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        Config config = new Config();
        config.setDebug(false);

        //Add spout and bolt
        topologyBuilder.setSpout("disasterSpout",new DisasterSpout(
                "resources/drive-download-20221212T122415Z-001/170826213907_hurricane_harvey_2017_20170827_vol-2.json/170826213907_hurricane_harvey_2017_20170827_vol-2.json"
                ),3);
        //topologyBuilder.setBolt("disasterPrintBolt",new DisasterPrintBolt(),4).shuffleGrouping("disasterSpout", "tweet_stream");
        topologyBuilder.setBolt("disasterBolt",new DisasterBolt(),8).shuffleGrouping("disasterSpout","tweet_stream");
        //topologyBuilder.setBolt("disasterLogBolt",new DisasterLogBolt(),1).globalGrouping("disasterBolt","tweet_stream");
        topologyBuilder.setBolt("disasterReportBolt",new DisasterReportBolt(30),1).globalGrouping("disasterBolt","tweet_stream");
        //config.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS,20000);
        //config.setMaxTaskParallelism(3);
        try (LocalCluster localCluster = new LocalCluster()){
            localCluster.submitTopology(topologyName,config,topologyBuilder.createTopology());
            Utils.sleep(600000);
            localCluster.killTopology(topologyName);
            localCluster.shutdown();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
