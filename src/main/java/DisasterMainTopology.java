import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.IOException;

public class DisasterMainTopology {
    static final String topologyName = "Disaster";

    public static void main(String[] args) throws IOException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        Config config = new Config();
        config.setDebug(false);
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setCurrentDirectory(new File(System.getProperty("user.home")));
        int result = fileChooser.showOpenDialog(null);
        String file = "";
        if(result == JFileChooser.APPROVE_OPTION){
            file = fileChooser.getSelectedFile().getPath();
        }
        //Add spout and bolt
        topologyBuilder.setSpout("disasterSpout",new DisasterSpout(file),3);

        topologyBuilder.setBolt("disasterBolt",new DisasterBolt(),8).shuffleGrouping("disasterSpout","tweet_stream");

        //topologyBuilder.setBolt("disasterLogBolt",new DisasterLogBolt(),1).globalGrouping("disasterBolt","tweet_stream");
        //topologyBuilder.setBolt("disasterReportBolt",new DisasterReportBolt(30),1).globalGrouping("disasterBolt","tweet_stream");
        topologyBuilder.setBolt("disasterRedisBolt",new DisasterRedisBolt(),1).globalGrouping("disasterBolt","tweet_stream");
        topologyBuilder.setBolt("disasterRedisBolt2",new DisasterRedisBolt(),1).globalGrouping("disasterBolt","top_hashtag");
        topologyBuilder.setBolt("disasterRedisBolt3",new DisasterRedisBolt(),1).globalGrouping("disasterBolt","time");

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
