import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class DisasterMainTopology {
    static final String topologyName = "Disaster";

    public static void main(String[] args){
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        Config config = new Config();

        //Add spout and bolt

        config.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS,10000);

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
