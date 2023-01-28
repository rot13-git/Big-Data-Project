import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class DisasterSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private TopologyContext context;
    private FileReader fileReader;
    private String filePath;
    private BufferedReader bufferedReader;
    public DisasterSpout(String filePath){
        this.filePath=filePath;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try{
            this.fileReader = new FileReader(filePath);
            this.bufferedReader = new BufferedReader(fileReader);
        }catch (FileNotFoundException fileNotFoundException){
            fileNotFoundException.printStackTrace();
        }
        this.context = topologyContext;
        this.collector = spoutOutputCollector;

    }

    @Override
    public void nextTuple() {
        String entry;
        try {
            entry = bufferedReader.readLine();
            if(entry!=null){
                this.collector.emit("tweet_stream",new Values(entry));
            }else{
                this.collector.emit("tweet_stream",new Values("finish"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("tweet_stream",new Fields("tweet"));
    }
}
