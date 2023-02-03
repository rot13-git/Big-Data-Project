import com.google.gson.*;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.stream.JsonReader;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

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
    private JsonReader reader;
    private Gson gson;
    private JsonStreamParser p;
    public DisasterSpout(String filePath){
        this.filePath=filePath;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try{

            this.fileReader = new FileReader(filePath);
            gson = new GsonBuilder().create();
            p = new JsonStreamParser(this.fileReader);
            System.out.println("FATTO");
        }catch (FileNotFoundException fileNotFoundException){
            fileNotFoundException.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.context = topologyContext;
        this.collector = spoutOutputCollector;

    }

    @Override
    public void nextTuple() {
            //Utils.sleep(10);
            if(p.hasNext()){
                try{
                    JsonElement e = p.next();
                    if (e.isJsonObject()) {
                        //LinkedTreeMap m = gson.fromJson(e, LinkedTreeMap.class);
                        String json = e.toString();
                        this.collector.emit("tweet_stream",new Values(json));
                        //System.out.println("[+] NUOVO TWEET");
                        //System.out.println(m.get("retweeted"));
                    }

                }catch (RuntimeException r){
                    this.collector.emit("tweet_stream",new Values("finish"));
                    //r.printStackTrace();
                }


                //this.collector.emit("tweet_stream",new Values(entry));
            }else{
                this.collector.emit("tweet_stream",new Values("finish"));
            }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("tweet_stream",new Fields("tweet"));
    }
}
