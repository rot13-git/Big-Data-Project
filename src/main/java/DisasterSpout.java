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
import java.util.Random;

public class DisasterSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private TopologyContext context;
    private FileReader fileReader;
    private String filePath;
    private BufferedReader bufferedReader;
    private JsonReader reader;
    private Gson gson;
    private JsonStreamParser p;
    private int numberOfFile=0;
    public DisasterSpout(String filePath){
        this.filePath=filePath;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try{
            if(filePath.equals("")){
                this.numberOfFile = 1;
                this.filePath = "resources/dataset/170826213907_hurricane_harvey_2017_20170827_vol-1.json/170826213907_hurricane_harvey_2017_20170827_vol-1.json";
            }
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
            if(p.hasNext()){
                try{
                    JsonElement e = p.next();
                    if (e.isJsonObject()) {
                        String json = e.toString();
                        this.collector.emit("tweet_stream",new Values(json));
                    }
                }catch (RuntimeException r){
                }
            }
            else{
                System.out.println("FINITO");
                /*
                if(numberOfFile!=0){
                    try {
                        openNewFile();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                }
                */
            }
    }

    private void openNewFile() throws FileNotFoundException {
        if(numberOfFile<31){
            numberOfFile+=1;
            this.filePath = this.filePath.substring(0,this.filePath.length()-6)+numberOfFile+".json";
            this.fileReader = new FileReader(filePath);
            gson = new GsonBuilder().create();
            p = new JsonStreamParser(this.fileReader);
        }


    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("tweet_stream",new Fields("tweet"));
    }
}
