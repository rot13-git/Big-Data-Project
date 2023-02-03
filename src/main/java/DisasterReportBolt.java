import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;
import utils.EncodeData;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class DisasterReportBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, JSONObject> tweets;
    public static final Logger LOG = LoggerFactory.getLogger(DisasterReportBolt.class);

    private boolean reported = false;
    private int limit;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.tweets = new HashMap<>();
    }

    public DisasterReportBolt(int limit){
        this.limit = limit;
    }

    @Override
    public void execute(Tuple tuple) {
        String res = tuple.getValue(0).toString();
        if(res.equals("finish") || tweets.size()==limit){
            if(!reported){
                LOG.info("SENDING DATA");
                reported = true;
                reportData();
            }
        }
        else{
            JSONObject tweet = new JSONObject(res);
            tweets.put(tweet.getString("id"),tweet);
        }
    }

    private void reportData() {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(Constants.POST_URL+"/test");

        String jsonMap = EncodeData.encodeJson(tweets);

        LOG.info(String.valueOf(tweets.size()));

        try{
            httppost.setEntity(new StringEntity(jsonMap));
        } catch (UnsupportedEncodingException unsupportedEncodingException) {
            unsupportedEncodingException.printStackTrace();
        }

        httppost.setHeader("Content-type", "application/json");


        //SEND REQUEST
        CloseableHttpResponse response = null;
        try{
            LOG.info("REQUEST SEND");
            response = httpClient.execute(httppost);
            response.close();
            System.out.println(response);

        } catch (ClientProtocolException clientProtocolException) {
            clientProtocolException.printStackTrace();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

        HttpEntity entity = response.getEntity();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

