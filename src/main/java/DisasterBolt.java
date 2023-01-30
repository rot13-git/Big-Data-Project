import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DisasterBolt extends BaseRichBolt {
    private StanfordCoreNLP stanfordCoreNLP;
    private OutputCollector outputCollector;
    private Properties properties;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        this.properties = new Properties();
        properties.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        this.stanfordCoreNLP = new StanfordCoreNLP(properties);
    }

    @Override
    public void execute(Tuple tuple) {
        Object o = tuple.getValue(0);
        if(o.toString().equals("finish")){
            System.out.println("FINITO");
        }
        else{
            Map ret = (Map) o;
            JSONObject tweet = new JSONObject(ret);

            String sentiment = sentimentAnalysis(tweet.getString("text"));
            System.out.println(sentiment);
        }
        //System.out.println(jsonObject.keySet().toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public String sentimentAnalysis(String text){
        String cleared_text = clearTweet(text);
        String sentimentName="";
        CoreDocument coreDocument = new CoreDocument(cleared_text);
        stanfordCoreNLP.annotate(coreDocument);
        List<CoreSentence> sentences = coreDocument.sentences();
        for(CoreSentence sentence : sentences) {
            sentimentName = sentence.sentiment();
        }
        return sentimentName;
    }

    private String clearTweet(String tweet) {
        tweet = tweet.replaceAll("((www\\.[^\\s]+)|(https?://[^\\s]+))", "URL");

        //remove user names
        tweet = tweet.replaceAll("@[^\\s]+", "ATUSER");

        //remove # from hash tag
        tweet = tweet.replaceAll("#", "");

        //remove punctuation
        tweet = tweet.replaceAll("\\p{Punct}+", "");

        return tweet;
    }
}
