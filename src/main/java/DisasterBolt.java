import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
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
            this.outputCollector.emit("tweet_stream", new Values("finish"));
        }
        else{
            //Map ret = (Map) o;
            //READ TWEET FROM SPOUT
            String ret = (String) o;
            JSONObject tweet = new JSONObject(ret);

            //EXECUTE SENTIMENT ANALYSIS
            String sentiment = sentimentAnalysis(tweet.getString("text"));
            tweet.put("sentiment",sentiment);

            //SEND TO OUTPUT BOLT
            this.outputCollector.emit("tweet_stream", new Values(tweet.toString()));

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("tweet_stream",new Fields("tweet"));
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
