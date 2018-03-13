/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterhashtagstorm;

import java.util.*;

import backtype.storm.tuple.Fields;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 *
 * @author SANDY
 */
public class TwitterHashtagStorm {

    long time;
    String keyword;

    public void initSetup() throws Exception {
        String consumerKey = "28LU9LF6bs7ZGeqA7PV5ZMHSW";
        String consumerSecret = "5dOOTFtMT227oEDmPLMtEdntuQGtOVDyj3UmZCJ2wFg1boaw4N";

        String accessToken = "138728642-o8oXcVQDc1ZrfETueT8k6Pog9fmvGRci8dted6sI";
        String accessTokenSecret = "qvQfeYylscGtLintHwuZBgMfmHjdALX2mq6yAvZ0rI50o";
        String[] arguments = {keyword};
        String[] keyWords = Arrays.copyOfRange(arguments, 0, arguments.length);

        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TwitterSampleSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keyWords));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config, builder.createTopology());
        Thread.sleep(time);
        cluster.shutdown();
    }
}
