package gdelt.analysis.spark.job;

import gdelt.analysis.common.data.GDELTEvent;
import gdelt.analysis.spark.scala.operations.GDELTInputReceiver;
//import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Tuple3;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author behrouz
 */
public class SparkJavaJob {
    public static void main(String[] args) throws InterruptedException {

	final String pathToGDELT = args[0];
	final String outputPath = args[1];
	final Integer duration = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf().setAppName("Spark Java GDELT Analyzer");
        String masterURL = conf.get("spark.master", "local[*]");
        conf.setMaster(masterURL);

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(duration));
        // checkpoint for storing the state
        jssc.checkpoint("checkpoint/");

        // function to store intermediate values in the state
        
	jssc
          .receiverStream(new GDELTInputReceiver(pathToGDELT))
	  .mapToPair(new PairFunction<GDELTEvent, String, Double>() {
              @Override
              public Tuple2<String,Double> call(GDELTEvent gdeltEvent) throws Exception {
                  return new Tuple2<>(gdeltEvent.actor1Code_countryCode, gdeltEvent.avgTone);
              }
          }).collect()
          .dstream().saveAsTextFiles(outputPath,"");
          //.print();

	jssc.start();
	jssc.awaitTermination();
    }
}

