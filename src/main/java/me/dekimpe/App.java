package me.dekimpe;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.ArrayList;
import me.dekimpe.config.ElasticSearch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.explode;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        // Important Variables
        final String hdfsHost = "hdfs://hdfs-namenode:9000";
        
        // Arguments Management
        int year = 0, month = 0, day = 0, hour = 0;
        Calendar cal = Calendar.getInstance();
        if (args.length == 0) {
            ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
            year = now.getYear();
            month = now.getMonthValue();
            day = now.getDayOfMonth();
            hour = now.getHour();
        } else if (args.length == 4) {
            year = Integer.parseInt(args[0]);
            month = Integer.parseInt(args[1]);
            day = Integer.parseInt(args[2]);
            hour = Integer.parseInt(args[3]);
        } else {
            System.err.println("Number of arguments is not correct.");
            System.exit(1);
        }
        
        // Get Directory to get Avro Files from
        String directory = "/topics/tweets/year=" + String.format("%04d", year) +
                "/month=" + String.format("%02d", month) +
                "/day=" + String.format("%02d", day) +
                "/hour=" + String.format("%02d", hour) + "/";
        
        // Get list of files from that period
        ArrayList<String> files = new ArrayList<>();
        try {
            String fileName;
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(hdfsHost), conf);
            FileStatus[] fileStatus = fs.listStatus(new Path(hdfsHost + directory));
            for (FileStatus status : fileStatus) {
                fileName = status.getPath().toString();
                if(fileName.contains(".avro")) {
                    files.add(status.getPath().toString());
                }
            }
            if(files.size() == 0) {
                System.err.println("No files in the directory. Check the date you provided : " + directory);
                System.exit(2);
            }
        } catch (Exception e) {
            System.err.println(e);
        }
        
        SparkSession spark = SparkSession.builder()
                .appName("Spark Top 10 Tweets")
                .master("spark://192.168.10.14:7077")
                .config("spark.executor.memory", "1g")
                .getOrCreate();
        
        Dataset<Row> hashtags = spark.read()
                .format("avro")
                .load(GetStringArray(files));
        
        // Explode Dataset into multiple lines for each hashtags find 
        Dataset<Row> expanded = hashtags.withColumn("hashtag", explode(hashtags.col("hashtags"))).drop(hashtags.col("hashtags"));
        Dataset<Row> result = expanded.groupBy("hashtag").agg(count("*").as("NumberOfHashtags")).cache();
        result = result.orderBy(result.col("NumberOfHashtags").desc()).cache();
        result.show(10);
        
        // Get timestamps to get the data to delete from ElasticSearch
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.add(Calendar.HOUR, 2);
        long from = cal.getTimeInMillis() / 1000l;
        cal.add(Calendar.HOUR, 1);
        long to = cal.getTimeInMillis() / 1000l;      
        
        // Delete previous results stored in ElasticSearch from SpeedLayer
        // Create a connection to ES cluster
        try {
            System.setProperty("es.set.netty.runtime.available.processors", "false");
            Settings settings = Settings.builder()
                    .put("cluster.name", ElasticSearch.CLUSTER_NAME)
                    .put("client.transport.sniff", "true").build();
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticSearch.HOST1), ElasticSearch.PORT))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticSearch.HOST2), ElasticSearch.PORT))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticSearch.HOST3), ElasticSearch.PORT));
            deleteOlderResults(client, from, to);
            //saveNewResults(client, result);
        } catch (UnknownHostException e) {
            System.err.println(e);
        }
        
        // Save results
        String resultFilename = String.format("%04d", year) + "-" + String.format("%02d", month) + "-" + String.format("%02d", day) + "-" + String.format("%02d", hour);
        result.limit(10).write().mode(SaveMode.Overwrite).format("csv").option("header", "true").save("hdfs://hdfs-namenode:9000/output/Top10-Tweets/" + resultFilename + ".csv");
    }
    
    public static String[] GetStringArray(ArrayList<String> arr) 
    { 
        String str[] = new String[arr.size()]; 
        for (int j = 0; j < arr.size(); j++) { 
            str[j] = arr.get(j); 
        } 
        return str; 
    }
    
    public static void deleteOlderResults(TransportClient client, long from, long to) throws UnknownHostException {
        
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                //.filter(QueryBuilders.termQuery("_type", "hashtags"))
                .filter(QueryBuilders.rangeQuery("timestamp").lt(to).gte(from))
                .source("tweets-management")
                .get();
        
        // Debugging
        long deleted = response.getDeleted();
        System.out.println("ElasticSearch Delete By Query : #" + deleted + " entries deleted.");
    }
    
    /*public static void saveNewResults(TransportClient client, Dataset<Row> results) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        results.toJavaRDD().foreach((new VoidFunction<Row>() {
            public void call(Row r) throws Exception {
                //json = "{\"timestamp\": " + r.get
                bulkRequest.add(client.prepareIndex(ElasticSearch.INDEX, "hashtags")
                    .setSource(json, XContentType.JSON));
            }
        }));
        /*
        results.foreach(row -> bulkRequest.add(client.prepareIndex(ElasticSearch.INDEX, "results").setSource(json, XContentType.JSON)));
        for (Tuple input : window.get()) {
            Tweet tweet = (Tweet) input.getValueByField("tweet");
            for (String hashtag : tweet.getHashtags()) {
                json = "{\"timestamp\": " + tweet.getTimestamp() + ", \"hashtag\": \"" + hashtag + "\"}";
                bulkRequest.add(client.prepareIndex(ElasticSearch.INDEX, "hashtags")
                    .setSource(json, XContentType.JSON));
            }
        }
    }*/
}
