package me.dekimpe;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
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
        if (args.length < 4) {
            System.err.println("Number of arguments is not correct.");
            System.exit(1);
        }
        int year = Integer.parseInt(args[0]);
        int month = Integer.parseInt(args[1]);
        int day = Integer.parseInt(args[2]);
        int hour = Integer.parseInt(args[3]);
        String directory = "/topics/tweet/year=" + String.format("%04d", year) + "/month=" + String.format("%02d", month) + "/day=" + String.format("%02d", day) + "/hour=" + String.format("%02d", hour) + "/";
        
        // Get list of files from that period
        ArrayList<String> files = new ArrayList<>();
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(hdfsHost), conf);
            FileStatus[] fileStatus = fs.listStatus(new Path(hdfsHost + directory));
            for (FileStatus status : fileStatus) {
                System.out.println(status.getPath().toString());
                files.add(status.getPath().toString());
            }
        } catch (Exception e) {
            System.err.println(e);
        }
        
        SparkSession spark = SparkSession.builder()
                .appName("Spark Parsing XML - Session")
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
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        long from = cal.getTimeInMillis() / 1000l;
        cal.set(Calendar.HOUR_OF_DAY, hour + 1);
        long to = cal.getTimeInMillis() / 1000l;
        
        // Delete previous results stored in ElasticSearch from SpeedLayer
        try {
            deleteOlderResults(from, to);
        } catch (UnknownHostException e) {
            System.err.println(e);
        }
        
        // Save results
        String resultFilename = String.format("%04d", year) + "-" + String.format("%02d", month) + "-" + String.format("%02d", day) + "-" + String.format("%02d", hour);
        result.limit(10).write().mode(SaveMode.Overwrite).format("csv").option("header", "true").save("hdfs://hdfs-namenode:9000/output/Top10-Tweets-" + resultFilename + ".csv");
    }
    
    public static String[] GetStringArray(ArrayList<String> arr) 
    { 
        String str[] = new String[arr.size()]; 
        for (int j = 0; j < arr.size(); j++) { 
            str[j] = arr.get(j); 
        } 
        return str; 
    }
    
    public static void deleteOlderResults(long from, long to) throws UnknownHostException {
        // Create a connection to ES cluster
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        Settings settings = Settings.builder()
                .put("cluster.name", ElasticSearch.CLUSTER_NAME)
                .put("client.transport.sniff", "true").build();
        
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticSearch.HOST1), ElasticSearch.PORT))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticSearch.HOST2), ElasticSearch.PORT))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticSearch.HOST3), ElasticSearch.PORT));
        
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.rangeQuery("timestamp").lt(to).gte(from))
                .source("tweets-management")
                .get();
        long deleted = response.getDeleted();
        System.out.println("ElasticSearch Delete By Query : #" + deleted + " entries deleted.");
    }
}
