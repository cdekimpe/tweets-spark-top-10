package me.dekimpe;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
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
                .config("spark.executor.memory", "4g")
                .getOrCreate();
        
        Dataset<Row> hashtags = spark.read()
                .format("avro")
                .load(GetStringArray(files)); //, stubPath + "stub-6.avsc"
        
        Dataset<Row> result = hashtags.select(hashtags.col("hashtag"), explode(hashtags.col("hashtags"))).groupBy("col.hashtags").agg(count("*").as("NumberOfHashtags"));
        result.show();
        result = result.orderBy(result.col("NumberOfHashtags").desc()).cache();
        result.show();
        
        /*Dataset<Row> joined = pagelinks.join(revisions, pagelinks.col("pl_id").equalTo(revisions.col("id")), "outer").where("pl_title = '" + subject + "' or title = '" + subject + "'").cache();
        Dataset<Row> exploded = joined.select(joined.col("pl_id"), explode(joined.col("revision"))).groupBy("col.contributor.username").agg(count("*").as("NumberOfRevisions"));
        Dataset<Row> result = exploded.orderBy(exploded.col("NumberOfRevisions").desc()).cache();
        
        
        result.show();
        result.write().mode(SaveMode.Overwrite).format("csv").option("header", "true").save("hdfs://hdfs-namenode:9000/output/" + args[0] + ".csv");*/
    }
    
    public static String[] GetStringArray(ArrayList<String> arr) 
    { 
  
        // declaration and initialise String Array 
        String str[] = new String[arr.size()]; 
  
        // ArrayList to Array Conversion 
        for (int j = 0; j < arr.size(); j++) { 
            
            // Assign each value to String array 
            str[j] = arr.get(j); 
        } 
  
        return str; 
    } 
}
