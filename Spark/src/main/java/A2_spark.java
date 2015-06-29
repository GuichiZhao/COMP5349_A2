import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by fish on 15/6/1.
 */
public class Assignment2_spark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Testing Dataset");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Test Input data
//        JavaRDD<String> userTimeCountry = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/user/gzha1710/testInput");

        // Input Data
        JavaRDD<String> placeData = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/place.txt");


// all files path
        JavaRDD<String>
         photoData00p2 = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n00p2.txt"),
         photoData0 = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n00.txt"),
                photoData1 = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n01.txt")
                , photoData2 = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n02.txt")
        , photoData3 = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n03.txt")
                , photoData4 = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n04.txt")
        , photoData5 = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n05.txt")
        , photoData6 = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n06.txt")
                , photoData7 = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n07.txt")
                , photoData8 = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n08.txt")
        ;


//  Single  photo input data              ,
                JavaRDD<String>
                        photoData = photoData5;

// sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n05.txt");

// Combination photo input data
//        // 2 GB
//        JavaRDD<String>
//                photoData = photoData1.union(photoData5).union(photoData6).union(photoData7).union(photoData8);

//        // 4GB
//        JavaRDD<String>
//                photoData = photoData1.union(photoData2).union(photoData4).union(photoData5).union(photoData6).union(photoData00p2);
//
//        // 6GB
//        JavaRDD<String>
//                photoData = photoData1.union(photoData0).union(photoData2).union(photoData5);
//
//        // 8GB
//        JavaRDD<String>
//                photoData = photoData1.union(photoData4).union(photoData0).union(photoData00p2);
//
//        // 10GB
//        JavaRDD<String>
//                photoData = sc.textFile("hdfs://ip-10-171-118-84.ec2.internal:8020/share/large/n*");



        // Output data
        String dir = "hdfs://ip-10-171-118-84.ec2.internal:8020/user/xjia9128/SparkAssignment/";

        //placeId, (userId, date-taken)
        JavaPairRDD<String, String> photoExtraction = photoData.mapToPair(
                s-> {
                    String[] parts = s.split("\t");

                    String placeId = parts[4];
                    String userId = parts[1];
                    String dateTaken = parts[3];

                    String value = userId + "\t" + dateTaken;

                    return new Tuple2<String, String>(placeId, value);


                }
        );

        //placeId, country
        JavaPairRDD<String, String> placeExtraction = placeData.mapToPair(
                s->{
                    String[] parts = s.split("\t");
                    int length = parts.length;

                    String placeId = parts[0];

                    String url = parts[length-1];
//                    String[] parts1 = url.split("\\/");
//                    String country = parts1[1];

                    return new Tuple2<String, String>(placeId, url);
                }
        );

        // placeId, ((userId    date-taken), url)
        JavaPairRDD<String, Tuple2<String,String>> joinResults = photoExtraction.join(placeExtraction);

//        joinResults.saveAsTextFile(dir + "joinResults");

        // (userId date-taken), url
        JavaPairRDD<String,String> tempSampleData = joinResults.values().mapToPair(
                v->v
        );

//        tempSampleData.saveAsTextFile(dir + "tempSampleData");


        // userId-> time + "/t" + Country
        JavaPairRDD<String, String> sampleData = tempSampleData.mapToPair(
          s-> {
              String[] parts = s._1().split("\t");
              String userId = parts[0];

//              String time = parts[1];


              String[] parts1 = s._2().split("\\/");
              String timeCountry = parts[1] + "\t" + parts1[1];

              return new Tuple2<String, String>(userId, timeCountry);
          }
        );

//        sampleData.saveAsTextFile(dir + "sampleData");

        // userId-> list [time+ "\t" +Country,....]
        JavaPairRDD<String, Iterable<String>> userValueList = sampleData.groupByKey(1);

//          userValueList.saveAsTextFile(dir + "userValueList");

        JavaPairRDD<String, String> computeTimeSpent = userValueList.mapToPair(
                s -> {
                    Map<String, List<Double>> countryDuration = new HashMap();

                    String userId = s._1();
                    //"time   country"
                    List<String> list = new ArrayList<String>();
//                    while (s._2().iterator().hasNext()) {
//                        list.add(s._2().iterator().next());
//                    }

                    for (String ss : s._2()) {
                        list.add(ss);
                    }

                    Collections.sort(list);

//                    List<String> timeList = null;
//                    List<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();
                    String firstElem = list.get(0);

                    String last_country = "";
                    String last_end_time = "";
                    String last_start_time = firstElem.split("\t")[0];

                    for (String elem : list) {

                        // split time country
                        String[] parts = elem.split("\t");
                        String time = parts[0];
                        String country = parts[1];
//
//
//                        //start a new country
                        if (!country.contains(last_country) && last_country != "") {

                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            Date dateTime = format.parse(time);
                            Date dateLast_end_time = format.parse(last_start_time);

                            long diff = dateTime.getTime() - dateLast_end_time.getTime();
                            long diffSeconds = diff / 1000 % 60;
                            long diffMinutes = diff / (60 * 1000) % 60;
                            long diffHours = diff / (60 * 60 * 1000);
                            double diffInDays = ((dateTime.getTime() - dateLast_end_time.getTime()) /(double)(1000 * 60 * 60 * 24));
                            diffInDays = Math.round(diffInDays * 10.0) / 10.0;

                            //String resultString=last_start_time+"\t"+last_end_time+"\t"+last_country;
                            String resultString = last_country + "\t" + diffInDays;


                            double duration = diffInDays;

                            if (!countryDuration.containsKey(last_country)) {
                                ArrayList<Double> durations = new ArrayList<Double>();

                                durations.add(duration);
                                countryDuration.put(last_country, durations);
                            } else {
                                countryDuration.get(last_country).add(duration);
                            }


//                            result.add(new Tuple2<String, String>(userId, resultString));
                            // result.add(new Tuple2<String,String>(userId, "Hello"));
                            //clear the string
                            resultString = "";
                            last_start_time = time;
                            last_country = country;


                        }
//                        //same country
                        else {
                            last_end_time = time;
                            last_country = country;

                        }


                    }
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date dateTime = format.parse(last_start_time);
                    Date dateLast_end_time = format.parse(last_end_time);

                    long diff = dateLast_end_time.getTime() - dateTime.getTime();
                    long diffSeconds = diff / 1000 % 60;
                    long diffMinutes = diff / (60 * 1000) % 60;
                    long diffHours = diff / (60 * 60 * 1000);
                    double diffInDays =  ((dateLast_end_time.getTime() - dateTime.getTime()) /(double)(1000 * 60 * 60 * 24));

                    diffInDays = Math.round(diffInDays * 10.0) / 10.0;
//                    result.add(new Tuple2<String, String>(userId, last_country + "\t" + diffInDays));

                    double duration = diffInDays;
                    if (!countryDuration.containsKey(last_country)) {
                        ArrayList<Double> durations = new ArrayList<Double>();

                        durations.add(duration);
                        countryDuration.put(last_country, durations);
                    } else {
                        countryDuration.get(last_country).add(duration);
                    }


                    String returnResult = "";

                    for (String c : countryDuration.keySet()) {
                        List<Double> durationList = countryDuration.get(c);
                        int timeVisit = durationList.size();
                        Collections.sort(durationList);

                        double minDuration = durationList.get(0);
                        double maxDuration = durationList.get(timeVisit - 1);

                        double total = 0.0;
                        for (double db : durationList) {
                            total += db;

                        }



                        double average = total / timeVisit;
                        average = Math.round(average * 10.0) /10.0;

                        String countryInfo = c + "(" + timeVisit + "," + maxDuration + "," + minDuration +
                                "," + average + "," + total + "), ";

                        returnResult += countryInfo;

                        returnResult = returnResult.substring(0,returnResult.length()-2);

                    }

                    //String lastElem=list.get(list.size()-1);

//                    return result;
                    return new Tuple2<String, String>(userId, returnResult);


                }
        );

        computeTimeSpent.saveAsTextFile(dir + "computeTimeSpent");




    }
}
