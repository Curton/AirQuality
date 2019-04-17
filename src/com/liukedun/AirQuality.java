package com.liukedun;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AirQuality {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: AirQuality <in> [<in>...] <out>");
            System.out.println(System.getProperty("user.dir"));
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Air Quality");
        job.setJarByClass(AirQuality.class);
        job.setMapperClass(AirQualityMapper.class);
        job.setReducerClass(AirQualityReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputDirRecursive(job, true);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class AirQualityMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String date = itr.nextToken();
            // get the value of month
            String month = date.substring(4, 6);
            // just need the data of November
            if (!month.equals("11")) {
                return;
            }
            // get the value of day
            String[] dateSplit = date.split(":");
            String day = dateSplit[1];
            // count the number of corresponding air quality level in a day
            AirQualityByDay airQualityByDay = new AirQualityByDay();
            while (itr.hasMoreTokens()) {
                String itrVal = itr.nextToken();
                airQualityByDay.add1(itrVal);
            }
            // each day in November as a Key, and the number of each levels in a day as a Value
            context.write(new Text(day), new Text(airQualityByDay.get()));
        }

        // for record the air quality levels in a day
        class AirQualityByDay {
            // number of corresponding record in a day
            int good = 0;
            int moderate = 0;
            int unhealthyForSensitiveGroup = 0;
            int unhealthy = 0;
            int veryUnhealthy = 0;
            int hazardous = 0;
            int invalid = 0;
            int numberOfDays = 0;

            // increase 1 to the corresponding counter
            public void add1(String level) {
                numberOfDays++;
                switch (level) {
                    case "Good":
                        good++;
                        break;
                    case "Moderate":
                        moderate++;
                        break;
                    case "Unhealthy_for_Sensitive_Group":
                        unhealthyForSensitiveGroup++;
                        break;
                    case "Unhealthy":
                        unhealthy++;
                        break;
                    case "Very Unhealthy":
                        veryUnhealthy++;
                        break;
                    case "Hazardous":
                        hazardous++;
                        break;
                    default:
                        invalid++;
                        break;
                }
            }

            // return all values as a string
            public String get() {
                return numberOfDays + ":" + good + ":" + moderate + ":" + unhealthyForSensitiveGroup + ":" + unhealthy + ":" + veryUnhealthy + ":" + hazardous + ":" + invalid;
            }
        }
    }

    public static class AirQualityReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // number of corresponding record in a day in the history of November
            int good = 0;
            int moderate = 0;
            int unhealthyForSensitiveGroup = 0;
            int unhealthy = 0;
            int veryUnhealthy = 0;
            int hazardous = 0;
            int invalid = 0;
            int numberOfDays = 0;
            String string = "";

            // read all values from Mapper
            for (Text val : values) {
                string = val.toString();
                // sum up the record of a day
                String[] stringSplit = string.split(":");
                numberOfDays += Integer.parseInt(stringSplit[0]);
                good += Integer.parseInt(stringSplit[1]);
                moderate += Integer.parseInt(stringSplit[2]);
                unhealthyForSensitiveGroup += Integer.parseInt(stringSplit[3]);
                unhealthy += Integer.parseInt(stringSplit[4]);
                veryUnhealthy += Integer.parseInt(stringSplit[5]);
                hazardous += Integer.parseInt(stringSplit[6]);
                invalid += Integer.parseInt(stringSplit[7]);
                //System.err.println(val);
            }

            // predict the air quality
            // get the maximum number of occurrence level in the history
            class maxLevel {
                String maxLevelName = "";
                int max = 0;
                int total;

                maxLevel(int total) {
                    this.total = total;
                }

                public int setMax(int val, String name) {
                    max = val;
                    maxLevelName = name;
                    return val;
                }

                public String getMax() {
                    // keep 2 decimals
                    DecimalFormat df = new DecimalFormat("#.00");
                    // calculate probability
                    String s = maxLevelName + " :   " + df.format(((double) max / total) * 100) + "%";
                    return s;
                }
            }

            maxLevel maxLevel = new maxLevel(numberOfDays);
            int max = good;
            maxLevel.setMax(good, "Good");
            max = max > moderate ? max : maxLevel.setMax(moderate, "Moderate");
            max = max > unhealthyForSensitiveGroup ? max : maxLevel.setMax(unhealthyForSensitiveGroup, "Unhealthy For Sensitive Group");
            max = max > unhealthy ? max : maxLevel.setMax(unhealthy, "Unhealthy");
            max = max > veryUnhealthy ? max : maxLevel.setMax(veryUnhealthy, "Very Unhealthy");
            max = max > hazardous ? max : maxLevel.setMax(hazardous, "Hazardous");
            max = max > invalid ? max : maxLevel.setMax(invalid, "Invalid data");


            context.write(new Text(key.toString()), new Text(maxLevel.getMax()));
        }
    }
}
