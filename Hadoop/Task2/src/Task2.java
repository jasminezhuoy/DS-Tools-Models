import java.io.IOException;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;

import java.util.*;

public class Task2 extends Configured implements Tool {

    static int printUsage() {
        System.out.println("task1 [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class Task2Mapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String nextLine = value.toString();

            String [] columns = nextLine.split(",");

            if (columns.length == 11 && !columns[0].equals("medallion"))
            {
                String taxi = columns[1];

                context.write(new Text(taxi), new DoubleWritable(Double.parseDouble(columns[10])));
            }
        }
    }

    public static class Task2Reducer extends Reducer<Text, DoubleWritable, Text,DoubleWritable> {
        //Pair
        static class Pair{
            Pair(String taxi, double revenue){
                this.taxi = taxi;
                this.revenue = revenue;
            }
            String taxi;
            Double revenue;
        }

        //Priority Queue
        private Queue<Pair> top5 = new PriorityQueue<Pair>(new Comparator<Pair>() {
            public int compare(Pair o1, Pair o2) {
                return o1.revenue.compareTo(o2.revenue);
            }
        });

        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) {

            double sum = 0.0;
            String taxi = key.toString();

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            Pair taxicurrent = new Pair(taxi, sum);
            if(top5.size() < 5){
                top5.add(taxicurrent);
            }else{
                Iterator<Pair> iterator = top5.iterator();
                while (iterator.hasNext()){
                    Pair taxi1 =  iterator.next();
                    if(taxi1.revenue < sum){
                        top5.remove(taxi1);
                        top5.add(taxicurrent);
                        break;
                    }
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            Iterator<Pair> iterator = top5.iterator();
            while (!top5.isEmpty()){
                Pair pair = top5.poll();
                context.write(new Text(pair.taxi),new DoubleWritable(pair.revenue));
            }
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task2");
        job.setJarByClass(Task2.class);
        job.setMapperClass(Task2Mapper.class);
        job.setCombinerClass(Task2Reducer.class);
        job.setReducerClass(Task2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-r".equals(args[i])) {
                    job.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i-1]);
                return printUsage();
            }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 2.");
            return printUsage();
        }
        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task2(), args);
        System.exit(res);
    }
}