import java.io.*;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex extends IntWritable implements Writable {

    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent = new Vector<>();     // the vertex neighbors

    public Vertex() {

    }

    public Vertex(short tag, long group) {

        this.tag = tag;
        this.group = group;
    }

    public Vertex(short tag, long group, long VID, Vector<Long> adjacent) {

        this.tag = tag;
        this.group = group;
        this.VID = VID;
        this.adjacent = adjacent;
    }

    /* ... */
    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeShort(this.tag);
        dataOutput.writeLong(this.group);
        dataOutput.writeLong(this.VID);
        for (Long adj : this.adjacent) {

            dataOutput.writeLong(adj);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.tag = dataInput.readShort();
        this.group = dataInput.readLong();
        this.VID = dataInput.readLong();
        this.adjacent = readDataFromVector(dataInput);
    }

    public static Vector<Long> readDataFromVector(DataInput in) throws IOException {

        Vector<Long> resultVector = new Vector<>();
        long number;
        int i = 1;

        while (i > 0) {

            try {

                if ((number = in.readLong()) != -1) {

                    resultVector.add(number);
                }
                else {

                    i = 0;
                }
            }
            catch (EOFException eof) {

                i = 0;
            }
        }
        return resultVector;
    }
}

public class Graph {

    public static void main(String[] args) throws Exception {

        Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        // ... First Map-Reduce job to read the graph
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setMapperClass(FirstMapper.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f0"));
        job1.waitForCompletion(true);

        for (short i = 0; i < 5; i++) {

            Job job2 = Job.getInstance();
            job2.setJobName("MyJob2");
            // ... Second Map-Reduce job to propagate the group number
            job2.setJarByClass(Graph.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setMapperClass(SecondMapper.class);
            job2.setReducerClass(SecondReducer.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f" + i));
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f" + (i + 1)));
            job2.waitForCompletion(true);
        }

        Job job3 = Job.getInstance();
        job3.setJobName("MyJob3");
        // ... Final Map-Reduce job to calculate the connected component sizes
        job3.setJarByClass(Graph.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setMapperClass(ThirdMapper.class);
        job3.setReducerClass(ThirdReducer.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        job3.waitForCompletion(true);
    }

    /* ... */
    public static class FirstMapper extends Mapper<Object, Text, LongWritable, Vertex> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            /* write your mapper code */

            //Reading data from the file
            String line = value.toString();
            String[] data = line.split(",");
            long vid = Long.parseLong(data[0]);
            Vector<Long> adjacentNodes = new Vector<>();
            for (int i = 1; i < data.length; i++) {

                adjacentNodes.add(Long.parseLong(data[i]));
            }

            context.write(new LongWritable(vid), new Vertex((short) 0, vid, vid, adjacentNodes));
        }
    }

    public static class SecondMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
        @Override
        public void map(LongWritable key, Vertex value, Context context)
                throws IOException, InterruptedException {

            /* write your mapper code */
            context.write(new LongWritable(value.VID), value);

            for (Long n : value.adjacent) {

                context.write(new LongWritable(n), new Vertex((short) 1, value.group));
            }
        }
    }

    public static class SecondReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
        @Override
        public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
                throws IOException, InterruptedException {

            /* write your reducer code */

            long m = Long.MAX_VALUE;

            Vector<Long> adj = new Vector<>();

            for (Vertex v : values) {

                if (v.tag == 0) {

                    adj = v.adjacent;
                }

                m = Math.min(m, v.group);
            }
            context.write(new LongWritable(m), new Vertex((short) 0, m, key.get(), adj));
        }
    }

    public static class ThirdMapper extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
        @Override
        public void map(LongWritable key, Vertex value, Context context)
                throws IOException, InterruptedException {

            /* write your mapper code */
            context.write(key, new LongWritable(1));
        }
    }

    public static class ThirdReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            /* write your reducer code */
            long m = 0;

            for (LongWritable v : values) {

                m += v.get();
            }

            context.write(key, new LongWritable(m));
        }
    }
}