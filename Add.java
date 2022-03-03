import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Elem implements Writable {
    public short tag;  
    public int index;  
    public double value;
    
    Elem () {}

    Elem (short tag, int index, double value){
        this.tag = tag;
        this.index = index;
        this.value = value;
    }
    @Override
    public void write (DataOutput out) throws IOException {
        out.writeShort(this.tag);
        out.writeInt(this.index);
        out.writeDouble(this.value);
    }
    @Override
    public void readFields (DataInput in) throws IOException {
        this.tag = in.readShort();
        this.index = in.readInt();
        this.value = in.readDouble();
    }
    
    public String toString () {
        return String.valueOf(tag);
    }
  }

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;

    Pair () {}
    Pair ( int i, int j ) { 
	this.i = i; 
	this.j = j; 
	}

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(this.i);
        out.writeInt(this.j);
    }

    public void readFields ( DataInput in ) throws IOException {
        this.i = in.readInt();
        this.j = in.readInt();
        
    }

    public int compareTo ( Pair p){
        int cmp = Integer.compare(i, p.i);
        if (cmp != 0){
            return cmp;
        }
        cmp = Integer.compare(j, p.j);
        return cmp;

    }

    public String toString () { 

        return String.valueOf(i);
    }
}

public class Add extends Configured implements Tool {
    public static class MatrixOneMapper extends Mapper<Object,Text,IntWritable,Elem > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            short tag = 0;
            Elem e = new Elem (tag, i, v);
            context.write(new IntWritable(j), e);
            s.close();
        }
    }

    

    public static class MatrixTwoMapper extends Mapper<Object,Text,IntWritable,Elem > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            short tag = 1;
            Elem e = new Elem (tag, i, v);
            context.write(new IntWritable(j), e);
            s.close();
        }
    }

    public static class ReducerOne extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
        ArrayList<Elem> M = new ArrayList<Elem>();
        ArrayList<Elem> N = new ArrayList<Elem>();
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                           throws IOException, InterruptedException {
            M.clear();
            N.clear();
            for (Elem v: values){
                if (v.tag == 0){
                    M.add(new Elem(v.tag, v.index, v.value));
                }       
				else{
					N.add(new Elem(v.tag, v.index, v.value));
				}
                
            };
                 
            System.out.println("Vector M: "+M);
            System.out.println("Vector N: "+N);
            for ( Elem m: M )
			{
				for ( Elem n: N )
				{
					if (m.index== n.index){
					context.write(new Pair (m.index, n.index),new DoubleWritable(m.value + n.value));//addition
					}
				}
			}
        }
    }

    public static class MapperTwo extends Mapper<Pair, DoubleWritable, Pair, DoubleWritable>{
        @Override
        public void map ( Pair key, DoubleWritable value, Context context )
                    throws IOException, InterruptedException{
            context.write(key, value);//Nothing to process
        }
    }

    public static class ReducerTwo extends Reducer<Pair,DoubleWritable,Text,DoubleWritable> {
        @Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
            double m = 0;
            for (DoubleWritable v: values) {
                m = m + v.get();//adding the values again
            };
            context.write(new Text (key.toString()),new DoubleWritable (m));//final output
        }
    }
	    @Override
    public int run ( String[] args ) throws Exception {
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
        Job JobFirst = Job.getInstance();
        JobFirst.setJobName("JobFirst");
        JobFirst.setJarByClass(Add.class);
        JobFirst.setOutputKeyClass(Pair.class);
        JobFirst.setOutputValueClass(DoubleWritable.class);
        JobFirst.setMapOutputKeyClass(IntWritable.class);
        JobFirst.setMapOutputValueClass(Elem.class);
        JobFirst.setReducerClass(ReducerOne.class);
        JobFirst.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(JobFirst,new Path(args[0]),TextInputFormat.class,MatrixOneMapper.class);
        MultipleInputs.addInputPath(JobFirst,new Path(args[1]),TextInputFormat.class,MatrixTwoMapper.class);
        FileOutputFormat.setOutputPath(JobFirst,new Path(args[2]));
        JobFirst.waitForCompletion(true);
        
        Job JobSecond = Job.getInstance();
        JobSecond.setJobName("JobSecond");
        JobSecond.setJarByClass(Add.class);
        JobSecond.setMapperClass(MapperTwo.class);
        JobSecond.setReducerClass(ReducerTwo.class);
        JobSecond.setOutputValueClass(DoubleWritable.class);
        JobSecond.setMapOutputKeyClass(Pair.class);
        JobSecond.setMapOutputValueClass(DoubleWritable.class);
        JobSecond.setInputFormatClass(SequenceFileInputFormat.class);
        JobSecond.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(JobSecond, new Path(args[2]));
        FileOutputFormat.setOutputPath(JobSecond,new Path(args[3]));
        JobSecond.waitForCompletion(true);
    }	
}