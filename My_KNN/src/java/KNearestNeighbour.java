import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import king.Utils.Distance;
import king.Utils.ListWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KNearestNeighbour {


    public static class  knnmap  extends Mapper<LongWritable,Text,paris,Text>{
        // TODO Auto-generated method stub
        static Integer t_id;//记录标识测试数据的行号，将行号作为测试数据id
        private ArrayList<String> testData=new ArrayList<String>();//存储测试数据
        String distanceway;//得到距离计算方式

        @Override
        protected void setup(Mapper<LongWritable, Text, paris, Text>.Context context)
                throws IOException, InterruptedException {//初始化 读取全局文件和全局参数
            // TODO Auto-generated method stub
            t_id=1;
            Configuration jobconf = context.getConfiguration();
            distanceway=jobconf.get("distanceway","osjl");
            Path [] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader joinReader = new BufferedReader(
                        new FileReader(cacheFiles[0].toString()));
                try{
                    String line=null;
                    while ((line = joinReader.readLine()) != null) {
                        testData.add(line);
                    }
                } finally {  joinReader.close(); }
                super.setup(context);
            }
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, paris, Text>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String train_data=value.toString();
            String[] rc1=train_data.split(",");//文件是csv格式的，使用','分割开
            String type=rc1[rc1.length-1];

            for(String i : testData){
                String[] rc2=i.split(",");
                Double dis=getdistance(rc1, rc2,distanceway);//计算距离
                context.write(new paris(t_id,dis), new Text(type));//使用自定数据类型写出
                t_id++;
            }
            t_id=1;//重置行号
        }


        public static class paris implements WritableComparable<paris>{

            Integer t_id;//测试数据id
            Double dis;//距离

            public paris(){

            }

            public paris(Integer t_id, Double dis) {
                super();
                this.t_id = t_id;
                this.dis = dis;
            }

            @Override
            public void write(DataOutput out) throws IOException {
                // TODO Auto-generated method stub
                out.writeInt(t_id);
                out.writeDouble(dis);
            }

            @Override
            public void readFields(DataInput in) throws IOException {
                // TODO Auto-generated method stub
                t_id=in.readInt();
                dis=in.readDouble();
            }

            @Override
            public int compareTo(paris o) {//实现compare完成特定的排序
                if(o.t_id.equals(t_id))//先比较测试数据的id
                    return dis.compareTo(o.dis);//再比较距离
                return t_id.compareTo(o.t_id);
            }

            @Override
            public boolean equals(Object o) {
                // TODO Auto-generated method stub
                paris i=(paris) o;
                return i.compareTo(this)==0;//判断是否相等，考虑出现距离相等的情况
            }

            @Override
            public int hashCode() {
                // TODO Auto-generated method stub
                return t_id.hashCode();//返回测数据的id的hash，将同一个id的发送到同一个reduce
            }

            @Override
            public String toString() {
                // TODO Auto-generated method stub
                return t_id.toString()+":"+dis.toString();
            }
        }

        @Override
        public void map(LongWritable textIndex, Text textLine, Context context)
                throws IOException, InterruptedException {

            ArrayList<Double> distance = new ArrayList<Double>(k);
            ArrayList<DoubleWritable> trainLabel = new ArrayList<DoubleWritable>(k);
            for(int i = 0;i < k;i++){
                distance.add(Double.MAX_VALUE);
                trainLabel.add(new DoubleWritable(-1.0));
            }
            ListWritable<DoubleWritable> Labels = new ListWritable<DoubleWritable>(DoubleWritable.class);
            Instance testInstance = new Instance(textLine.toString());
            for(int i = 0;i < trainSet.size();i++){
                try {
                    double dis = Distance.EuclideanDistance(trainSet.get(i).getAtrributeValue(), testInstance.getAtrributeValue());
                    int index = indexOfMax(distance);
                    if(dis < distance.get(index)){
                        distance.remove(index);
                        trainLabel.remove(index);
                        distance.add(dis);
                        trainLabel.add(new DoubleWritable(trainSet.get(i).getLabel()));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Labels.setList(trainLabel);
            context.write(textIndex, Labels);
        }

    public static class knncombiner extends Reducer<paris, Text, paris, Text>{
        static int k;//全局参数k
        int j;			//当前id剩余数据量
        Integer currentt_id;//当前id

        public void setup(Context context) {//初始化变量
            Configuration jobconf = context.getConfiguration();
            k = jobconf.getInt("k", 5);
            currentt_id=null;
            j=k;
        }

        @Override
        protected void reduce(paris key, Iterable<Text> value,
                              Reducer<paris, Text, paris, Text>.Context context)
                throws IOException, InterruptedException {
            if(currentt_id==null)//考虑到第一次运行
                currentt_id=key.t_id;

            if(currentt_id!=key.t_id){//测试数据id变更就重置j和当前id
                j=k;
                currentt_id=key.t_id;
            }

            if(j!=0)//限制每个id只写出k条数据
                for(Text i:value){//考虑到距离相同的情况
                    j--;
                    context.write(key, i);
                    if(j==0)
                        break;
                }
        }
    }

        public int indexOfMax(ArrayList<Double> array){
            int index = -1;
            Double min = Double.MIN_VALUE;
            for (int i = 0;i < array.size();i++){
                if(array.get(i) > min){
                    min = array.get(i);
                    index = i;
                }
            }
            return index;
        }
    }

public static class knnreduce extends Reducer<paris, Text, Text, NullWritable> {

    static int k;//全局参数限制量top k
    String distanceway;//距离计算方式
    int j;//当前id剩余量
    HashMap<String, Integer> hm;//存储类标签和对应的次数
    Integer currentt_id;

    double right_count;//存储与检验文件相同的数量
    double false_count;//存储于检验文件不同的数量
    private ArrayList<String> lableData = new ArrayList<String>();//读取检验文件用于正确率的计算

    public void setup(Context context) throws IOException, InterruptedException {
        Configuration jobconf = context.getConfiguration();//初始化变量
        k = jobconf.getInt("k", 5);
        distanceway = jobconf.get("distanceway", "osjl");//默认距离计算方式欧式距离
        hm = new HashMap<String, Integer>();
        currentt_id = null;
        j = k;

        right_count = 0;
        false_count = 0;
        Path[] cacheFiles = context.getLocalCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            BufferedReader joinReader = new BufferedReader(
                    new FileReader(cacheFiles[1].toString()));
            try {
                String line = null;
                while ((line = joinReader.readLine()) != null) {
                    lableData.add(line.trim());
                }
            } finally {
                joinReader.close();
            }
            super.setup(context);
        }
    }


    @Override
    protected void reduce(paris key, Iterable<Text> value,
                          Reducer<paris, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {

        if(currentt_id==null)//考虑第一次运行
            currentt_id=key.t_id;

        if(currentt_id!=key.t_id){//id变更修改当前id剩余量，改变当前id
            j=k;
            currentt_id=key.t_id;
        }

        if(j!=0)//控制前k条数据进行运算
            for(Text i:value){//考虑距离一样的情况
                Integer count=1;
                if(hm.containsKey(i.toString()))
                    count+=hm.get(i.toString());
                hm.put(i.toString(), count);//更新类别对应的计数
                j--;
                if(j==0){//id对应的前k条数据录入完毕，计算投票的结果
                    String newkey=getvalue(hm, 1).trim();
                    context.write(new Text(newkey), NullWritable.get());//写出类别预测结果
                    hm.clear();
                    if(lableData.get((int) (false_count+right_count)).equals(newkey))
                        right_count+=1;//计算预测效果和真实效果的差别数
                    else
                        false_count+=1;
                }
            }}

    @Override
    protected void cleanup(
            Reducer<paris, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub//写出预测的结果和一些参数设置情况
        context.write(new Text("距离计算方式:"+distanceway), NullWritable.get());
        context.write(new Text("k:"+k), NullWritable.get());
        context.write(new Text("准确率:"+right_count/(right_count+false_count)), NullWritable.get());
    }
}

        public DoubleWritable valueOfMostFrequent(ListWritable<DoubleWritable> list) throws Exception{
            if(list.isEmpty())
                throw new Exception("list is empty!");
            else{
                HashMap<DoubleWritable,Integer> tmp = new HashMap<DoubleWritable,Integer>();
                for(int i = 0 ;i < list.size();i++){
                    if(tmp.containsKey(list.get(i))){
                        Integer frequence = tmp.get(list.get(i)) + 1;
                        tmp.remove(list.get(i));
                        tmp.put(list.get(i), frequence);
                    }else{
                        tmp.put(list.get(i), new Integer(1));
                    }
                }
                DoubleWritable value = new DoubleWritable();
                Integer frequence = new Integer(Integer.MIN_VALUE);
                Iterator<Entry<DoubleWritable, Integer>> iter = tmp.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<DoubleWritable,Integer> entry = (Map.Entry<DoubleWritable,Integer>) iter.next();
                    if(entry.getValue() > frequence){
                        frequence = entry.getValue();
                        value = entry.getKey();
                    }
                }
                return value;
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        Job kNNJob = Job.getInstance(conf, "kNNJob");
        kNNJob.setJarByClass(KNearestNeighbour.class);
        kNNJob.addCacheFile(URI.create(args[2]));
        kNNJob.getConfiguration().setInt("k", Integer.parseInt(args[3]));

        kNNJob.setMapperClass(KNNMap.class);
        kNNJob.setMapOutputKeyClass(LongWritable.class);
        kNNJob.setMapOutputValueClass(ListWritable.class);

        kNNJob.setReducerClass(KNNReduce.class);
        kNNJob.setOutputKeyClass(NullWritable.class);
        kNNJob.setOutputValueClass(DoubleWritable.class);

        kNNJob.setInputFormatClass(TextInputFormat.class);
        kNNJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(kNNJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(kNNJob, new Path(args[1]));

        kNNJob.waitForCompletion(true);
        System.out.println("finished!");
    }
}

