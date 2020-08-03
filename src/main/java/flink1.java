import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class flink1 {
    public static void main(String[] args) throws Exception {
        //主题和分区发现
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();    //流执行环境
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.157.161:9092");    //服务引导
        properties.setProperty("group.id", "test1");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>("test1", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<String> map = stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {

                FileWriter fw = null;
                try {
                    File f=new File("G:\\test.txt");   //如果文件存在，则追加内容；如果文件不存在，则创建文件
                    fw = new FileWriter(f, true);       //true:追加写入
                } catch (IOException e) {
                    e.printStackTrace();
                }
                PrintWriter pw = new PrintWriter(fw,true);

                JSONObject object = JSON.parseObject(s);        //把s由字符串转换成json对象，这样就可以使用json函数自动生成格式
                object.put("flinkTime",System.currentTimeMillis());     //接着json对象往上加
           //     String objStr = JSON.toJSONString(object);              //再把json对象转化为字符串，用来输出
                pw.println(object);

              //  pw.println(s+"flinkTime"+System.currentTimeMillis());
                pw.flush();
                return s;
            }
        });
        map.print();
        env.execute("123");
    }
}


