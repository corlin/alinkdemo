package cn.creditease;

//import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;

public class KafkaDemo {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认情况下，检查点被禁用。要启用检查点，请在StreamExecutionEnvironment上调用enableCheckpointing(n)方法，
        // 其中n是以毫秒为单位的检查点间隔。每隔5000 ms进行启动一个检查点,则下一个检查点将在上一个检查点完成后5秒钟内启动

        env.enableCheckpointing(500);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.17.0.10:9092,172.17.0.3:9092,172.17.0.5:9092");//kafka的节点的IP或者hostName，多个使用逗号分隔
        //properties.setProperty("zookeeper.connect", "hadoop01:2181");//zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
        properties.setProperty("group.id", "test-consumer-group");//flink consumer flink的消费者的group.id
        System.out.println("=======");
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("gbdt", new org.apache.flink.api.common.serialization.SimpleStringSchema(), properties);
        // FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("test",new SimpleStringSchema(),properties);//test0是kafka中开启的topic
        myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
        DataStream<String> keyedStream = env.addSource(myConsumer);//将kafka生产者发来的数据进行处理，本例子我进任何处理
        System.out.println("=========");
        keyedStream.print();//直接将从生产者接收到的数据在控制台上进行打印
        // execute program
        System.out.println("=============");
        env.execute("Flink Streaming Java API Skeleton");

    }
}