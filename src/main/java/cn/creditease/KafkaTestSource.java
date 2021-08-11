package cn.creditease;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.KafkaSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;

public class KafkaTestSource {
    public static void main(String[] args) throws Exception {
        //AlinkGlobalConfiguration.setPluginDir("D:/devroot/alinktest/alink_plugins/");
        String URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/adult_train.csv";
        //"D:/devroot/alinktest/data/adult_train.csv";

        String SCHEMA_STR = "age bigint, workclass string, fnlwgt bigint, education string, " +
                "education_num bigint, marital_status string, occupation string, " +
                "relationship string, race string, sex string, capital_gain bigint, " +
                "capital_loss bigint, hours_per_week bigint, native_country string, label string";

        CsvSourceStreamOp data = new CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);

        KafkaSinkStreamOp sink = new KafkaSinkStreamOp()
                .setBootstrapServers("172.17.0.10:9092,172.17.0.2:9092,172.17.0.7:9092")
                //.setBootstrapServers("kubedev:9092")
                .setDataFormat("json")
                .setTopic("gbdt");

        data.link(sink);



        StreamOperator.execute();
    }
}
