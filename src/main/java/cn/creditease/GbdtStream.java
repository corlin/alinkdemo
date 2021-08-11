package cn.creditease;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.GbdtPredictStreamOp;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.operator.stream.sink.KafkaSinkStreamOp;
import com.alibaba.alink.operator.stream.source.AkSourceStreamOp;
import com.alibaba.alink.operator.stream.source.KafkaSourceStreamOp;

import org.apache.flink.types.Row;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import java.util.Arrays;
import java.util.List;

import java.util.Properties;

public class GbdtStream {
    public static void main(String[] args) throws Exception{


        //offline train &test
        String schema = "age bigint, workclass string, fnlwgt bigint, education string, " +
                "education_num bigint, marital_status string, occupation string, " +
                "relationship string, race string, sex string, capital_gain bigint, " +
                "capital_loss bigint, hours_per_week bigint, native_country string, label string";
        String mdfile="/data/model/gbdt.ak";
        //String mdfile="d:/devroot/alinkdemo/gbdt.ak";

        System.out.println("1======================================================");
        Properties props=System.getProperties(); //系统属性
        System.out.println("用户的账户名称："+props.getProperty("user.name"));
        System.out.println("用户的主目录："+props.getProperty("user.home"));
        System.out.println("用户的当前工作目录："+props.getProperty("user.dir"));

        AkSourceBatchOp gbdtmd = new AkSourceBatchOp().setFilePath(mdfile);
        //AkSourceBatchOp gbdtmd = new AkSourceBatchOp().setFilePath(new FilePath(mdfile));
        gbdtmd.print();

        //new AkSourceStreamOp().setFilePath(new FilePath(mdfile)).print();
        //StreamOperator.execute();

        //获取model
        System.out.println("2======================================================");

        //Stream Kafka
        KafkaSourceStreamOp source = new KafkaSourceStreamOp()
                .setBootstrapServers("172.17.0.10:9092,172.17.0.2:9092,172.17.0.7:9092")
                //.setBootstrapServers("kubedev:9092")
                .setTopic("gbdt")
                .setStartupMode("EARLIEST")
                .setGroupId("bbb");

        StreamOperator data = source
                .link(
                        new JsonValueStreamOp()
                                .setSelectedCol("message")
                                .setReservedCols(new String[] {})
                                .setOutputCols(
                                        new String[] {"age",
                                                "workclass",
                                                "fnlwgt",
                                                "education",
                                                "education_num",
                                                "marital_status",
                                                "occupation",
                                                "relationship",
                                                "race",
                                                "sex",
                                                "capital_gain",
                                                "capital_loss",
                                                "hours_per_week",
                                                "native_country",
                                                "label"})
                                .setJsonPath(new String[] {"$age",
                                        "$workclass",
                                        "$fnlwgt",
                                        "$education",
                                        "$education_num",
                                        "$marital_status",
                                        "$occupation",
                                        "$relationship",
                                        "$race",
                                        "$sex",
                                        "$capital_gain",
                                        "$capital_loss",
                                        "$hours_per_week",
                                        "$native_country",
                                        "$label"})
                )
                .select(" CAST(age  AS DOUBLE ) AS age,"
                        + "CAST(capital_gain  AS DOUBLE ) AS capital_gain,"
                        + "CAST(capital_loss  AS DOUBLE ) AS capital_loss,"
                        + "CAST(hours_per_week  AS DOUBLE ) AS hours_per_week,"
                        + " CAST(education_num  AS DOUBLE ) AS education_num,"
                        + "CAST(fnlwgt  AS DOUBLE ) AS fnlwgt ,"
                        + "education,workclass,label,native_country,sex,race,relationship,occupation,marital_status"
                );


        data.print();



        StreamOperator predictStreamOp = new GbdtPredictStreamOp(gbdtmd)
                .setPredictionDetailCol("pred_detail")
                .setPredictionCol("pred");

        //predictStreamOp.linkFrom(data).print();

        predictStreamOp.linkFrom(data).link(new KafkaSinkStreamOp()
                .setBootstrapServers("172.17.0.10:9092,172.17.0.2:9092,172.17.0.7:9092")
                //.setBootstrapServers("kubedev:9092")
                .setDataFormat("json")
                .setTopic("gbdtout"));

        StreamOperator.execute();

    }
}
