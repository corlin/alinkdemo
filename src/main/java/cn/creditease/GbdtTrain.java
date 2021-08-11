package cn.creditease;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.GbdtPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.GbdtTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.sink.AppendModelStreamFileSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.GbdtPredictStreamOp;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.operator.stream.sink.KafkaSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.operator.stream.source.KafkaSourceStreamOp;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;


/**
 * Example for GBDT.
 */
public class GbdtTrain {

    public static void main(String[] args) throws Exception {

        //offline train &test
        String schema = "age bigint, workclass string, fnlwgt bigint, education string, " +
                "education_num bigint, marital_status string, occupation string, " +
                "relationship string, race string, sex string, capital_gain bigint, " +
                "capital_loss bigint, hours_per_week bigint, native_country string, label string";
        String mdfile="/data/model/gbdt.ak";
        //String mdfile="d:/devroot/alinkdemo/gbdt.ak";

        BatchOperator trainData = new CsvSourceBatchOp()
                .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/adult_train.csv").setSchemaStr(schema);

        BatchOperator testData = new CsvSourceBatchOp()
                .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/adult_test.csv").setSchemaStr(schema);

        GbdtClassifier gbdt = new GbdtClassifier()
                .setFeatureCols(new String[]{"age", "capital_gain", "capital_loss", "hours_per_week",
                        "workclass", "education", "marital_status", "occupation"})
                .setCategoricalCols(new String[]{"workclass", "education", "marital_status", "occupation"})
                .setLabelCol("label")
                .setNumTrees(20)
                .setPredictionCol("prediction_result");

        gbdt.fit(trainData).transform(testData).print();

        //Batch CSVfile
        BatchOperator  batchSource = new CsvSourceBatchOp()
                .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/adult_train.csv").setSchemaStr(schema);
        BatchOperator trainOp = new GbdtTrainBatchOp()
                .setLearningRate(1.0)
                .setNumTrees(3)
                .setMinSamplesPerLeaf(1)
                .setLabelCol("label")
                .setFeatureCols(new String[]{"age", "capital_gain", "capital_loss", "hours_per_week",
                        "workclass", "education", "marital_status", "occupation"})
                .setCategoricalCols(new String[]{"workclass", "education", "marital_status", "occupation"})
                .linkFrom(batchSource);


        BatchOperator predictBatchOp = new GbdtPredictBatchOp()
                .setPredictionDetailCol("pred_detail")
                .setPredictionCol("pred");
        predictBatchOp.linkFrom(trainOp, batchSource).print();


        trainOp.linkFrom(batchSource).link(
                new AkSinkBatchOp()
                        .setFilePath(mdfile).setOverwriteSink(true)
        );
        BatchOperator.execute();
    }
}
