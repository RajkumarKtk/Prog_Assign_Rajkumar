import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class DataExecution implements Serializable {

    private final static DataExecution DATA_EXECUTION = new DataExecution();

    private DataExecution(){

    }

    public static DataExecution getInstance(){
        return DATA_EXECUTION;
    }

    void execute(String custdataPaths, String transDataPaths) throws IOException {

        //Initializing Spark Session
        SparkSession ses = SparkSession.builder().master("local").getOrCreate();

        //Read Customer.csv file into Dataset
        Dataset<Row> custDS = readCSVFile.readCustomerDS(ses, custdataPaths).dropDuplicates();  // dropping duplicate data if any present

        //Read transactions.csv file into Dataset
        Dataset<Row> transDS = readCSVFile.readTransactionDS(ses, transDataPaths).dropDuplicates();   // dropping duplicate data if any present

        //Formatting the "date" column into "yyyy-MM-dd" format, currently the "date" has different formats in the csv file
        transDS = transDS.withColumn("fDate", functions.when(to_date(col("date"), "yyyy-MM-dd").isNotNull(),
                        to_date(col("date"), "yyyy-MM-dd"))
                .when(to_date(col("date"), "MM/dd/yyyy").isNotNull(),
                        to_date(col("date"), "MM/dd/yyyy"))
                .when(to_date(col("date"), "M/d/yyyy").isNotNull(),
                        to_date(col("date"), "M/d/yyyy"))
                .when(to_date(col("date"), "yyyy MMMM dd").isNotNull(),
                        to_date(col("Date"), "yyyy MMMM dd"))
                .otherwise("Unknown")).drop(col("date"));

        transDS = transDS.withColumnRenamed("fDate","date");
        transDS.cache(); //cache transaction Dataset

        //filter out customers younger than 20
        custDS = custDS.filter(col("age").gt(lit(20)));

        custDS.cache(); //cache customer Dataset after filtered the unwanted data

        //mask postcode of the customer if cell size is greater than 5 for state, gender and age combination
        custDS = custDS.withColumn("sga", concat(col("state"), (col("gender")), (col("age"))))
                .withColumn("postcode", when(length(col("sga")).gt(5), lit("******")).otherwise(col("postcode"))).drop(col("sga"));

        //5 year bucketing for age column
        custDS = custDS.withColumn("bAge", col("age").$minus(col("age").$percent(5)))
                .withColumn("age", concat(lit("["),col("bAge"),lit("-"),col("bAge").$plus(4),lit("]"))).drop(col("bAge"));


        //correct or add loyalty flag
        //filter out customers that did not transact and transactions of customers that do not present in the customer transactions of customers that do not present in the customer
        Dataset<Row> cusTransDS = transDS.groupBy(col("customer_id"),month(col("date")).as("month"),year(col("date")).as("year"))
                .agg(sum(col("total")).as("spend"));
        cusTransDS = cusTransDS.withColumn("loyal_cust", when(col("spend").gt(1000),"true").otherwise("false"))
                .drop("spend","month","year").dropDuplicates();

        WindowSpec windowSpec = Window.partitionBy("customer_id").orderBy("loyal_cust");
        cusTransDS = cusTransDS.withColumn("row_number",rank().over(windowSpec));
        cusTransDS = cusTransDS.withColumn("loyal",when(col("loyal_cust").equalTo("true").and(col("row_number").equalTo(1)),"true")
                        .otherwise("false")).select("customer_id","loyal").dropDuplicates();


        custDS = custDS.join(cusTransDS,custDS.col("person_id").equalTo(cusTransDS.col("customer_id")));
        custDS =  custDS.withColumn("loyal_customer",custDS.col("loyal")).drop("customer_id","loyal").dropDuplicates();

        //summary of spendings by days of week
        Dataset<Row> dowTrans = transDS.withColumn("day_of_week",date_format(col("date"), "EEEE"))
                .withColumn("dow",dayofweek(col("date")))
                .groupBy("customer_id","day_of_week","dow").agg(sum("total").as("total_spend")).orderBy("customer_id","dow").drop("dow");
        //dowTrans.show(100);


        //transactions happening on Wednesdays cap the Total amount to 99 if theamount is more or equal to 100
        transDS = transDS.withColumn("day_of_week",date_format(col("date"), "EEEE"))
                .withColumn("total_spend", when(col("total").geq(100).and(col("day_of_week").equalTo(lit("Wednesday"))), 99)
                 .otherwise(col("total")))
                .withColumn("cogs",when(col("day_of_week").equalTo(lit("Wednesday")).and(col("total_spend").equalTo(lit(99))),col("total_spend").$times(lit(100).$div(lit(120))))
                        .otherwise(col("cogs")))
                .withColumn("tax_%",when(col("day_of_week").equalTo(lit("Wednesday")).and(col("total_spend").equalTo(lit(99))),col("total_spend").$minus(col("cogs")))
                        .otherwise(col("tax_%")))
                .withColumn("unit_price",when(col("day_of_week").equalTo(lit("Wednesday")).and(col("total_spend").equalTo(lit(99))),col("cogs").$div(col("quantity")))
                        .otherwise(col("unit_price")));

        transDS = transDS.drop("total").withColumnRenamed("total_spend","total");

        //column Time in transactions round down by 15 min periods
        transDS = transDS.withColumn("round_hours",hour(col("time")))
                .withColumn("round_min",minute(col("time")))
                .withColumn("round_min",floor(col("round_min").$div(15)).$times(15));
        transDS = transDS.withColumn("time", concat(col("round_hours"),lit(":"),col("round_min"))).drop("round_hours","round_min");


        transDS = transDS.select("customer_id","product_line","unit_price","quantity","tax_%","total","date","day_of_week","time","payment","cogs","gross_margin_percent","gross_income","rating")
                .orderBy("customer_id","date");

        custDS = custDS.select("person_id","postcode","state","gender","age","account_type","loyal_customer").orderBy("person_id");

        //write csv output File under 'output' directory
        Path transPath = new Path("src/main/output/trans.csv");
        Path cusPath = new Path("src/main/output/cust.csv");
        writeCsvFile(transPath,transDS,ses);
        writeCsvFile(cusPath,custDS,ses);


        custDS.unpersist();
        transDS.unpersist();
    }

    public void writeCsvFile(Path transPath, Dataset<Row> finalDS, SparkSession ses) throws IOException {
        Path tmpFinalPath = transPath.suffix(".tmp");
        finalDS.coalesce(1).write().option("header", "true").csv(tmpFinalPath.toString());

        Configuration hadoopConfig = ses.sparkContext().hadoopConfiguration();
        FileSystem tmpFinalSrcFS = tmpFinalPath.getFileSystem(hadoopConfig);
        FileSystem tmpFinalDstFS = tmpFinalPath.getFileSystem(hadoopConfig);

        tmpFinalSrcFS.setWriteChecksum(false);
        OutputStream out = tmpFinalDstFS.create(transPath);
        try {
            FileStatus contents[] = tmpFinalSrcFS.globStatus(new Path(tmpFinalPath, "part-*.csv"));
            Arrays.sort(contents);
            for (int i = 0; i < contents.length; i++) {
                if (contents[i].isFile()) {
                    InputStream in = tmpFinalSrcFS.open(contents[i].getPath());
                    try {
                        IOUtils.copyBytes(in, out, hadoopConfig, false);
                    } finally {
                        in.close();
                    }
                }
            }
        }
        finally {
            out.close();
        }
        tmpFinalSrcFS.delete(tmpFinalPath,true);
    }




}
