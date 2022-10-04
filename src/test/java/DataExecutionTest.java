import org.apache.spark.sql.*;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class DataExecutionTest {

    SparkSession ses = SparkSession.builder().master("local").getOrCreate();

    @org.junit.jupiter.api.Test
    public void validateData() {
        String cusDataDirPath = "src/test/resources/customer.csv";
        String TransDataDirPath = "src/test/resources/transactions.csv";
        Dataset<Row> custData = readCSVFile.readCustomerDS(ses, cusDataDirPath).dropDuplicates();
        List<Row> cusRows = custData.collectAsList();

        Dataset<Row> transData = readCSVFile.readTransactionDS(ses, TransDataDirPath).dropDuplicates();

        Row cusExpectedData = RowFactory.create("750-67-8428", 2000, "NSW", "F", 19, "credit", true);

        assertEquals(cusExpectedData, cusRows.get(0));
        assertEquals(5, transData.count());

        Dataset<Row> processCustDS = DataExecution.getInstance().processCustData(custData, transData);
        List<Row> processCusRows = processCustDS.collectAsList();

        Row processCusData = RowFactory.create("226-31-3081", "******", "NSW", "F", "[20-24]", "credit", "false");
        assertEquals(processCusData, processCusRows.get(0));

        transData = transData.withColumn("fDate", functions.when(to_date(col("date"), "yyyy-MM-dd").isNotNull(),
                        to_date(col("date"), "yyyy-MM-dd"))
                .when(to_date(col("date"), "MM/dd/yyyy").isNotNull(),
                        to_date(col("date"), "MM/dd/yyyy"))
                .when(to_date(col("date"), "M/d/yyyy").isNotNull(),
                        to_date(col("date"), "M/d/yyyy"))
                .when(to_date(col("date"), "yyyy MMMM dd").isNotNull(),
                        to_date(col("Date"), "yyyy MMMM dd"))
                .otherwise("Unknown")).drop(col("date"));
        transData = transData.withColumnRenamed("fDate", "date");
        Dataset<Row> processTransDS = DataExecution.getInstance().processTransData(transData,custData);
        assertEquals(5, processTransDS.count());
    }

}