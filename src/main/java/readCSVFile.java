import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static common.constant.*;

public class readCSVFile {


    public static Dataset<Row> readCustomerDS(SparkSession ss, String custdataPaths){
        StructType custSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(PERSON_ID,  DataTypes.StringType, true),
                DataTypes.createStructField(POSTCODE, DataTypes.LongType, true),
                DataTypes.createStructField(STATE, DataTypes.StringType, true),
                DataTypes.createStructField(GENDER, DataTypes.StringType, true),
                DataTypes.createStructField(AGE, DataTypes.IntegerType, true),
                DataTypes.createStructField(ACCOUNT_TYPE, DataTypes.StringType, true),
                DataTypes.createStructField(LOYAL_CUSTOMER, DataTypes.BooleanType, true)
        });

        //create customer data Dataset
        return ss.read().schema(custSchema).option("header","true").option("nullValue",null).option("ignoreLeadingWhiteSpace","true")
                .option("ignoreTrailingWhiteSpace","true").csv(custdataPaths);
    }

    public static Dataset<Row> readTransactionDS(SparkSession ss, String transdataPaths){
        StructType transSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(CUSTOMER_ID,  DataTypes.StringType, true),
                DataTypes.createStructField(PRODUCT_LINE, DataTypes.StringType, true),
                DataTypes.createStructField(UNIT_PRICE, DataTypes.FloatType, true),
                DataTypes.createStructField(QUANTITY, DataTypes.IntegerType, true),
                DataTypes.createStructField(TAX, DataTypes.FloatType, true),
                DataTypes.createStructField(TOTAL, DataTypes.FloatType, true),
                DataTypes.createStructField(DATE, DataTypes.StringType, true),
                DataTypes.createStructField(TIME, DataTypes.StringType, true),
                DataTypes.createStructField(PAYMENT, DataTypes.StringType, true),
                DataTypes.createStructField(COGS, DataTypes.FloatType, true),
                DataTypes.createStructField(GROSS_MARGIN_PERCENTAGE, DataTypes.DoubleType, true),
                DataTypes.createStructField(GROSS_INCOME, DataTypes.FloatType, true),
                DataTypes.createStructField(RATING, DataTypes.FloatType, true)
        });
        //create transactions data Dataset
        return ss.read().schema(transSchema).option("header","true").option("nullValue",null).option("ignoreLeadingWhiteSpace","true")
              .option("ignoreTrailingWhiteSpace","true").csv(transdataPaths);
    }

    public static Dataset<Row> readDataDS(SparkSession ss, String dataPaths){
        StructType daraSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(DATA,  DataTypes.StringType, true),
                DataTypes.createStructField(ID, DataTypes.StringType, true)
        });

        //create customer data Dataset
        return ss.read().schema(daraSchema).option("header","true").option("nullValue",null).option("ignoreLeadingWhiteSpace","true")
                .option("ignoreTrailingWhiteSpace","true").csv(dataPaths);
    }

}
