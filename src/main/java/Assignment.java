import java.io.IOException;

public class Assignment {                               //Assignment class is the main class for execution


    public static void main(String[] args) throws IOException {

        //Input file path for customer.csv and transactions.csv
        String cusDataDirPath = "src/main/resources/customer.csv";
        String transDataDirPath = "src/main/resources/transactions.csv";
        String dataDirPath = "src/main/resources/data.csv";

        //Data execution process
        DataExecution.getInstance().execute(cusDataDirPath,transDataDirPath,dataDirPath);
    }


}