package DataCleaner;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.ForeachFunction;

import java.io.Serializable;

abstract class AbstractCleaner implements Serializable {

    protected Dataset<Row> data;
    protected SparkSession sparkSession;

    abstract protected void ProcessRow(Row row);

    // Input the dataset iterator and return the output iterator of the cleaned data
    public void Clean(){
        RowFunction processFun = new RowFunction(this);
        data.foreach(processFun);
        this.sparkSession.close();
    }

    public static class RowFunction implements ForeachFunction<Row>, Serializable {

        private final AbstractCleaner abstractCleaner;

        public RowFunction(AbstractCleaner abstractCleaner){
            this.abstractCleaner = abstractCleaner;
        }

        @Override
        public void call(Row row) {
            abstractCleaner.ProcessRow(row);
        }
    }
}
