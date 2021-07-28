package hive_udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author Djh on  2021/6/10 18:40
 * @E-Mail 1544579459.djh@gmail.com
 */
public class UdfTest extends UDF {
    public String evaluate(String value) {
        if (value.contains("2015")) {
            return "This year is 2015";
        } else {
            return "This year is 2016";
        }
    }
}
