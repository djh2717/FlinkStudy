package hive_udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Collections;
import java.util.List;

/**
 * @Author Djh on  2021/6/16 14:38
 * @E-Mail 1544579459.djh@gmail.com
 */
public class UdtfTest extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 判断参数个数.
        List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();
        if (allStructFieldRefs.size() != 1) {
            throw new UDFArgumentException("需要一个参数!");
        }

        // 判断参数类型


        // 确定输出数据的类型
        return ObjectInspectorFactory.getStandardStructObjectInspector(
                Collections.singletonList("result_column_name"),
                Collections.singletonList(ObjectInspectorFactory.getStandardMapObjectInspector(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector
                ))
        );
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String s = args[0].toString();
        for (String s1 : s.split(",")) {
            forward(s1);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}