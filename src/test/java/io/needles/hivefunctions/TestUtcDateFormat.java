package io.needles.hivefunctions;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.Timestamp;


public class TestUtcDateFormat {

    /**
     * For this test to be meaningful it should be run in a non-utc timezone
     */
    @Test
    public void testUnixEpocTimestamp() throws Exception {
        UtcDateFormat udf = new UtcDateFormat();
        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
        Text fmtText = new Text("yyyy-MM-dd'T'HH:mm:ssX");
        ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
                .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, fmtText);
        ObjectInspector[] arguments = { valueOI0, valueOI1 };

        udf.initialize(arguments);

        GenericUDF.DeferredObject valueObj0 = new GenericUDF.DeferredJavaObject(new TimestampWritable(new Timestamp(0l)));
        GenericUDF.DeferredObject valueObj1 = new GenericUDF.DeferredJavaObject(fmtText);
        GenericUDF.DeferredObject[] args = { valueObj0, valueObj1 };
        Text output = (Text) udf.evaluate(args);
        assertEquals("1970-01-01T00:00:00Z", output.toString());

    }
}
