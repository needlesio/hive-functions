package io.needles.hivefunctions;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


/**
 * UtcDateFormat.
 *
 * Like GenericUDFDateFormat, but first converts the timezone to Utc/GMT
 *
 */
@Description(name = "utc_date_format", value = "_FUNC_(date/timestamp/string, fmt) - converts a date/timestamp/string "
        + "to a value of string in the format specified by the date format fmt.",
        extended = "Supported formats are SimpleDateFormat formats - "
                + "https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html. "
                + "Second argument fmt should be constant.\n"
                + "Example: > SELECT _FUNC_('2015-04-08', 'y');\n '2015'")
public class UtcDateFormat extends GenericUDFDateFormat {
    private transient SimpleDateFormat formatter;
    private transient Converter[] tsConverters = new Converter[2];
    private transient Converter[] dtConverters = new Converter[2];
    private transient PrimitiveCategory[] dtInputTypes = new PrimitiveCategory[2];
    private final Text output = new Text();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        ObjectInspector oi =  super.initialize(arguments);
        String fmtStr = getConstantStringValue(arguments, 1);
        formatter = new SimpleDateFormat(fmtStr);
        formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
        obtainTimestampConverter(arguments, 0, new PrimitiveCategory[2], tsConverters);
        obtainDateConverter(arguments, 0, dtInputTypes, dtConverters);
        return oi;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (formatter == null) {
            return null;
        }
        Date date = getTimestampValue(arguments, 0, tsConverters);
        if (date == null) {
            date = getDateValue(arguments, 0, dtInputTypes, dtConverters);
            if (date == null) {
                return null;
            }
        }

        String res = formatter.format(date);
        if (res == null) {
            return null;
        }
        output.set(res);
        return output;
    }

    @Override
    protected String getFuncName() {
        return "utc_date_format";
    }
}
