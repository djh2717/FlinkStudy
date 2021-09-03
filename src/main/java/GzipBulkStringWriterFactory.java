import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class GzipBulkStringWriterFactory<T> implements BulkWriter.Factory<T> {
    @Override
    public BulkWriter<T> create(FSDataOutputStream fsDataOutputStream) throws IOException {
        final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fsDataOutputStream, true);
        return new GzipStringBulkWriter<>(gzipOutputStream);
    }
}
