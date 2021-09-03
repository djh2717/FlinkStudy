import org.apache.flink.api.common.serialization.BulkWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

public class GzipStringBulkWriter<T> implements BulkWriter<T> {

    private final GZIPOutputStream gzipOutputStream;

    public GzipStringBulkWriter(GZIPOutputStream gzipOutputStream) {
        this.gzipOutputStream = gzipOutputStream;
    }

    @Override
    public void addElement(T t) throws IOException {
        // write String only
        gzipOutputStream.write(String.valueOf(t).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void flush() throws IOException {
        gzipOutputStream.flush();
    }

    @Override
    public void finish() throws IOException {
        gzipOutputStream.close();
    }
}