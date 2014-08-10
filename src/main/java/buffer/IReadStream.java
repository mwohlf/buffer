package buffer;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface IReadStream extends Closeable {

	ByteBuffer read() throws InterruptedException ;

}
