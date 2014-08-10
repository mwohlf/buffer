package buffer;

import java.io.Closeable;
import java.nio.ByteBuffer;


public interface IWriteStream extends Closeable {
	
	void write(long timestamp, ByteBuffer buffer);
	
}
