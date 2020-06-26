package record.pb;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.concurrent.CancellationException;

import record.DefaultRecord;
import record.Record;
import record.RecordStream;
import record.RecordStreamException;
import record.DataSetWriter;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class PBDataSetWriter implements DataSetWriter {
	private final OutputStream m_out;
	
	PBDataSetWriter(OutputStream out) {
		m_out = out;
	}

	@Override
	public long write(RecordStream stream) {
		Record rec = DefaultRecord.of(stream.getRecordSchema());
		
		long count = 0;
		try {
			PBDataSets.toProto(stream.getRecordSchema()).writeDelimitedTo(m_out);
			while ( stream.next(rec) ) {
				PBDataSets.toProto(rec).writeDelimitedTo(m_out);
				++count;
			}
			
			return count;
		}
		catch ( InterruptedIOException e ) {
			throw new CancellationException("" + e);
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
		finally {
			IOUtils.closeQuietly(m_out);
		}
	}
}
