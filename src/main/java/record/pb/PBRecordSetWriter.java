package record.pb;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.concurrent.CancellationException;

import record.DefaultRecord;
import record.Record;
import record.RecordSet;
import record.RecordSetWriter;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class PBRecordSetWriter implements RecordSetWriter {
	private final OutputStream m_out;
	
	PBRecordSetWriter(OutputStream out) {
		m_out = out;
	}

	@Override
	public void close() throws IOException {
		m_out.close();
	}

	@Override
	public long write(RecordSet rset) throws IOException {
		Record rec = DefaultRecord.of(rset.getRecordSchema());
		
		long count = 0;
		try {
			PBRecords.toProto(rset.getRecordSchema()).writeDelimitedTo(m_out);
			while ( rset.next(rec) ) {
				PBRecords.toProto(rec).writeDelimitedTo(m_out);
				++count;
			}
			
			return count;
		}
		catch ( InterruptedIOException e ) {
			throw new CancellationException("" + e);
		}
		finally {
			rset.closeQuietly();
			IOUtils.closeQuietly(m_out);
		}
	}
}
