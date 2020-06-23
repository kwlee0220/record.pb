package record.pb;

import static utils.Utilities.checkNotNullArgument;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

import proto.RecordProto;
import proto.RecordSchemaProto;
import record.AbstractRecordSet;
import record.Record;
import record.RecordSchema;
import record.RecordSet;
import record.RecordSetException;
import record.RecordSetReader;
import utils.Throwables;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class PBRecordSetReader implements RecordSetReader {
	private final Supplier<InputStream> m_inputGen;
	
	PBRecordSetReader(Supplier<InputStream> inputGen) {
		m_inputGen = inputGen;
	}

	@Override
	public RecordSet read() throws IOException {
		return new PBInputStreamRecordSet(m_inputGen.get());
	}

	private static class PBInputStreamRecordSet extends AbstractRecordSet {
		private final RecordSchema m_schema;
		private final InputStream m_is;
		
		private PBInputStreamRecordSet(InputStream is) {
			checkNotNullArgument(is, "InputStream");
			
			try {
				m_schema = PBRecords.fromProto(RecordSchemaProto.parseDelimitedFrom(is));
				m_is = is;
			}
			catch ( Exception e ) {
				Throwables.throwIfInstanceOf(e, RuntimeException.class);
				throw new RecordSetException(e);
			}
		}

		@Override
		protected void closeInGuard() {
			IOUtils.closeQuietly(m_is);
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			try {
				RecordProto proto = RecordProto.parseDelimitedFrom(m_is);
				if ( proto != null ) {
					PBRecords.fromProto(proto, output);
					return true;
				}
				else {
					return false;
				}
			}
			catch ( IOException e ) {
				throw new RecordSetException("" + e);
			}
		}
		
		@Override
		public Record nextCopy() {
			try {
				RecordProto proto = RecordProto.parseDelimitedFrom(m_is);
				if ( proto != null ) {
					return PBRecords.fromProto(m_schema, proto);
				}
				else {
					return null;
				}
			}
			catch ( IOException e ) {
				throw new RecordSetException("" + e);
			}
		}
	}
}
