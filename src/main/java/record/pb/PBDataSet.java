package record.pb;

import static utils.Utilities.checkNotNullArgument;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

import proto.RecordProto;
import proto.RecordSchemaProto;
import record.DataSet;
import record.DataSetException;
import record.Record;
import record.RecordSchema;
import record.RecordStream;
import record.RecordStreamException;
import record.stream.AbstractRecordStream;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class PBDataSet implements DataSet {
	private final Supplier<InputStream> m_inputGen;
	private RecordSchema m_schema = null;
	
	PBDataSet(Supplier<InputStream> inputGen) {
		m_inputGen = inputGen;
	}
	
	PBDataSet(RecordSchema schema, Supplier<InputStream> inputGen) {
		m_inputGen = inputGen;
		m_schema = schema;
	}

	@Override
	public RecordSchema getRecordSchema() {
		if ( m_schema == null ) {
			try ( InputStream is = m_inputGen.get() ) {
				RecordSchemaProto schemaProto = RecordSchemaProto.parseDelimitedFrom(is);
				if ( schemaProto != null ) {
					m_schema = PBDataSets.fromProto(schemaProto);
				}
				else {
					throw new DataSetException("Data is corrupted: no RecordSchema");
				}
			}
			catch ( DataSetException e ) {
				throw e;
			}
			catch ( Exception e ) {
				throw new DataSetException("" + e);
			}
		}
		
		return m_schema;
	}

	public RecordStream read() {
		PBRecordStream iter = new PBRecordStream(m_inputGen.get());
		if ( m_schema == null ) {
			m_schema = iter.getRecordSchema();
		}
		
		return iter;
	}

	private static class PBRecordStream extends AbstractRecordStream {
		private final RecordSchema m_schema;
		private final InputStream m_is;
		
		private PBRecordStream(InputStream is) {
			checkNotNullArgument(is, "InputStream");
			
			try {
				m_is = is;
				RecordSchemaProto schemaProto = RecordSchemaProto.parseDelimitedFrom(is);
				if ( schemaProto == null ) {
					throw new DataSetException("Data is corrupted: no RecordSchema");
				}
				m_schema = PBDataSets.fromProto(schemaProto);
			}
			catch ( DataSetException e ) {
				throw e;
			}
			catch ( Exception e ) {
				throw new DataSetException("" + e);
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
					PBDataSets.fromProto(proto, output);
					return true;
				}
				else {
					return false;
				}
			}
			catch ( IOException e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}
}
