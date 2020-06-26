package record.pb;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.function.Supplier;

import org.slf4j.LoggerFactory;

import proto.RecordProto;
import proto.RecordSchemaProto;
import proto.RecordSchemaProto.ColumnProto;
import proto.ValueProto;
import record.Column;
import record.DataSet;
import record.DataSetException;
import record.DefaultRecord;
import record.Record;
import record.RecordSchema;
import record.RecordStream;
import record.DataSetWriter;
import record.type.DataType;
import utils.Utilities;
import utils.async.AbstractThreadedExecution;
import utils.io.IOUtils;
import utils.io.InputStreamFromOutputStream;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBDataSets {
	private PBDataSets() {
		throw new AssertionError("Should not be called here: class=" + getClass());
	}
	
	public static DataSet fromFile(File file) {
		Supplier<InputStream> supplier = () -> {
			try {
				return new FileInputStream(file);
			}
			catch ( IOException e ) {
				throw new DataSetException("invalid PBDataSet file: path=" + file);
			}
		};
		
		return new PBDataSet(supplier);
	}
	
	public static DataSet fromBytes(byte[] bytes, int offset, int length) {
		Supplier<InputStream> supplier = () -> new ByteArrayInputStream(bytes, offset, length);
		return new PBDataSet(supplier);
	}
	
	public static DataSet fromBytes(byte[] bytes) {
		Supplier<InputStream> supplier = () -> new ByteArrayInputStream(bytes);
		return new PBDataSet(supplier);
	}
	
	public static DataSetWriter writer(OutputStream os) {
		return new PBDataSetWriter(os);
	}

	private static final int DEFAULT_PIPE_SIZE = 64 * 1024;
	public static InputStream toInputStream(RecordStream rset) {
		return new InputStreamFromOutputStream(os -> {
			WriteRecordSetToOutStream pump = new WriteRecordSetToOutStream(rset, os);
			pump.start();
			return pump;
		}, DEFAULT_PIPE_SIZE);
	}
	
	public static RecordSchemaProto toProto(RecordSchema schema) {
		List<ColumnProto> cols = schema.streamColumns().map(c -> toProto(c)).toList();
		return RecordSchemaProto.newBuilder()
								.addAllColumn(cols)
								.build();
	}
	
	public static void fromProto(RecordProto proto, Record record) {
		for ( int i =0; i < record.getColumnCount(); ++i ) {
			record.set(i, PBValueProtos.fromProto(proto.getColumn(i)));
		}
	}
	
	public static DefaultRecord fromProto(RecordSchema schema, RecordProto proto) {
		DefaultRecord record = DefaultRecord.of(schema);
		fromProto(proto, record);
		return record;
	}
	
	public static RecordProto toProto(Record record) {
		RecordProto.Builder builder = RecordProto.newBuilder();
		
		RecordSchema schema = record.getRecordSchema();
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			Column col = schema.getColumnAt(i);
			
			ValueProto vproto = PBValueProtos.toValueProto(col.type().getTypeCode(), record.get(i));
			builder.addColumn(vproto);
		}
		
		return builder.build();
	}

	public static RecordProto toProto(RecordSchema schema, Object[] values) {
		RecordProto.Builder builder = RecordProto.newBuilder();
		
		for ( int i =0; i < values.length; ++i ) {
			Column col = schema.getColumnAt(i);
			
			ValueProto vproto = PBValueProtos.toValueProto(col.type().getTypeCode(), values[i]);
			builder.addColumn(vproto);
		}
		
		return builder.build();
	}

	public static RecordSchema fromProto(RecordSchemaProto proto) {
		Utilities.checkNotNullArgument(proto, "RecordSchemaProto is null");
		
		return FStream.from(proto.getColumnList())
						.map(cp -> fromProto(cp))
						.foldLeft(RecordSchema.builder(),
									(b,c)->b.addColumn(c.name(),c.type()))
						.build();
	}
	
	private static ColumnProto toProto(Column column) {
		return ColumnProto.newBuilder()
							.setName(column.name())
							.setTypeCodeValue(column.type().getTypeCode().get())
							.build();
	}
	
	private static Column fromProto(ColumnProto proto) {
		DataType type = DataType.fromTypeCode((byte)proto.getTypeCodeValue());
		
		return new Column(proto.getName(), type);
	}
	
	private static class WriteRecordSetToOutStream extends AbstractThreadedExecution<Long> {
		private final RecordStream m_rset;
		private final OutputStream m_os;
		
		private WriteRecordSetToOutStream(RecordStream rset, OutputStream os) {
			m_rset = rset;
			m_os = os;
			
			setLogger(LoggerFactory.getLogger(WriteRecordSetToOutStream.class));
		}

		@Override
		protected Long executeWork() throws CancellationException, Exception {
			Record rec = DefaultRecord.of(m_rset.getRecordSchema());
			
			long count = 0;
			try {
				toProto(m_rset.getRecordSchema()).writeDelimitedTo(m_os);
				while ( m_rset.next(rec) ) {
					if ( !isRunning() ) {
						break;
					}
					
					toProto(rec).writeDelimitedTo(m_os);
					++count;
				}
				
				return count;
			}
			catch ( InterruptedIOException e ) {
				throw new CancellationException("" + e);
			}
			finally {
				m_rset.closeQuietly();
				IOUtils.closeQuietly(m_os);
			}
		}
	}
}
