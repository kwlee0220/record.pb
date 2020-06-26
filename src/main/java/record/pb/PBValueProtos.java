package record.pb;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.google.protobuf.ByteString;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import proto.TypeCodeProto;
import proto.ValueProto;
import record.type.DataType;
import record.type.GeometryDataType;
import record.type.TypeCode;
import utils.LocalDateTimes;
import utils.LocalDates;
import utils.LocalTimes;
import utils.Utilities;
import utils.func.Tuple;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBValueProtos {
	public static ValueProto NONE = ValueProto.newBuilder().build();
	
	private PBValueProtos() {
		throw new AssertionError("Should not be called: " + getClass());
	}
	
	//
	// TypeCode 정보가 있는 경우의 Object transform
	//
	public static ValueProto toValueProto(TypeCode tc, Object obj) {
		ValueProto.Builder builder = ValueProto.newBuilder();
		
		if ( obj == null ) {
			return builder.setNullValue(TypeCodeProto.valueOf(tc.name())).build();
		}
		switch ( tc ) {
			case BYTE:
				builder.setByteValue((byte)obj);
				break;
			case SHORT:
				builder.setShortValue((short)obj);
				break;
			case INT:
				builder.setIntValue((int)obj);
				break;
			case LONG:
				builder.setLongValue((long)obj);
				break;
			case FLOAT:
				builder.setFloatValue((float)obj);
				break;
			case DOUBLE:
				builder.setDoubleValue((double)obj);
				break;
			case BOOLEAN:
				builder.setBoolValue((boolean)obj);
				break;
			case STRING:
				builder.setStringValue((String)obj);
				break;
			case BINARY:
				builder.setBinaryValue(ByteString.copyFrom((byte[])obj));
				break;
			case DATETIME:
				builder.setDatetimeValue(LocalDateTimes.toUtcMillis((LocalDateTime)obj));
				break;
			case DATE:
				builder.setDateValue(LocalDates.toEpochMillis((LocalDate)obj));
				break;
			case TIME:
				builder.setTimeValue(LocalTimes.toString((LocalTime)obj));
				break;
			case COORDINATE:
				builder.setCoordinateValue(PBUtils.toProto((Coordinate)obj));
				break;
			case ENVELOPE:
				builder.setEnvelopeValue(PBUtils.toProto((Envelope)obj));
				break;
			case MULTI_POINT:
			case LINESTRING:
			case MULTI_LINESTRING:
			case POLYGON:
			case MULTI_POLYGON:
			case GEOM_COLLECTION:
			case GEOMETRY:
				builder.setGeometryValue(PBUtils.toProto((Geometry)obj));
				break;
			default:
				throw new AssertionError();
		}
		
		return builder.build();
	}

	//
	// TypeCode 정보가 없는 경우의 Object transform
	//
	public static ValueProto toValueProto(Object obj) {
		if ( obj == null ) {
			return ValueProto.newBuilder().build();
		}
		
		ValueProto.Builder builder = ValueProto.newBuilder();
		if ( obj instanceof String ) {
			builder.setStringValue((String)obj);
		}
		else if ( obj instanceof Integer ) {
			builder.setIntValue((int)obj);
		}
		else if ( obj instanceof Double ) {
			builder.setDoubleValue((double)obj);
		}
		else if ( obj instanceof Long ) {
			builder.setLongValue((long)obj);
		}
		else if ( obj instanceof Boolean ) {
			builder.setBoolValue((boolean)obj);
		}
		else if ( obj instanceof Coordinate ) {
			builder.setCoordinateValue(PBUtils.toProto((Coordinate)obj));
		}
		else if ( obj instanceof Geometry ) {
			builder.setGeometryValue(PBUtils.toProto((Geometry)obj));
		}
		else if ( obj instanceof Envelope ) {
			builder.setEnvelopeValue(PBUtils.toProto((Envelope)obj));
		}
		else if ( obj instanceof Byte[] ) {
			builder.setBinaryValue(ByteString.copyFrom((byte[])obj));
		}
		else if ( obj instanceof Byte ) {
			builder.setByteValue((byte)obj);
		}
		else if ( obj instanceof Short ) {
			builder.setShortValue((short)obj);
		}
		else if ( obj instanceof Float ) {
			builder.setFloatValue((float)obj);
		}
		else if ( obj instanceof LocalDateTime ) {
			builder.setDatetimeValue(Utilities.toUTCEpocMillis((LocalDateTime)obj));
		}
		else if ( obj instanceof LocalDate ) {
			builder.setDateValue(LocalDates.toEpochMillis((LocalDate)obj));
		}
		else if ( obj instanceof LocalTime ) {
			builder.setTimeValue(((LocalTime)obj).toString());
		}
		else {
			throw new AssertionError();
		}
		
		return builder.build();
	}
	
	//
	//
	//

	public static Tuple<DataType,Object> fromProto(ValueProto proto) {
		switch ( proto.getValueCase() ) {
			case BYTE_VALUE:
				return Tuple.of(DataType.BYTE, (byte)proto.getByteValue());
			case SHORT_VALUE:
				return Tuple.of(DataType.SHORT, (short)proto.getShortValue());
			case INT_VALUE:
				return Tuple.of(DataType.INT, (int)proto.getIntValue());
			case LONG_VALUE:
				return Tuple.of(DataType.LONG, proto.getLongValue());
			case FLOAT_VALUE:
				return Tuple.of(DataType.FLOAT, proto.getFloatValue());
			case DOUBLE_VALUE:
				return Tuple.of(DataType.DOUBLE, proto.getDoubleValue());
			case BOOL_VALUE:
				return Tuple.of(DataType.BOOLEAN, proto.getBoolValue());
			case STRING_VALUE:
				return Tuple.of(DataType.STRING, proto.getStringValue());
			case BINARY_VALUE:
				return Tuple.of(DataType.BINARY, proto.getBinaryValue().toByteArray());
				
			case DATETIME_VALUE:
				return Tuple.of(DataType.DATETIME, LocalDateTimes.fromUtcMillis(proto.getDatetimeValue()));
			case DATE_VALUE:
				return Tuple.of(DataType.DATE, LocalDates.fromEpochMillis(proto.getDateValue()));
			case TIME_VALUE:
				return Tuple.of(DataType.TIME, LocalTimes.fromString(proto.getTimeValue()));
			
			case COORDINATE_VALUE:
				
			case ENVELOPE_VALUE:
				return Tuple.of(DataType.ENVELOPE, PBUtils.fromProto(proto.getEnvelopeValue()));
			case GEOMETRY_VALUE:
				Geometry geom = PBUtils.fromProto(proto.getGeometryValue());
				DataType type = GeometryDataType.fromGeometry(geom);
				return Tuple.of(type, geom);
			case NULL_VALUE:
				TypeCode tc = TypeCode.valueOf(proto.getNullValue().name());
				return Tuple.of(DataType.fromTypeCode(tc), null);
			case VALUE_NOT_SET:
				return Tuple.of(null, null);
			default:
				throw new AssertionError();
		}
	}
}
