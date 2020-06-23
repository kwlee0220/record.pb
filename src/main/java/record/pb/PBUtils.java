package record.pb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import proto.CoordinateProto;
import proto.EnvelopeProto;
import proto.ErrorProto;
import proto.ErrorProto.Code;
import proto.GeometryProto;
import proto.Int64Proto;
import proto.StringProto;
import proto.TypeCodeProto;
import proto.VoidProto;
import proto.VoidResponse;
import proto.stream.DownMessage;
import proto.stream.UpMessage;
import record.type.GeometryCollectionType;
import record.type.GeometryDataType;
import record.type.GeometryType;
import record.type.LineStringType;
import record.type.MultiLineStringType;
import record.type.MultiPointType;
import record.type.MultiPolygonType;
import record.type.PointType;
import record.type.PolygonType;
import record.type.TypeCode;
import utils.CSV;
import utils.Throwables;
import utils.func.CheckedRunnable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBUtils {
	private PBUtils() {
		throw new AssertionError("Should not be called here: class=" + getClass());
	}
	
	public static final VoidProto VOID = VoidProto.newBuilder().build();
	public static final VoidResponse VOID_RESPONSE = VoidResponse.newBuilder()
																	.setValue(VOID)
																	.build();
	
	public static void handle(VoidResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return;
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static void replyVoid(CheckedRunnable runnable, StreamObserver<VoidResponse> response) {
		try {
			runnable.run();
			response.onNext(VOID_RESPONSE);
		}
		catch ( Throwable e ) {
			response.onNext(VoidResponse.newBuilder()
										.setError(PBUtils.ERROR(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static StringProto STRING(String str) {
		return StringProto.newBuilder().setValue(str).build();
	}
	public static String STRING(ByteString bstr) throws InvalidProtocolBufferException {
		return StringProto.parseFrom(bstr).getValue();
	}
	
	public static ByteString BYTE_STRING(String str) {
		return StringProto.newBuilder().setValue(str).build().toByteString();
	}
	public static ByteString BYTE_STRING(long value) {
		return Int64Proto.newBuilder().setValue(value).build().toByteString();
	}
	
	public static Int64Proto INT64(long value) {
		return Int64Proto.newBuilder().setValue(value).build();
	}
	
	public static ErrorProto ERROR(Code code, String details) {
		if ( details == null ) {
			details = "";
		}
		
		return ErrorProto.newBuilder().setCode(code).setDetails(details).build();
	}
	
	public static ErrorProto ERROR(Throwable e) {
		if ( e instanceof CancellationException ) {
			return ERROR(Code.CANCELLED, e.getMessage());
		}
		else if ( e instanceof IllegalArgumentException ) {
			return ERROR(Code.INVALID_ARGUMENT, e.getMessage());
		}
		else if ( e instanceof IllegalStateException ) {
			return ERROR(Code.INVALID_STATE, e.getMessage());
		}
		else if ( e instanceof NotFoundException ) {
			return ERROR(Code.NOT_FOUND, e.getMessage());
		}
		else if ( e instanceof AlreadyExistsException ) {
			return ERROR(Code.ALREADY_EXISTS, e.getMessage());
		}
		else if ( e instanceof TimeoutException ) {
			return ERROR(Code.TIMEOUT, e.getMessage());
		}
		else if ( e instanceof IOException || e instanceof UncheckedIOException ) {
			return ERROR(Code.IO_ERROR, e.getMessage());
		}
		else if ( e instanceof StatusException ) {
			Status status = ((StatusException)e).getStatus();
			return ERROR(Code.GRPC_STATUS, status.getCode().name() + ":" + status.getDescription());
		}
		else if ( e instanceof StatusRuntimeException ) {
			Status status = ((StatusRuntimeException)e).getStatus();
			return ERROR(Code.GRPC_STATUS, status.getCode().name() + ":" + status.getDescription());
		}
		else if ( e instanceof InternalException ) {
			return ERROR(Code.INTERNAL, e.getMessage());
		}
		else {
			return ERROR(Code.INTERNAL, e.getMessage());
		}
	}
	
	public static Exception toException(ErrorProto error) {
		switch ( error.getCode() ) {
			case CANCELLED:
				return new CancellationException(error.getDetails());
			case INVALID_ARGUMENT:
				return new IllegalArgumentException(error.getDetails());
			case INVALID_STATE:
				return new IllegalStateException(error.getDetails());
			case NOT_FOUND:
				return new NotFoundException(error.getDetails());
			case ALREADY_EXISTS:
				return new AlreadyExistsException(error.getDetails());
			case TIMEOUT:
				return new TimeoutException(error.getDetails());
			case IO_ERROR:
				return new IOException(error.getDetails());
			case GRPC_STATUS:
				String[] parts = CSV.parseCsv(error.getDetails(), ':').toArray(String.class);
				Status.Code code = Status.Code.valueOf(parts[0]);
				String desc = parts[1];
				return Status.fromCode(code).withDescription(desc).asRuntimeException();
			case INTERNAL:
				return new InternalException(error.getDetails());
			default:
				return new RuntimeException(error.getDetails());
		}
	}
	
	public static VoidResponse ERROR_RESPONSE(ErrorProto error) {
		return VoidResponse.newBuilder().setError(error).build();
	}

	public static boolean isCancelled(ErrorProto error) {
		return error.getCode() == ErrorProto.Code.CANCELLED;
	}
	
//	public static Status getStatus(Throwable e) {
//		if ( e instanceof StatusRuntimeException ) {
//			return ((StatusRuntimeException)e).getStatus();
//		}
//		else if ( e instanceof StatusException ) {
//			return ((StatusException)e).getStatus();
//		}
//		throw new AssertionError("not StatusException: " + e);
//	}
//
//	public static boolean isOperationCancelled(Throwable cause) {
//		Status status = getStatus(cause);
//		return status.getCode() == Status.CANCELLED.getCode();
//	}
//	
//	public static StatusRuntimeException toStatusRuntimeException(Throwable cause) {
//		if ( cause instanceof StatusRuntimeException ) {
//			return (StatusRuntimeException)cause;
//		}
//		else if ( cause instanceof StatusException ) {
//			return ((StatusException)cause).getStatus().asRuntimeException();
//		}
//		else {
//			return INTERNAL_ERROR(cause);
//		}
//	}
//	
//	public static StatusRuntimeException CANCELLED() {
//		return Status.CANCELLED.withDescription("operation cancelled").asRuntimeException();
//	}
//	
//	public static StatusRuntimeException INTERNAL_ERROR(Throwable cause) {
//		return Status.INTERNAL.withDescription("" + cause).asRuntimeException();
//	}
//	
//	public static StatusRuntimeException INTERNAL_ERROR(String desc) {
//		return Status.INTERNAL.withDescription(desc).asRuntimeException();
//	}
//	
//	public static DownMessage toResultDownMessage(ByteString chunk) {
//		return DownMessage.newBuilder().setResult(chunk).build();
//	}
//	public static DownMessage toErrorDownMessage(Throwable error) {
//		return DownMessage.newBuilder().setError(ERROR(error)).build();
//	}
	
	public static final DownMessage EMPTY_DOWN_MESSAGE = DownMessage.newBuilder().setDummy(VOID).build();
	public static final UpMessage EMPTY_UP_MESSAGE = UpMessage.newBuilder().setDummy(VOID).build();
	
/*
	public static final SerializedProto serialize(Object obj) {
		if ( obj instanceof PBSerializable ) {
			return ((PBSerializable<?>)obj).serialize();
		}
		else if ( obj instanceof Message ) {
			return PBUtils.serialize((Message)obj);
		}
		else if ( obj instanceof Serializable ) {
			return PBUtils.serializeJava((Serializable)obj);
		}
		else {
			throw new IllegalStateException("unable to serialize: " + obj);
		}
	}
	
	public static final SerializedProto serializeJava(Serializable obj) {
		try {
			JavaSerializedProto proto = JavaSerializedProto.newBuilder()
										.setSerialized(ByteString.copyFrom(IOUtils.serialize(obj)))
										.build();
			return SerializedProto.newBuilder()
									.setJava(proto)
									.build();
		}
		catch ( Exception e ) {
			throw new PBException("fails to serialize object: proto=" + obj, e);
		}
	}
	
	public static final SerializedProto serialize(Message proto) {
		ProtoBufSerializedProto serialized = ProtoBufSerializedProto.newBuilder()
										.setProtoClass(proto.getClass().getName())
										.setSerialized(proto.toByteString())
										.build();
		return SerializedProto.newBuilder()
								.setProtoBuf(serialized)
								.build();
	}
	
	public static final <T> T deserialize(SerializedProto proto) {
		switch ( proto.getMethodCase() ) {
			case PROTO_BUF:
				return deserialize(proto.getProtoBuf());
			case JAVA:
				return deserialize(proto.getJava());
			default:
				throw new AssertionError("unregistered serialization method: method="
										+ proto.getMethodCase());
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T deserialize(JavaSerializedProto proto) {
		try {
			return (T)IOUtils.deserialize(proto.getSerialized().toByteArray());
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw new PBException("fails to deserialize: proto=" + proto, cause);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static final <T> T deserialize(ProtoBufSerializedProto proto) {
		try {
			ByteString serialized = proto.getSerialized();
			
			FOption<String> clsName = getOptionField(proto, "object_class");
			Class<?> protoCls = Class.forName(proto.getProtoClass());

			Method parseFrom = protoCls.getMethod("parseFrom", ByteString.class);
			Message optorProto = (Message)parseFrom.invoke(null, serialized);
			
			if ( clsName.isPresent() ) {
				Class<?> cls = Class.forName(clsName.get());
				Method fromProto = cls.getMethod("fromProto", protoCls);
				return (T)fromProto.invoke(null, optorProto);
			}
			else {
				return (T)ProtoBufActivator.activate(optorProto);
			}
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw new PBException("fails to deserialize: proto=" + proto + ", cause=" + cause, cause);
		}
	}
	
	public static Enum<?> getCase(Message proto, String field) {
		try {
			String partName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, field);
			Method getCase = proto.getClass().getMethod("get" + partName + "Case", new Class<?>[0]);
			return (Enum<?>)getCase.invoke(proto, new Object[0]);
		}
		catch ( Exception e ) {
			throw new PBException("fails to get the case " + field, e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> FOption<T> getOptionField(Message proto, String field) {
		try {
			return FOption.ofNullable((T)KVFStream.from(proto.getAllFields())
												.filter(kv -> kv.key().getName().equals(field))
												.next()
												.map(kv -> kv.value())
												.getOrNull());
					}
		catch ( Exception e ) {
			throw new PBException("fails to get the field " + field, e);
		}
	}

	public static FOption<String> getStringOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(String.class);
	}

	public static FOption<Double> getDoubleOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Double.class);
	}

	public static FOption<Long> geLongOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Long.class);
	}

	public static FOption<Integer> getIntOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Integer.class);
	}

	@SuppressWarnings("unchecked")
	public static <T> T getField(Message proto, String field) {
		try {
			return(T)KVFStream.from(proto.getAllFields())
									.filter(kv -> kv.key().getName().equals(field))
									.next()
									.map(kv -> kv.value())
									.getOrElseThrow(()
										-> new PBException("unknown field: name=" + field
																	+ ", msg=" + proto));
		}
		catch ( Exception e ) {
			throw new PBException("fails to get the field " + field, e);
		}
	}
	
	public static VoidResponse toVoidResponse() {
		return VOID_RESPONSE;
	}
	
	public static VoidResponse toVoidResponse(Throwable e) {
		return VoidResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static StringResponse toStringResponse(Throwable e) {
		return StringResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static StringResponse toStringResponse(String value) {
		return StringResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static String getValue(StringResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static BoolResponse toBoolResponse(boolean value) {
		return BoolResponse.newBuilder()
							.setValue(value)
							.build();
	}
	public static BoolResponse toBoolResponse(Throwable e) {
		return BoolResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	public static LongResponse toLongResponse(Throwable e) {
		return LongResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static FloatResponse toFloatResponse(float value) {
		return FloatResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static FloatResponse toFloatResponse(Throwable e) {
		return FloatResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static DoubleResponse toDoubleResponse(double value) {
		return DoubleResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static DoubleResponse toDoubleResponse(Throwable e) {
		return DoubleResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static RecordResponse toRecordResponse(Record value) {
		return RecordResponse.newBuilder()
							.setRecord(value.toProto())
							.build();
	}
	
	public static RecordResponse toRecordResponse(Throwable e) {
		return RecordResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}

	public static <T extends Message> FStream<T> toFStream(Iterator<T> respIter) {
		if ( !respIter.hasNext() ) {
			// Iterator가 empty인 경우는 예외가 발생하지 않았고, 결과가 없는 경우를
			// 의미하기 때문에 empty FStream을 반환한다.
			return FStream.empty();
		}
		
		PeekingIterator<T> piter = Iterators.peekingIterator(respIter);
		T proto = piter.peek();
		FOption<ErrorProto> error = getOptionField(proto, "error");
		if ( error.isPresent() ) {
			throw Throwables.toRuntimeException(toException(error.get()));
		}
		
		return FStream.from(piter);
	}
	
	public static void handle(VoidResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VOID:
				return;
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static String handle(StringResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static boolean handle(BoolResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static long handle(LongResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static float handle(FloatResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static double handle(DoubleResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static <X extends Throwable> void replyBoolean(CheckedSupplier<Boolean> supplier,
									StreamObserver<BoolResponse> response) {
		try {
			boolean done = supplier.get();
			response.onNext(BoolResponse.newBuilder()
										.setValue(done)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(BoolResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static <X extends Throwable> void replyLong(CheckedSupplier<Long> supplier,
							StreamObserver<LongResponse> response) {
		try {
			long ret = supplier.get();
			response.onNext(LongResponse.newBuilder()
										.setValue(ret)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(LongResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static <X extends Throwable> void replyString(CheckedSupplier<String> supplier,
									StreamObserver<StringResponse> response) {
		try {
			String ret = supplier.get();
			response.onNext(StringResponse.newBuilder()
										.setValue(ret)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(StringResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static void replyVoid(CheckedRunnable runnable,
									StreamObserver<VoidResponse> response) {
		try {
			runnable.run();
			response.onNext(VoidResponse.newBuilder()
										.setVoid(VOID)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(VoidResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static byte[] toDelimitedBytes(Message proto) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			proto.writeTo(baos);
		}
		catch ( IOException e ) {
			throw new RuntimeException(e);
		}
		finally {
			IOUtils.closeQuietly(baos);
		}
		
		return baos.toByteArray();
	}

	public static boolean fromProto(BoolProto proto) {
		return proto.getValue();
	}
	
	public static BoolProto toProto(boolean value) {
		return BoolProto.newBuilder().setValue(value).build();
	}
	
	public static Size2i fromProto(Size2iProto proto) {
		return new Size2i(proto.getWidth(), proto.getHeight());
	}
	
	public static Size2iProto toProto(Size2i dim) {
		return Size2iProto.newBuilder()
									.setWidth(dim.getWidth())
									.setHeight(dim.getHeight())
									.build();
	}
	
	public static Size2d fromProto(Size2dProto proto) {
		return new Size2d(proto.getWidth(), proto.getHeight());
	}
	
	public static Size2dProto toProto(Size2d dim) {
		return Size2dProto.newBuilder()
						.setWidth(dim.getWidth())
						.setHeight(dim.getHeight())
						.build();
	}
	
	public static Interval fromProto(IntervalProto proto) {
		return Interval.between(proto.getStart(), proto.getEnd());
	}
	
	public static IntervalProto toProto(Interval intvl) {
		return IntervalProto.newBuilder()
							.setStart(intvl.getStartMillis())
							.setEnd(intvl.getEndMillis())
							.build();
	}
*/
	
	public static Coordinate fromProto(CoordinateProto proto) {
		return new Coordinate(proto.getX(), proto.getY());
	}
	
	public static CoordinateProto toProto(Coordinate coord) {
		return CoordinateProto.newBuilder()
								.setX(coord.x)
								.setY(coord.y)
								.build();
	}
	
	public static Envelope fromProto(EnvelopeProto proto) {
		return new Envelope(fromProto(proto.getTl()), fromProto(proto.getBr()));
	}
	
	public static EnvelopeProto toProto(Envelope envl) {
		return EnvelopeProto.newBuilder()
							.setTl(CoordinateProto.newBuilder()
												.setX(envl.getMinX())
												.setY(envl.getMinY())
												.build())
							.setBr(CoordinateProto.newBuilder()
												.setX(envl.getMaxX())
												.setY(envl.getMaxY())
												.build())
							.build();
	}

	public static TypeCodeProto toProto(TypeCode tc) {
		return TypeCodeProto.forNumber(tc.get());
	}
	public static TypeCode fromProto(TypeCodeProto proto) {
		return TypeCode.fromCode(proto.getNumber());
	}
	
	public static Geometry fromProto(GeometryProto proto) {
		switch ( proto.getEitherCase() ) {
			case POINT:
				return PointType.toPoint(fromProto(proto.getPoint()));
			case WKB:
				return GeometryDataType.fromWkb(proto.getWkb().toByteArray());
			case EMPTY_GEOM_TC:
				switch ( fromProto(proto.getEmptyGeomTc()) ) {
					case POINT: return PointType.EMPTY;
					case MULTI_POINT: return MultiPointType.EMPTY;
					case LINESTRING: return LineStringType.EMPTY;
					case MULTI_LINESTRING: return MultiLineStringType.EMPTY;
					case POLYGON: return PolygonType.EMPTY;
					case MULTI_POLYGON: return MultiPolygonType.EMPTY;
					case GEOM_COLLECTION: return GeometryCollectionType.EMPTY;
					case GEOMETRY: return GeometryType.EMPTY;
					default: throw new IllegalArgumentException("unexpected Geometry type: code=" + fromProto(proto.getEmptyGeomTc()));
				}
			case EITHER_NOT_SET:
				return null;
			default:
				throw new AssertionError();
		}
	}

	private static final GeometryProto NULL_GEOM = GeometryProto.newBuilder().build();
	public static GeometryProto toProto(Geometry geom) {
		if ( geom == null ) {
			return NULL_GEOM;
		}
		else if ( geom.isEmpty() ) {
			TypeCode tc = GeometryDataType.fromGeometry(geom).getTypeCode();
			TypeCodeProto tcProto = toProto(tc);
			return GeometryProto.newBuilder().setEmptyGeomTc(tcProto).build();
		}
		else if ( geom instanceof Point ) {
			Point pt = (Point)geom;
			return GeometryProto.newBuilder().setPoint(toProto(pt.getCoordinate())).build();
		}
		else {
			ByteString wkb = ByteString.copyFrom(GeometryDataType.toWkb(geom));
			return GeometryProto.newBuilder().setWkb(wkb).build();
		}
	}
}
