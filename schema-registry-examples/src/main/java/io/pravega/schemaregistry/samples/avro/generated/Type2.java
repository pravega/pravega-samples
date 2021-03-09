/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package io.pravega.schemaregistry.samples.avro.generated;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;

@org.apache.avro.specific.AvroGenerated
public class Type2 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -3170325568730780362L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Type2\",\"namespace\":\"io.pravega.schemaregistry.samples.avro\",\"fields\":[{\"name\":\"c\",\"type\":\"string\"},{\"name\":\"d\",\"type\":\"int\"},{\"name\":\"e\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<Type2> ENCODER =
      new BinaryMessageEncoder<Type2>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Type2> DECODER =
      new BinaryMessageDecoder<Type2>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<Type2> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<Type2> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<Type2> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<Type2>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this Type2 to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a Type2 from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a Type2 instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static Type2 fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private CharSequence c;
   private int d;
   private CharSequence e;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Type2() {}

  /**
   * All-args constructor.
   * @param c The new value for c
   * @param d The new value for d
   * @param e The new value for e
   */
  public Type2(CharSequence c, Integer d, CharSequence e) {
    this.c = c;
    this.d = d;
    this.e = e;
  }

  public SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return c;
    case 1: return d;
    case 2: return e;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: c = (CharSequence)value$; break;
    case 1: d = (Integer)value$; break;
    case 2: e = (CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'c' field.
   * @return The value of the 'c' field.
   */
  public CharSequence getC() {
    return c;
  }


  /**
   * Sets the value of the 'c' field.
   * @param value the value to set.
   */
  public void setC(CharSequence value) {
    this.c = value;
  }

  /**
   * Gets the value of the 'd' field.
   * @return The value of the 'd' field.
   */
  public int getD() {
    return d;
  }


  /**
   * Sets the value of the 'd' field.
   * @param value the value to set.
   */
  public void setD(int value) {
    this.d = value;
  }

  /**
   * Gets the value of the 'e' field.
   * @return The value of the 'e' field.
   */
  public CharSequence getE() {
    return e;
  }


  /**
   * Sets the value of the 'e' field.
   * @param value the value to set.
   */
  public void setE(CharSequence value) {
    this.e = value;
  }

  /**
   * Creates a new Type2 RecordBuilder.
   * @return A new Type2 RecordBuilder
   */
  public static Type2.Builder newBuilder() {
    return new Type2.Builder();
  }

  /**
   * Creates a new Type2 RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Type2 RecordBuilder
   */
  public static Type2.Builder newBuilder(Type2.Builder other) {
    if (other == null) {
      return new Type2.Builder();
    } else {
      return new Type2.Builder(other);
    }
  }

  /**
   * Creates a new Type2 RecordBuilder by copying an existing Type2 instance.
   * @param other The existing instance to copy.
   * @return A new Type2 RecordBuilder
   */
  public static Type2.Builder newBuilder(Type2 other) {
    if (other == null) {
      return new Type2.Builder();
    } else {
      return new Type2.Builder(other);
    }
  }

  /**
   * RecordBuilder for Type2 instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Type2>
    implements org.apache.avro.data.RecordBuilder<Type2> {

    private CharSequence c;
    private int d;
    private CharSequence e;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Type2.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.c)) {
        this.c = data().deepCopy(fields()[0].schema(), other.c);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.d)) {
        this.d = data().deepCopy(fields()[1].schema(), other.d);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.e)) {
        this.e = data().deepCopy(fields()[2].schema(), other.e);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing Type2 instance
     * @param other The existing instance to copy.
     */
    private Builder(Type2 other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.c)) {
        this.c = data().deepCopy(fields()[0].schema(), other.c);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.d)) {
        this.d = data().deepCopy(fields()[1].schema(), other.d);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.e)) {
        this.e = data().deepCopy(fields()[2].schema(), other.e);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'c' field.
      * @return The value.
      */
    public CharSequence getC() {
      return c;
    }


    /**
      * Sets the value of the 'c' field.
      * @param value The value of 'c'.
      * @return This builder.
      */
    public Type2.Builder setC(CharSequence value) {
      validate(fields()[0], value);
      this.c = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'c' field has been set.
      * @return True if the 'c' field has been set, false otherwise.
      */
    public boolean hasC() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'c' field.
      * @return This builder.
      */
    public Type2.Builder clearC() {
      c = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'd' field.
      * @return The value.
      */
    public int getD() {
      return d;
    }


    /**
      * Sets the value of the 'd' field.
      * @param value The value of 'd'.
      * @return This builder.
      */
    public Type2.Builder setD(int value) {
      validate(fields()[1], value);
      this.d = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'd' field has been set.
      * @return True if the 'd' field has been set, false otherwise.
      */
    public boolean hasD() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'd' field.
      * @return This builder.
      */
    public Type2.Builder clearD() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'e' field.
      * @return The value.
      */
    public CharSequence getE() {
      return e;
    }


    /**
      * Sets the value of the 'e' field.
      * @param value The value of 'e'.
      * @return This builder.
      */
    public Type2.Builder setE(CharSequence value) {
      validate(fields()[2], value);
      this.e = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'e' field has been set.
      * @return True if the 'e' field has been set, false otherwise.
      */
    public boolean hasE() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'e' field.
      * @return This builder.
      */
    public Type2.Builder clearE() {
      e = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Type2 build() {
      try {
        Type2 record = new Type2();
        record.c = fieldSetFlags()[0] ? this.c : (CharSequence) defaultValue(fields()[0]);
        record.d = fieldSetFlags()[1] ? this.d : (Integer) defaultValue(fields()[1]);
        record.e = fieldSetFlags()[2] ? this.e : (CharSequence) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Type2>
    WRITER$ = (org.apache.avro.io.DatumWriter<Type2>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Type2>
    READER$ = (org.apache.avro.io.DatumReader<Type2>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.c);

    out.writeInt(this.d);

    out.writeString(this.e);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.c = in.readString(this.c instanceof Utf8 ? (Utf8)this.c : null);

      this.d = in.readInt();

      this.e = in.readString(this.e instanceof Utf8 ? (Utf8)this.e : null);

    } else {
      for (int i = 0; i < 3; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.c = in.readString(this.c instanceof Utf8 ? (Utf8)this.c : null);
          break;

        case 1:
          this.d = in.readInt();
          break;

        case 2:
          this.e = in.readString(this.e instanceof Utf8 ? (Utf8)this.e : null);
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}









