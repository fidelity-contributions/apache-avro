/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.reflect;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

import java.io.IOException;
import java.lang.reflect.Field;

class FieldAccessReflect extends FieldAccess {

  @Override
  protected FieldAccessor getAccessor(Field field) {
    AvroEncode enc = ReflectionUtil.getAvroEncode(field);
    if (enc != null)
      try {
        return new ReflectionBasesAccessorCustomEncoded(field, enc.using().getDeclaredConstructor().newInstance());
      } catch (Exception e) {
        throw new AvroRuntimeException("Could not instantiate custom Encoding");
      }
    return new ReflectionBasedAccessor(field);
  }

  private static class ReflectionBasedAccessor extends FieldAccessor {
    protected final Field field;
    private final boolean isStringable;
    private final boolean isCustomEncoded;

    public ReflectionBasedAccessor(Field field) {
      this.field = field;
      this.field.setAccessible(true);
      isStringable = field.isAnnotationPresent(Stringable.class);
      isCustomEncoded = ReflectionUtil.getAvroEncode(field) != null;
    }

    @Override
    public String toString() {
      return field.getName();
    }

    @Override
    public Object get(Object object) throws IllegalAccessException {
      return field.get(object);
    }

    @Override
    public void set(Object object, Object value) throws IllegalAccessException, IOException {
      if (value == null && field.getType().isPrimitive()) {
        Object defaultValue = null;
        if (int.class.equals(field.getType())) {
          defaultValue = INT_DEFAULT_VALUE;
        } else if (float.class.equals(field.getType())) {
          defaultValue = FLOAT_DEFAULT_VALUE;
        } else if (short.class.equals(field.getType())) {
          defaultValue = SHORT_DEFAULT_VALUE;
        } else if (byte.class.equals(field.getType())) {
          defaultValue = BYTE_DEFAULT_VALUE;
        } else if (boolean.class.equals(field.getType())) {
          defaultValue = BOOLEAN_DEFAULT_VALUE;
        } else if (char.class.equals(field.getType())) {
          defaultValue = CHAR_DEFAULT_VALUE;
        } else if (long.class.equals(field.getType())) {
          defaultValue = LONG_DEFAULT_VALUE;
        } else if (double.class.equals(field.getType())) {
          defaultValue = DOUBLE_DEFAULT_VALUE;
        }
        field.set(object, defaultValue);
      } else {
        field.set(object, value);
      }
    }

    @Override
    protected Field getField() {
      return field;
    }

    @Override
    protected boolean isStringable() {
      return isStringable;
    }

    @Override
    protected boolean isCustomEncoded() {
      return isCustomEncoded;
    }
  }

  private static final class ReflectionBasesAccessorCustomEncoded extends ReflectionBasedAccessor {

    private final CustomEncoding<?> encoding;

    public ReflectionBasesAccessorCustomEncoded(Field f, CustomEncoding<?> encoding) {
      super(f);
      this.encoding = encoding;
    }

    @Override
    protected void read(Object object, Decoder in) throws IOException {
      try {
        field.set(object, encoding.read(in));
      } catch (IllegalAccessException e) {
        throw new AvroRuntimeException(e);
      }
    }

    @Override
    protected void write(Object object, Encoder out) throws IOException {
      try {
        encoding.write(field.get(object), out);
      } catch (IllegalAccessException e) {
        throw new AvroRuntimeException(e);
      }
    }

    @Override
    protected boolean isCustomEncoded() {
      return true;
    }

    @Override
    protected boolean supportsIO() {
      return true;
    }
  }
}
