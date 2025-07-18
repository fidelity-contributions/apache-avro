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
package org.apache.avro.compiler.specific;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.SchemaParser;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.specific.SpecificData;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generate specific Java interfaces and classes for protocols and schemas.
 * <p>
 * Java reserved keywords are mangled to preserve compilation.
 */
public class SpecificCompiler {

  /*
   * From Section 4.10 of the Java VM Specification: A method descriptor is valid
   * only if it represents method parameters with a total length of 255 or less,
   * where that length includes the contribution for this in the case of instance
   * or interface method invocations. The total length is calculated by summing
   * the contributions of the individual parameters, where a parameter of type
   * long or double contributes two units to the length and a parameter of any
   * other type contributes one unit.
   *
   * Arguments of type Double/Float contribute 2 "parameter units" to this limit,
   * all other types contribute 1 "parameter unit". All instance methods for a
   * class are passed a reference to the instance (`this), and hence, they are
   * permitted at most `JVM_METHOD_ARG_LIMIT-1` "parameter units" for their
   * arguments.
   *
   * @see <a href=
   * "https://docs.oracle.com/javase/specs/jvms/se7/html/jvms-4.html#jvms-4.10">
   * JVM Spec: Section 4.10</a>
   */
  private static final int JVM_METHOD_ARG_LIMIT = 255;

  /*
   * Note: This is protected instead of private only so it's visible for testing.
   */
  protected static final int MAX_FIELD_PARAMETER_UNIT_COUNT = JVM_METHOD_ARG_LIMIT - 1;

  public enum FieldVisibility {
    PUBLIC, PRIVATE
  }

  void addLogicalTypeConversions(SpecificData specificData) {
    specificData.addLogicalTypeConversion(new TimeConversions.DateConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.TimestampNanosConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.LocalTimestampNanosConversion());
    specificData.addLogicalTypeConversion(new Conversions.UUIDConversion());
  }

  private final SpecificData specificData = new SpecificData();

  private final Set<Schema> queue = new HashSet<>();
  private Protocol protocol;
  private VelocityEngine velocityEngine;
  private String templateDir;
  private FieldVisibility fieldVisibility = FieldVisibility.PRIVATE;
  private boolean createOptionalGetters = false;
  private boolean gettersReturnOptional = false;
  private boolean optionalGettersForNullableFieldsOnly = false;
  private boolean createSetters = true;
  private boolean createNullSafeAnnotations = false;
  private boolean createAllArgsConstructor = true;
  private String outputCharacterEncoding;
  private boolean enableDecimalLogicalType = false;
  private String suffix = ".java";
  private List<Object> additionalVelocityTools = Collections.emptyList();

  private String nullSafeAnnotationNullable = "org.jetbrains.annotations.Nullable";
  private String nullSafeAnnotationNotNull = "org.jetbrains.annotations.NotNull";

  private String recordSpecificClass = "org.apache.avro.specific.SpecificRecordBase";

  private String errorSpecificClass = "org.apache.avro.specific.SpecificExceptionBase";

  /*
   * Used in the record.vm template.
   */
  public boolean isCreateAllArgsConstructor() {
    return createAllArgsConstructor;
  }

  private static final String FILE_HEADER = "/*\n * Autogenerated by Avro\n *\n * DO NOT EDIT DIRECTLY\n */\n";

  public SpecificCompiler(Protocol protocol) {
    this();
    // enqueue all types
    for (Schema s : protocol.getTypes()) {
      enqueue(s);
    }
    this.protocol = protocol;
  }

  public SpecificCompiler(Schema schema) {
    this(Collections.singleton(schema));
  }

  public SpecificCompiler(Collection<Schema> schemas) {
    this();
    for (Schema schema : schemas) {
      enqueue(schema);
    }
    this.protocol = null;
  }

  public SpecificCompiler(Iterable<Schema> schemas) {
    this();
    schemas.forEach(this::enqueue);
    this.protocol = null;
  }

  /**
   * Creates a specific compiler with the given type to use for date/time related
   * logical types.
   */
  SpecificCompiler() {
    this.templateDir = System.getProperty("org.apache.avro.specific.templates",
        "/org/apache/avro/compiler/specific/templates/java/classic/");
    initializeVelocity();
    initializeSpecificData();
  }

  /**
   * Set additional Velocity tools (simple POJOs) to be injected into the Velocity
   * template context.
   */
  public void setAdditionalVelocityTools(List<Object> additionalVelocityTools) {
    this.additionalVelocityTools = additionalVelocityTools;
  }

  /**
   * Set the resource directory where templates reside. First, the compiler checks
   * the system path for the specified file, if not it is assumed that it is
   * present on the classpath.
   */
  public void setTemplateDir(String templateDir) {
    this.templateDir = templateDir;
  }

  /**
   * Set the resource file suffix, .java or .xxx
   */
  public void setSuffix(String suffix) {
    this.suffix = suffix;
  }

  /**
   * @return true if the record fields should be public
   */
  public boolean publicFields() {
    return this.fieldVisibility == FieldVisibility.PUBLIC;
  }

  /**
   * @return true if the record fields should be private
   */
  public boolean privateFields() {
    return this.fieldVisibility == FieldVisibility.PRIVATE;
  }

  /**
   * Sets the field visibility option.
   */
  public void setFieldVisibility(FieldVisibility fieldVisibility) {
    this.fieldVisibility = fieldVisibility;
  }

  public boolean isCreateSetters() {
    return this.createSetters;
  }

  /**
   * Set to false to not create setter methods for the fields of the record.
   */
  public void setCreateSetters(boolean createSetters) {
    this.createSetters = createSetters;
  }

  public boolean isCreateNullSafeAnnotations() {
    return this.createNullSafeAnnotations;
  }

  /**
   * Set to true to add @Nullable and @NotNull annotations. By default, JetBrains
   * annotations are used (org.jetbrains.annotations.Nullable and
   * org.jetbrains.annotations.NotNull) but this can be overridden using
   * {@link #setNullSafeAnnotationNullable)} and
   * {@link #setNullSafeAnnotationNotNull)}.
   */
  public void setCreateNullSafeAnnotations(boolean createNullSafeAnnotations) {
    this.createNullSafeAnnotations = createNullSafeAnnotations;
  }

  public String getNullSafeAnnotationNullable() {
    return this.nullSafeAnnotationNullable;
  }

  /**
   * Sets the annotation to use for nullable fields. Default is
   * "org.jetbrains.annotations.Nullable". The annotation must include the full
   * package path.
   */
  public void setNullSafeAnnotationNullable(String nullSafeAnnotationNullable) {
    this.nullSafeAnnotationNullable = nullSafeAnnotationNullable;
  }

  public String getNullSafeAnnotationNotNull() {
    return this.nullSafeAnnotationNotNull;
  }

  /**
   * Sets the annotation to use for non-nullable fields. Default is
   * "org.jetbrains.annotations.NotNull". The annotation must include the full
   * package path.
   */
  public void setNullSafeAnnotationNotNull(String nullSafeAnnotationNotNull) {
    this.nullSafeAnnotationNotNull = nullSafeAnnotationNotNull;
  }

  public boolean isCreateOptionalGetters() {
    return this.createOptionalGetters;
  }

  /**
   * Set to false to not create the getters that return an Optional.
   */
  public void setCreateOptionalGetters(boolean createOptionalGetters) {
    this.createOptionalGetters = createOptionalGetters;
  }

  public boolean isGettersReturnOptional() {
    return this.gettersReturnOptional;
  }

  /**
   * Set to false to not create the getters that return an Optional.
   */
  public void setGettersReturnOptional(boolean gettersReturnOptional) {
    this.gettersReturnOptional = gettersReturnOptional;
  }

  public boolean isOptionalGettersForNullableFieldsOnly() {
    return optionalGettersForNullableFieldsOnly;
  }

  /**
   * Set to true to create the Optional getters only for nullable fields.
   */
  public void setOptionalGettersForNullableFieldsOnly(boolean optionalGettersForNullableFieldsOnly) {
    this.optionalGettersForNullableFieldsOnly = optionalGettersForNullableFieldsOnly;
  }

  /**
   * Set to true to use {@link java.math.BigDecimal} instead of
   * {@link java.nio.ByteBuffer} for logical type "decimal"
   */
  public void setEnableDecimalLogicalType(boolean enableDecimalLogicalType) {
    this.enableDecimalLogicalType = enableDecimalLogicalType;
  }

  public void addCustomConversion(Class<?> conversionClass) {
    try {
      final Conversion<?> conversion = (Conversion<?>) conversionClass.getDeclaredConstructor().newInstance();
      specificData.addLogicalTypeConversion(conversion);
    } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException("Failed to instantiate conversion class " + conversionClass, e);
    }
  }

  public Collection<String> getUsedConversionClasses(Schema schema) {
    Collection<String> result = new HashSet<>();
    for (Conversion<?> conversion : getUsedConversions(schema)) {
      result.add(conversion.getClass().getCanonicalName());
    }
    return result;
  }

  public Map<String, String> getUsedCustomLogicalTypeFactories(Schema schema) {
    final Set<String> logicalTypeNames = getUsedLogicalTypes(schema).stream().map(LogicalType::getName)
        .collect(Collectors.toSet());

    return LogicalTypes.getCustomRegisteredTypes().entrySet().stream()
        .filter(entry -> logicalTypeNames.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getClass().getCanonicalName()));
  }

  private void collectUsedTypes(Schema schema, Set<Conversion<?>> conversionResults,
      Set<LogicalType> logicalTypeResults, Set<Schema> seenSchemas) {
    if (seenSchemas.contains(schema)) {
      return;
    }

    final LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);
    if (logicalTypeResults != null && logicalType != null)
      logicalTypeResults.add(logicalType);

    final Conversion<?> conversion = specificData.getConversionFor(logicalType);
    if (conversionResults != null && conversion != null)
      conversionResults.add(conversion);

    seenSchemas.add(schema);
    switch (schema.getType()) {
    case RECORD:
      for (Schema.Field field : schema.getFields()) {
        collectUsedTypes(field.schema(), conversionResults, logicalTypeResults, seenSchemas);
      }
      break;
    case MAP:
      collectUsedTypes(schema.getValueType(), conversionResults, logicalTypeResults, seenSchemas);
      break;
    case ARRAY:
      collectUsedTypes(schema.getElementType(), conversionResults, logicalTypeResults, seenSchemas);
      break;
    case UNION:
      for (Schema s : schema.getTypes())
        collectUsedTypes(s, conversionResults, logicalTypeResults, seenSchemas);
      break;
    case NULL:
    case ENUM:
    case FIXED:
    case STRING:
    case BYTES:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case BOOLEAN:
      break;
    default:
      throw new RuntimeException("Unknown type: " + schema);
    }
  }

  private Set<Conversion<?>> getUsedConversions(Schema schema) {
    final Set<Conversion<?>> conversionResults = new HashSet<>();
    collectUsedTypes(schema, conversionResults, null, new HashSet<>());
    return conversionResults;
  }

  private Set<LogicalType> getUsedLogicalTypes(Schema schema) {
    final Set<LogicalType> logicalTypeResults = new HashSet<>();
    collectUsedTypes(schema, null, logicalTypeResults, new HashSet<>());
    return logicalTypeResults;
  }

  private void initializeVelocity() {
    this.velocityEngine = new VelocityEngine();

    // These properties tell Velocity to use its own classpath-based
    // loader, then drop down to check the root and the current folder
    velocityEngine.addProperty("resource.loaders", "class, file");
    velocityEngine.addProperty("resource.loader.class.class",
        "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
    velocityEngine.addProperty("resource.loader.file.class",
        "org.apache.velocity.runtime.resource.loader.FileResourceLoader");
    velocityEngine.addProperty("resource.loader.file.path", "/, ., ");
    velocityEngine.setProperty("runtime.strict_mode.enable", true);

    // Set whitespace gobbling to Backward Compatible (BC)
    // https://velocity.apache.org/engine/2.0/developer-guide.html#space-gobbling
    velocityEngine.setProperty("parser.space_gobbling", "bc");
  }

  private void initializeSpecificData() {
    addLogicalTypeConversions(specificData);
    specificData.addLogicalTypeConversion(new Conversions.DecimalConversion());
  }

  /**
   * Captures output file path and contents.
   */
  static class OutputFile {
    String path;
    String contents;
    String outputCharacterEncoding;

    /**
     * Writes output to path destination directory when it is newer than src,
     * creating directories as necessary. Returns the created file.
     */
    File writeToDestination(File src, File destDir) throws IOException {
      File f = new File(destDir, path);
      if (src != null && f.exists() && f.lastModified() >= src.lastModified())
        return f; // already up to date: ignore
      f.getParentFile().mkdirs();
      Writer fw = null;
      FileOutputStream fos = null;
      try {
        if (outputCharacterEncoding != null) {
          fos = new FileOutputStream(f);
          fw = new OutputStreamWriter(fos, outputCharacterEncoding);
        } else {
          fw = Files.newBufferedWriter(f.toPath(), UTF_8);
        }
        fw.write(FILE_HEADER);
        fw.write(contents);
      } finally {
        if (fw != null)
          fw.close();
        if (fos != null)
          fos.close();
      }
      return f;
    }
  }

  /**
   * Generates Java interface and classes for a protocol.
   *
   * @param src  the source Avro protocol file
   * @param dest the directory to place generated files in
   */
  public static void compileProtocol(File src, File dest) throws IOException {
    compileProtocol(new File[] { src }, dest);
  }

  /**
   * Generates Java interface and classes for a number of protocol files.
   *
   * @param srcFiles the source Avro protocol files
   * @param dest     the directory to place generated files in
   */
  public static void compileProtocol(File[] srcFiles, File dest) throws IOException {
    for (File src : srcFiles) {
      Protocol protocol = Protocol.parse(src);
      SpecificCompiler compiler = new SpecificCompiler(protocol);
      compiler.compileToDestination(src, dest);
    }
  }

  /**
   * Generates Java classes for a schema.
   */
  public static void compileSchema(File src, File dest) throws IOException {
    compileSchema(new File[] { src }, dest);
  }

  /**
   * Generates Java classes for a number of schema files.
   */
  public static void compileSchema(File[] srcFiles, File dest) throws IOException {
    SchemaParser parser = new SchemaParser();

    for (File src : srcFiles) {
      parser.parse(src);
    }
    // FIXME: use lastModified() without causing a NoSuchMethodError in the build
    File lastModifiedSourceFile = Stream.of(srcFiles).max(Comparator.comparing(File::lastModified)).orElse(null);
    for (Schema schema : parser.getParsedNamedSchemas()) {
      SpecificCompiler compiler = new SpecificCompiler(schema);
      compiler.compileToDestination(lastModifiedSourceFile, dest);
    }
  }

  /**
   * Recursively enqueue schemas that need a class generated.
   */
  private void enqueue(Schema schema) {
    if (queue.contains(schema))
      return;
    switch (schema.getType()) {
    case RECORD:
      queue.add(schema);
      for (Schema.Field field : schema.getFields())
        enqueue(field.schema());
      break;
    case MAP:
      enqueue(schema.getValueType());
      break;
    case ARRAY:
      enqueue(schema.getElementType());
      break;
    case UNION:
      for (Schema s : schema.getTypes())
        enqueue(s);
      break;
    case ENUM:
    case FIXED:
      queue.add(schema);
      break;
    case STRING:
    case BYTES:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case BOOLEAN:
    case NULL:
      break;
    default:
      throw new RuntimeException("Unknown type: " + schema);
    }
  }

  /**
   * Generate java classes for enqueued schemas.
   */
  Collection<OutputFile> compile() {
    List<OutputFile> out = new ArrayList<>(queue.size() + 1);
    for (Schema schema : queue) {
      out.add(compile(schema));
    }
    if (protocol != null) {
      out.add(compileInterface(protocol));
    }
    return out;
  }

  /**
   * Generate output under dst, unless existing file is newer than src.
   */
  public void compileToDestination(File src, File dst) throws IOException {
    for (Schema schema : queue) {
      OutputFile o = compile(schema);
      o.writeToDestination(src, dst);
    }
    if (protocol != null) {
      compileInterface(protocol).writeToDestination(src, dst);
    }
  }

  private String renderTemplate(String templateName, VelocityContext context) {
    Template template;
    try {
      template = this.velocityEngine.getTemplate(templateName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    StringWriter writer = new StringWriter();
    template.merge(context, writer);
    return writer.toString();
  }

  OutputFile compileInterface(Protocol protocol) {
    protocol = addStringType(protocol); // annotate protocol as needed
    VelocityContext context = new VelocityContext();
    context.put("protocol", protocol);
    context.put("this", this);
    for (Object velocityTool : additionalVelocityTools) {
      String toolName = velocityTool.getClass().getSimpleName().toLowerCase();
      context.put(toolName, velocityTool);
    }
    String out = renderTemplate(templateDir + "protocol.vm", context);

    OutputFile outputFile = new OutputFile();
    String mangledName = mangleTypeIdentifier(protocol.getName());
    outputFile.path = makePath(mangledName, mangle(protocol.getNamespace()));
    outputFile.contents = out;
    outputFile.outputCharacterEncoding = outputCharacterEncoding;
    return outputFile;
  }

  // package private for testing purposes
  String makePath(String name, String space) {
    if (space == null || space.isEmpty()) {
      return name + suffix;
    } else {
      return space.replace('.', File.separatorChar) + File.separatorChar + name + suffix;
    }
  }

  /**
   * Returns the number of parameter units required by fields for the
   * AllArgsConstructor.
   *
   * @param record a Record schema
   */
  protected int calcAllArgConstructorParameterUnits(Schema record) {

    if (record.getType() != Schema.Type.RECORD)
      throw new RuntimeException("This method must only be called for record schemas.");

    return record.getFields().size();
  }

  protected void validateRecordForCompilation(Schema record) {
    this.createAllArgsConstructor = calcAllArgConstructorParameterUnits(record) <= MAX_FIELD_PARAMETER_UNIT_COUNT;

    if (!this.createAllArgsConstructor) {
      Logger logger = LoggerFactory.getLogger(SpecificCompiler.class);
      logger.warn("Record '" + record.getFullName() + "' contains more than " + MAX_FIELD_PARAMETER_UNIT_COUNT
          + " parameters which exceeds the JVM "
          + "spec for the number of permitted constructor arguments. Clients must "
          + "rely on the builder pattern to create objects instead. For more info " + "see JIRA ticket AVRO-1642.");
    }
  }

  OutputFile compile(Schema schema) {
    schema = addStringType(schema); // annotate schema as needed
    String output = "";
    VelocityContext context = new VelocityContext();
    context.put("this", this);
    context.put("schema", schema);
    for (Object velocityTool : additionalVelocityTools) {
      String toolName = velocityTool.getClass().getSimpleName().toLowerCase();
      context.put(toolName, velocityTool);
    }

    switch (schema.getType()) {
    case RECORD:
      validateRecordForCompilation(schema);
      output = renderTemplate(templateDir + "record.vm", context);
      break;
    case ENUM:
      output = renderTemplate(templateDir + "enum.vm", context);
      break;
    case FIXED:
      output = renderTemplate(templateDir + "fixed.vm", context);
      break;
    case BOOLEAN:
    case NULL:
      break;
    default:
      throw new RuntimeException("Unknown type: " + schema);
    }

    OutputFile outputFile = new OutputFile();
    String name = mangleTypeIdentifier(schema.getName());
    outputFile.path = makePath(name, mangle(schema.getNamespace()));
    outputFile.contents = output;
    outputFile.outputCharacterEncoding = outputCharacterEncoding;
    return outputFile;
  }

  private StringType stringType = StringType.CharSequence;

  /**
   * Set the Java type to be emitted for string schemas.
   */
  public void setStringType(StringType t) {
    this.stringType = t;
  }

  // annotate map and string schemas with string type
  private Protocol addStringType(Protocol p) {
    if (stringType != StringType.String)
      return p;

    Protocol newP = new Protocol(p.getName(), p.getDoc(), p.getNamespace());
    Map<Schema, Schema> types = new LinkedHashMap<>();

    p.forEachProperty(newP::addProp);

    // annotate types
    Collection<Schema> namedTypes = new LinkedHashSet<>();
    for (Schema s : p.getTypes())
      namedTypes.add(addStringType(s, types));
    newP.setTypes(namedTypes);

    // annotate messages
    Map<String, Message> newM = newP.getMessages();
    for (Message m : p.getMessages().values())
      newM.put(m.getName(),
          m.isOneWay() ? newP.createMessage(m, addStringType(m.getRequest(), types))
              : newP.createMessage(m, addStringType(m.getRequest(), types), addStringType(m.getResponse(), types),
                  addStringType(m.getErrors(), types)));
    return newP;
  }

  private Schema addStringType(Schema s) {
    if (stringType != StringType.String)
      return s;
    return addStringType(s, new HashMap<>());
  }

  // annotate map and string schemas with string type
  private Schema addStringType(Schema s, Map<Schema, Schema> seen) {
    if (seen.containsKey(s))
      return seen.get(s); // break loops
    Schema result = s;
    switch (s.getType()) {
    case STRING:
      result = Schema.create(Schema.Type.STRING);
      if (s.getLogicalType() == null) {
        GenericData.setStringType(result, stringType);
      }
      break;
    case RECORD:
      result = Schema.createRecord(s.getFullName(), s.getDoc(), null, s.isError());
      for (String alias : s.getAliases())
        result.addAlias(alias, null); // copy aliases
      seen.put(s, result);
      List<Field> newFields = new ArrayList<>(s.getFields().size());
      for (Field f : s.getFields()) {
        Schema fSchema = addStringType(f.schema(), seen);
        Field newF = new Field(f, fSchema);
        newFields.add(newF);
      }
      result.setFields(newFields);
      break;
    case ARRAY:
      Schema e = addStringType(s.getElementType(), seen);
      result = Schema.createArray(e);
      break;
    case MAP:
      Schema v = addStringType(s.getValueType(), seen);
      result = Schema.createMap(v);
      GenericData.setStringType(result, stringType);
      break;
    case UNION:
      List<Schema> types = new ArrayList<>(s.getTypes().size());
      for (Schema branch : s.getTypes())
        types.add(addStringType(branch, seen));
      result = Schema.createUnion(types);
      break;
    }
    result.addAllProps(s);
    if (s.getLogicalType() != null) {
      s.getLogicalType().addToSchema(result);
    }
    seen.put(s, result);
    return result;
  }

  /**
   * Utility for template use (and also internal use). Returns a string giving the
   * FQN of the Java type to be used for a string schema or for the key of a map
   * schema. (It's an error to call this on a schema other than a string or map.)
   */
  public String getStringType(Schema s) {
    String prop;
    switch (s.getType()) {
    case MAP:
      prop = SpecificData.KEY_CLASS_PROP;
      break;
    case STRING:
      prop = SpecificData.CLASS_PROP;
      break;
    default:
      throw new IllegalArgumentException("Can't check string-type of non-string/map type: " + s);
    }
    return getStringType(s.getObjectProp(prop));
  }

  private String getStringType(Object overrideClassProperty) {
    if (overrideClassProperty != null)
      return overrideClassProperty.toString();
    switch (stringType) {
    case String:
      return "java.lang.String";
    case Utf8:
      return "org.apache.avro.util.Utf8";
    case CharSequence:
      return "java.lang.CharSequence";
    default:
      throw new RuntimeException("Unknown string type: " + stringType);
    }
  }

  /**
   * Utility for template use. Returns true iff a STRING-schema or the key of a
   * MAP-schema is what SpecificData defines as "stringable" (which means we need
   * to call toString on it before before writing it).
   */
  public boolean isStringable(Schema schema) {
    String t = getStringType(schema);
    return !(t.equals("java.lang.String") || t.equals("java.lang.CharSequence")
        || t.equals("org.apache.avro.util.Utf8"));
  }

  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  /**
   * Utility for template use. Returns the java type for a Schema.
   */
  public String javaType(Schema schema) {
    return javaType(schema, true);
  }

  private String javaType(Schema schema, boolean checkConvertedLogicalType) {
    if (checkConvertedLogicalType) {
      String convertedLogicalType = getConvertedLogicalType(schema);
      if (convertedLogicalType != null) {
        return convertedLogicalType;
      }
    }

    switch (schema.getType()) {
    case RECORD:
    case ENUM:
    case FIXED:
      return SpecificData.mangleFullyQualified(schema.getFullName());
    case ARRAY:
      return "java.util.List<" + javaType(schema.getElementType()) + ">";
    case MAP:
      return "java.util.Map<" + getStringType(schema.getObjectProp(SpecificData.KEY_CLASS_PROP)) + ","
          + javaType(schema.getValueType()) + ">";
    case UNION:
      List<Schema> types = schema.getTypes(); // elide unions with null
      if ((types.size() == 2) && types.contains(NULL_SCHEMA))
        return javaType(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
      return "java.lang.Object";
    case STRING:
      return getStringType(schema.getObjectProp(SpecificData.CLASS_PROP));
    case BYTES:
      return "java.nio.ByteBuffer";
    case INT:
      return "java.lang.Integer";
    case LONG:
      return "java.lang.Long";
    case FLOAT:
      return "java.lang.Float";
    case DOUBLE:
      return "java.lang.Double";
    case BOOLEAN:
      return "java.lang.Boolean";
    case NULL:
      return "java.lang.Void";
    default:
      throw new RuntimeException("Unknown type: " + schema);
    }
  }

  private LogicalType getLogicalType(Schema schema) {
    if (enableDecimalLogicalType || !(schema.getLogicalType() instanceof LogicalTypes.Decimal)) {
      return schema.getLogicalType();
    }
    return null;
  }

  private String getConvertedLogicalType(Schema schema) {
    final Conversion<?> conversion = specificData.getConversionFor(getLogicalType(schema));
    if (conversion != null) {
      return conversion.getConvertedType().getName();
    }
    return null;
  }

  /**
   * Utility for template use.
   */
  public String generateSetterCode(Schema schema, String name, String pname) {
    Conversion<?> conversion = specificData.getConversionFor(schema.getLogicalType());
    if (conversion != null) {
      return conversion.adjustAndSetValue("this." + name, pname);
    }
    return "this." + name + " = " + pname + ";";
  }

  /**
   * Utility for template use. Returns the unboxed java type for a Schema.
   *
   * @deprecated use javaUnbox(Schema, boolean), kept for backward compatibility
   *             of custom templates
   */
  @Deprecated
  public String javaUnbox(Schema schema) {
    return javaUnbox(schema, false);
  }

  /**
   * Utility for template use. Returns the unboxed java type for a Schema
   * including the void type.
   */
  public String javaUnbox(Schema schema, boolean unboxNullToVoid) {
    String convertedLogicalType = getConvertedLogicalType(schema);
    if (convertedLogicalType != null) {
      return convertedLogicalType;
    }

    switch (schema.getType()) {
    case INT:
      return "int";
    case LONG:
      return "long";
    case FLOAT:
      return "float";
    case DOUBLE:
      return "double";
    case BOOLEAN:
      return "boolean";
    case NULL:
      if (unboxNullToVoid) {
        // Used for preventing unnecessary returns for RPC methods without response but
        // with error(s)
        return "void";
      }
    default:
      return javaType(schema, false);
    }
  }

  /**
   * Utility for template use. Return a string with a given number of spaces to be
   * used for indentation purposes.
   */
  public String indent(int n) {
    return new String(new char[n]).replace('\0', ' ');
  }

  /**
   * Utility for template use. For a two-branch union type with one null branch,
   * returns the index of the null branch. It's an error to use on anything other
   * than a two-branch union with on null branch.
   */
  public int getNonNullIndex(Schema s) {
    if (s.getType() != Schema.Type.UNION || s.getTypes().size() != 2 || !s.getTypes().contains(NULL_SCHEMA))
      throw new IllegalArgumentException("Can only be used on 2-branch union with a null branch: " + s);
    return (s.getTypes().get(0).equals(NULL_SCHEMA) ? 1 : 0);
  }

  /**
   * Utility for template use. Returns true if the encode/decode logic in
   * record.vm can handle the schema being presented.
   */
  public boolean isCustomCodable(Schema schema) {
    return isCustomCodable(schema, new HashSet<>());
  }

  private boolean isCustomCodable(Schema schema, Set<Schema> seen) {
    if (!seen.add(schema))
      // Recursive call: assume custom codable until a caller on the call stack proves
      // otherwise.
      return true;
    if (schema.getLogicalType() != null)
      return false;
    boolean result = true;
    switch (schema.getType()) {
    case RECORD:
      if (schema.isError())
        return false;
      for (Schema.Field f : schema.getFields())
        result &= isCustomCodable(f.schema(), seen);
      break;
    case MAP:
      result = isCustomCodable(schema.getValueType(), seen);
      break;
    case ARRAY:
      result = isCustomCodable(schema.getElementType(), seen);
      break;
    case UNION:
      List<Schema> types = schema.getTypes();
      // Only know how to handle "nulling" unions for now
      if (types.size() != 2 || !types.contains(NULL_SCHEMA))
        return false;
      for (Schema s : types)
        result &= isCustomCodable(s, seen);
      break;
    default:
    }
    return result;
  }

  public boolean hasLogicalTypeField(Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      if (field.schema().getLogicalType() != null) {
        return true;
      }
    }
    return false;
  }

  public String conversionInstance(Schema schema) {
    if (schema == null || schema.getLogicalType() == null) {
      return "null";
    }

    if (LogicalTypes.Decimal.class.equals(schema.getLogicalType().getClass()) && !enableDecimalLogicalType) {
      return "null";
    }

    final Conversion<Object> conversion = specificData.getConversionFor(schema.getLogicalType());
    if (conversion != null) {
      return "new " + conversion.getClass().getCanonicalName() + "()";
    }

    return "null";
  }

  /**
   * Utility for template use. Returns the java annotations for a schema.
   */
  public String[] javaAnnotations(JsonProperties props) {
    final Object value = props.getObjectProp("javaAnnotation");
    if (value instanceof String && isValidAsAnnotation((String) value))
      return new String[] { value.toString() };
    if (value instanceof List) {
      final List<?> list = (List<?>) value;
      final List<String> annots = new ArrayList<>(list.size());
      for (Object o : list) {
        if (isValidAsAnnotation(o.toString()))
          annots.add(o.toString());
      }
      return annots.toArray(new String[0]);
    }
    return new String[0];
  }

  private static final String PATTERN_IDENTIFIER_PART = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";
  private static final String PATTERN_IDENTIFIER = String.format("(?:%s(?:\\.%s)*)", PATTERN_IDENTIFIER_PART,
      PATTERN_IDENTIFIER_PART);
  private static final String PATTERN_STRING = "\"(?:\\\\[\\\\\"ntfb]|(?<!\\\\).)*\"";
  private static final String PATTERN_NUMBER = "(?:\\((?:byte|char|short|int|long|float|double)\\))?[x0-9_.]*[fl]?";
  private static final String PATTERN_LITERAL_VALUE = String.format("(?:%s|%s|true|false)", PATTERN_STRING,
      PATTERN_NUMBER);
  private static final String PATTERN_PARAMETER_LIST = String.format(
      "\\(\\s*(?:%s|%s\\s*=\\s*%s(?:\\s*,\\s*%s\\s*=\\s*%s)*)?\\s*\\)", PATTERN_LITERAL_VALUE, PATTERN_IDENTIFIER,
      PATTERN_LITERAL_VALUE, PATTERN_IDENTIFIER, PATTERN_LITERAL_VALUE);
  private static final Pattern VALID_AS_ANNOTATION = Pattern
      .compile(String.format("%s(?:%s)?", PATTERN_IDENTIFIER, PATTERN_PARAMETER_LIST));

  private boolean isValidAsAnnotation(String value) {
    return VALID_AS_ANNOTATION.matcher(value.strip()).matches();
  }

  // maximum size for string constants, to avoid javac limits
  int maxStringChars = 8192;

  /**
   * Utility for template use. Takes a (potentially overly long) string and splits
   * it into a quoted, comma-separated sequence of escaped strings.
   *
   * @param s The string to split
   * @return A sequence of quoted, comma-separated, escaped strings
   */
  public String javaSplit(String s) throws IOException {
    StringBuilder b = new StringBuilder(s.length());
    b.append("\""); // initial quote
    for (int i = 0; i < s.length(); i += maxStringChars) {
      if (i != 0)
        b.append("\",\""); // insert quote-comma-quote
      String chunk = s.substring(i, Math.min(s.length(), i + maxStringChars));
      b.append(escapeForJavaString(chunk)); // escape chunks
    }
    b.append("\""); // final quote
    return b.toString();
  }

  /**
   * Utility for template use. Escapes quotes and backslashes.
   */
  public static String escapeForJavaString(String o) {
    return o.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  /**
   * Utility for template use (previous name). Escapes quotes and backslashes.
   *
   * @deprecated Use {@link #escapeForJavaString(String)} instead
   */
  @Deprecated(since = "1.12.1", forRemoval = true)
  public static String javaEscape(String o) {
    return escapeForJavaString(o);
  }

  /**
   * Utility for template use. Escapes comment end with HTML entities.
   */
  public static String escapeForJavadoc(String s) {
    return s.replace("*/", "*&#47;").replace("<", "&lt;").replace(">", "&gt;");
  }

  /**
   * Utility for template use. Returns empty string for null.
   */
  public static String nullToEmpty(String x) {
    return x == null ? "" : x;
  }

  /**
   * Utility for template use. Adds a dollar sign to reserved words.
   */
  public static String mangle(String word) {
    return SpecificData.mangle(word, false);
  }

  /**
   * Utility for template use. Adds a dollar sign to reserved words.
   */
  public static String mangle(String word, boolean isError) {
    return SpecificData.mangle(word, isError);
  }

  /**
   * Utility for template use. Adds a dollar sign to reserved words in type
   * identifiers.
   */
  public static String mangleTypeIdentifier(String word) {
    return SpecificData.mangleTypeIdentifier(word, false);
  }

  /**
   * Utility for template use. Adds a dollar sign to reserved words in type
   * identifiers.
   */
  public static String mangleTypeIdentifier(String word, boolean isError) {
    return SpecificData.mangle(word, isError);
  }

  /**
   * Utility for template use. Adds a dollar sign to reserved words.
   */
  public static String mangle(String word, Set<String> reservedWords) {
    return SpecificData.mangle(word, reservedWords, false);
  }

  /**
   * Utility for template use. Adds a dollar sign to reserved words.
   */
  public static String mangle(String word, Set<String> reservedWords, boolean isMethod) {
    return SpecificData.mangle(word, reservedWords, isMethod);
  }

  public boolean canGenerateEqualsAndHashCode(Schema schema) {
    return getUsedCustomLogicalTypeFactories(schema).isEmpty();
  }

  public boolean isPrimitiveType(Schema schema) {
    return !isUnboxedJavaTypeNullable(schema) && getConvertedLogicalType(schema) == null;
  }

  public String hashCodeFor(Schema schema, String name) {
    switch (javaUnbox(schema, false)) {
    case "int":
      return "Integer.hashCode(" + name + ")";
    case "long":
      return "Long.hashCode(" + name + ")";
    case "float":
      return "Float.hashCode(" + name + ")";
    case "double":
      return "Double.hashCode(" + name + ")";
    case "boolean":
      return "Boolean.hashCode(" + name + ")";
    default:
      // Hashcode of Union is expected to match ordinal
      if (schema.getType() == Schema.Type.ENUM || ((schema.getType() == Schema.Type.UNION)
          && (schema.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.ENUM)))) {
        if (schema.getType() == Schema.Type.ENUM
            || (schema.getTypes().size() == 2 && schema.getTypes().contains(NULL_SCHEMA))) {
          return "(" + name + " == null ? 0 : ((java.lang.Enum) " + name + ").ordinal())";
        } else {
          return "(" + name + " == null ? 0 : " + name + " instanceof java.lang.Enum ? ((java.lang.Enum) " + name
              + ").ordinal() : " + name + ".hashCode())";
        }
      }
      return "(" + name + " == null ? 0 : " + name + ".hashCode())";
    }
  }

  public boolean ignoredField(Field field) {
    return field.order() == Field.Order.IGNORE;
  }

  /**
   * Utility for use by templates. Return schema fingerprint as a long.
   */
  public static long fingerprint64(Schema schema) {
    return SchemaNormalization.parsingFingerprint64(schema);
  }

  /**
   * Generates the name of a field accessor method.
   *
   * @param schema the schema in which the field is defined.
   * @param field  the field for which to generate the accessor name.
   * @return the name of the accessor method for the given field.
   */
  public static String generateGetMethod(Schema schema, Field field) {
    return generateMethodName(schema, field, "get", "");
  }

  /**
   * Generates the name of a field accessor method that returns a Java 8 Optional.
   *
   * @param schema the schema in which the field is defined.
   * @param field  the field for which to generate the accessor name.
   * @return the name of the accessor method for the given field.
   */
  public static String generateGetOptionalMethod(Schema schema, Field field) {
    return generateMethodName(schema, field, "getOptional", "");
  }

  /**
   * Generates the name of a field mutator method.
   *
   * @param schema the schema in which the field is defined.
   * @param field  the field for which to generate the mutator name.
   * @return the name of the mutator method for the given field.
   */
  public static String generateSetMethod(Schema schema, Field field) {
    return generateMethodName(schema, field, "set", "");
  }

  /**
   * Generates the name of a field "has" method.
   *
   * @param schema the schema in which the field is defined.
   * @param field  the field for which to generate the "has" method name.
   * @return the name of the has method for the given field.
   */
  public static String generateHasMethod(Schema schema, Field field) {
    return generateMethodName(schema, field, "has", "");
  }

  /**
   * Generates the name of a field "clear" method.
   *
   * @param schema the schema in which the field is defined.
   * @param field  the field for which to generate the accessor name.
   * @return the name of the has method for the given field.
   */
  public static String generateClearMethod(Schema schema, Field field) {
    return generateMethodName(schema, field, "clear", "");
  }

  /**
   * Utility for use by templates. Does this schema have a Builder method?
   */
  public static boolean hasBuilder(Schema schema) {
    switch (schema.getType()) {
    case RECORD:
      return true;

    case UNION:
      List<Schema> types = schema.getTypes(); // elide unions with null
      if ((types.size() == 2) && types.contains(NULL_SCHEMA)) {
        return hasBuilder(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
      }
      return false;

    default:
      return false;
    }
  }

  /**
   * Generates the name of a field Builder accessor method.
   *
   * @param schema the schema in which the field is defined.
   * @param field  the field for which to generate the Builder accessor name.
   * @return the name of the Builder accessor method for the given field.
   */
  public static String generateGetBuilderMethod(Schema schema, Field field) {
    return generateMethodName(schema, field, "get", "Builder");
  }

  /**
   * Generates the name of a field Builder mutator method.
   *
   * @param schema the schema in which the field is defined.
   * @param field  the field for which to generate the Builder mutator name.
   * @return the name of the Builder mutator method for the given field.
   */
  public static String generateSetBuilderMethod(Schema schema, Field field) {
    return generateMethodName(schema, field, "set", "Builder");
  }

  /**
   * Generates the name of a field Builder "has" method.
   *
   * @param schema the schema in which the field is defined.
   * @param field  the field for which to generate the "has" Builder method name.
   * @return the name of the "has" Builder method for the given field.
   */
  public static String generateHasBuilderMethod(Schema schema, Field field) {
    return generateMethodName(schema, field, "has", "Builder");
  }

  /**
   * Generates a method name from a field name.
   *
   * @param schema  the schema in which the field is defined.
   * @param field   the field for which to generate the accessor name.
   * @param prefix  method name prefix, e.g. "get" or "set".
   * @param postfix method name postfix, e.g. "" or "Builder".
   * @return the generated method name.
   */
  private static String generateMethodName(Schema schema, Field field, String prefix, String postfix) {

    // Check for the special case in which the schema defines two fields whose
    // names are identical except for the case of the first character:
    int indexNameConflict = calcNameIndex(field.name(), schema);

    StringBuilder methodBuilder = new StringBuilder(prefix);
    String fieldName = SpecificData.mangleMethod(field.name(), schema.isError());

    boolean nextCharToUpper = true;
    for (int ii = 0; ii < fieldName.length(); ii++) {
      if (fieldName.charAt(ii) == '_') {
        nextCharToUpper = true;
      } else if (nextCharToUpper) {
        methodBuilder.append(Character.toUpperCase(fieldName.charAt(ii)));
        nextCharToUpper = false;
      } else {
        methodBuilder.append(fieldName.charAt(ii));
      }
    }
    methodBuilder.append(postfix);

    // If there is a field name conflict append $0 or $1
    if (indexNameConflict >= 0) {
      if (methodBuilder.charAt(methodBuilder.length() - 1) != '$') {
        methodBuilder.append('$');
      }
      methodBuilder.append(indexNameConflict);
    }

    return methodBuilder.toString();
  }

  /**
   * Calc name index for getter / setter field in case of conflict as example,
   * having a schema with fields __X, _X, _x, X, x should result with indexes __X:
   * 3, _X: 2, _x: 1, X: 0 x: None (-1)
   *
   * @param fieldName : field name.
   * @param schema    : schema.
   * @return index for field.
   */
  private static int calcNameIndex(String fieldName, Schema schema) {
    // get name without underscore at start
    // and calc number of other similar fields with same subname.
    int countSimilar = 0;
    String pureFieldName = fieldName;
    while (!pureFieldName.isEmpty() && pureFieldName.charAt(0) == '_') {
      pureFieldName = pureFieldName.substring(1);
      if (schema.getField(pureFieldName) != null) {
        countSimilar++;
      }
      String reversed = reverseFirstLetter(pureFieldName);
      if (schema.getField(reversed) != null) {
        countSimilar++;
      }
    }
    // field name start with upper have +1
    String reversed = reverseFirstLetter(fieldName);
    if (!pureFieldName.isEmpty() && Character.isUpperCase(pureFieldName.charAt(0))
        && schema.getField(reversed) != null) {
      countSimilar++;
    }

    int ret = -1; // if no similar name, no index.
    if (countSimilar > 0) {
      ret = countSimilar - 1; // index is count similar -1 (start with $0)
    }

    return ret;
  }

  /**
   * Reverse first letter upper <=> lower. __Name <=> __name
   *
   * @param name : input name.
   * @return name with change case of first letter.
   */
  private static String reverseFirstLetter(String name) {
    StringBuilder builder = new StringBuilder(name);
    int index = 0;
    while (builder.length() > index && builder.charAt(index) == '_') {
      index++;
    }
    if (builder.length() > index) {
      char c = builder.charAt(index);
      char inverseC = Character.isLowerCase(c) ? Character.toUpperCase(c) : Character.toLowerCase(c);
      builder.setCharAt(index, inverseC);
    }
    return builder.toString();
  }

  /**
   * Tests whether an unboxed Java type can be set to null
   */
  public static boolean isUnboxedJavaTypeNullable(Schema schema) {
    switch (schema.getType()) {
    // Primitives can't be null; assume anything else can
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case BOOLEAN:
      return false;
    default:
      return true;
    }
  }

  public static void main(String[] args) throws Exception {
    // compileSchema(new File(args[0]), new File(args[1]));
    compileProtocol(new File(args[0]), new File(args[1]));
  }

  /**
   * Sets character encoding for generated java file
   *
   * @param outputCharacterEncoding Character encoding for output files (defaults
   *                                to system encoding)
   */
  public void setOutputCharacterEncoding(String outputCharacterEncoding) {
    this.outputCharacterEncoding = outputCharacterEncoding;
  }

  public String getSchemaParentClass(boolean isError) {
    if (isError) {
      return this.errorSpecificClass;
    } else {
      return this.recordSpecificClass;
    }
  }

  public void setRecordSpecificClass(final String recordSpecificClass) {
    this.recordSpecificClass = recordSpecificClass;
  }

  public void setErrorSpecificClass(final String errorSpecificClass) {
    this.errorSpecificClass = errorSpecificClass;
  }
}
