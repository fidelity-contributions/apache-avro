/**
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

using System;
using System.Collections.Generic;
using NUnit.Framework;
using System.IO;
using System.Linq;
using System.Text;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Avro.Test
{
    using Decoder = Avro.IO.Decoder;
    using Encoder = Avro.IO.Encoder;

    /// <summary>
    /// Tests the JsonEncoder and JsonDecoder.
    /// </summary>
    [TestFixture]
    public class JsonCodecTests
    {
        [TestCase("{ \"type\": \"record\", \"name\": \"r\", \"fields\": [ " +
                  " { \"name\" : \"f1\", \"type\": \"int\" }, " +
                  " { \"name\" : \"f2\", \"type\": \"float\" } " +
                  "] }",
            "{ \"f2\": 10.4, \"f1\": 10 } ")]
        [TestCase("{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": [ \"s1\", \"s2\"] }", " \"s1\" ")]
        [TestCase("{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": [ \"s1\", \"s2\"] }", " \"s2\" ")]
        [TestCase("{ \"type\": \"fixed\", \"name\": \"f\", \"size\": 5 }", "\"hello\"")]
        [TestCase("{ \"type\": \"array\", \"items\": \"int\" }", "[ 10, 20, 30 ]")]
        [TestCase("{ \"type\": \"map\", \"values\": \"int\" }", "{ \"k1\": 10, \"k2\": 20, \"k3\": 30 }")]
        [TestCase("[ \"int\", \"long\" ]", "{ \"int\": 10 }")]
        [TestCase("\"string\"", "\"hello\"")]
        [TestCase("\"bytes\"", "\"hello\"")]
        [TestCase("\"int\"", "10")]
        [TestCase("\"long\"", "10")]
        [TestCase("\"float\"", "10.0")]
        [TestCase("\"double\"", "10.0")]
        [TestCase("\"boolean\"", "true")]
        [TestCase("\"boolean\"", "false")]
        [TestCase("\"null\"", "null")]
        public void TestJsonAllTypesValidValues(String schemaStr, String value)
        {
            Schema schema = Schema.Parse(schemaStr);
            byte[] avroBytes = fromJsonToAvro(value, schema);

            Assert.IsTrue(JToken.DeepEquals(JToken.Parse(value),
                JToken.Parse(fromAvroToJson(avroBytes, schema, true))));
        }

        [TestCase("{ \"type\": \"record\", \"name\": \"r\", \"fields\": [ " +
                  " { \"name\" : \"f1\", \"type\": \"int\" }, " +
                  " { \"name\" : \"f2\", \"type\": \"float\" } " +
                  "] }",
            "{ \"f4\": 10.4, \"f3\": 10 } ")]
        [TestCase("{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": [ \"s1\", \"s2\"] }", " \"s3\" ")]
        [TestCase("{ \"type\": \"fixed\", \"name\": \"f\", \"size\": 10 }", "\"hello\"")]
        [TestCase("{ \"type\": \"array\", \"items\": \"int\" }", "[ \"10\", \"20\", \"30\" ]")]
        [TestCase("{ \"type\": \"map\", \"values\": \"int\" }", "{ \"k1\": \"10\", \"k2\": \"20\"}")]
        [TestCase("[ \"int\", \"long\" ]", "10")]
        [TestCase("\"string\"", "10")]
        [TestCase("\"bytes\"", "10")]
        [TestCase("\"int\"", "\"hi\"")]
        [TestCase("\"long\"", "\"hi\"")]
        [TestCase("\"float\"", "\"hi\"")]
        [TestCase("\"double\"", "\"hi\"")]
        [TestCase("\"boolean\"", "\"hi\"")]
        [TestCase("\"boolean\"", "\"hi\"")]
        [TestCase("\"null\"", "\"hi\"")]
        public void TestJsonAllTypesInvalidValues(String schemaStr, String value)
        {
            Schema schema = Schema.Parse(schemaStr);
            Assert.Throws<AvroTypeException>(() => fromJsonToAvro(value, schema));
        }

        [TestCase("{ \"type\": \"record\", \"name\": \"r\", \"fields\": [ " +
                  " { \"name\" : \"f1\", \"type\": \"int\" }, " +
                  " { \"name\" : \"f2\", \"type\": \"float\" } " +
                  "] }",
            "{ \"f2\": 10.4, \"f1")]
        [TestCase("{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": [ \"s1\", \"s2\"] }", "s1")]
        [TestCase("\"string\"", "\"hi")]
        public void TestJsonMalformed(String schemaStr, String value)
        {
            Schema schema = Schema.Parse(schemaStr);
            Assert.Throws<JsonReaderException>(() => fromJsonToAvro(value, schema));
        }

        [Test]
        public void TestJsonEncoderWhenIncludeNamespaceOptionIsFalse()
        {
            string value = "{\"b\": {\"string\":\"myVal\"}, \"a\": 1}";
            string schemaStr = "{\"type\": \"record\", \"name\": \"ab\", \"fields\": [" +
                               "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": [\"null\", \"string\"]}" +
                               "]}";
            Schema schema = Schema.Parse(schemaStr);
            byte[] avroBytes = fromJsonToAvro(value, schema);

            Assert.IsTrue(JToken.DeepEquals(JObject.Parse("{\"b\":\"myVal\",\"a\":1}"),
                JObject.Parse(fromAvroToJson(avroBytes, schema, false))));
        }

        [Test]
        public void TestJsonEncoderWhenIncludeNamespaceOptionIsTrue()
        {
            string value = "{\"b\": {\"string\":\"myVal\"}, \"a\": 1}";
            string schemaStr = "{\"type\": \"record\", \"name\": \"ab\", \"fields\": [" +
                               "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": [\"null\", \"string\"]}" +
                               "]}";
            Schema schema = Schema.Parse(schemaStr);
            byte[] avroBytes = fromJsonToAvro(value, schema);

            Assert.IsTrue(JToken.DeepEquals(JObject.Parse("{\"b\":{\"string\":\"myVal\"},\"a\":1}"),
                JObject.Parse(fromAvroToJson(avroBytes, schema, true))));
        }

        [Test]
        public void TestJsonRecordOrdering()
        {
            string value = "{\"b\": 2, \"a\": 1}";
            Schema schema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [" +
                                         "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": \"int\"}" +
                                         "]}");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value);
            object o = reader.Read(null, decoder);

            Assert.AreEqual("{\"a\":1,\"b\":2}", fromDatumToJson(o, schema, false));
        }

        [Test]
        public void TestJsonRecordOrdering2()
        {
            string value = "{\"b\": { \"b3\": 1.4, \"b2\": 3.14, \"b1\": \"h\"}, \"a\": {\"a2\":true, \"a1\": null}}";
            Schema schema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n" +
                                         "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n" +
                                         "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n" +
                                         "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n" +
                                         "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":\"float\"}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n" +
                                         "]}");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value);
            object o = reader.Read(null, decoder);

            Assert.AreEqual("{\"a\":{\"a1\":null,\"a2\":true},\"b\":{\"b1\":\"h\",\"b2\":3.14,\"b3\":1.4}}",
                fromDatumToJson(o, schema, false));
        }

        [Test]
        public void TestJsonRecordOrderingWithProjection()
        {
            String value = "{\"b\": { \"b3\": 1.4, \"b2\": 3.14, \"b1\": \"h\"}, \"a\": {\"a2\":true, \"a1\": null}}";
            Schema writerSchema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
                                               + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
                                               + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n"
                                               + "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n"
                                               + "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":\"float\"}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n"
                                               + "]}");
            Schema readerSchema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
                                               + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
                                               + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}}\n" +
                                               "]}");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(writerSchema, readerSchema);
            Decoder decoder = new JsonDecoder(writerSchema, value);
            Object o = reader.Read(null, decoder);

            Assert.AreEqual("{\"a\":{\"a1\":null,\"a2\":true}}",
                fromDatumToJson(o, readerSchema, false));
        }


        [Test]
        public void TestJsonRecordOrderingWithProjection2()
        {
            String value =
                "{\"b\": { \"b1\": \"h\", \"b2\": [3.14, 3.56], \"b3\": 1.4}, \"a\": {\"a2\":true, \"a1\": null}}";
            Schema writerSchema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
                                               + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
                                               + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n"
                                               + "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n"
                                               + "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":{\"type\":\"array\", \"items\":\"float\"}}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n"
                                               + "]}");

            Schema readerSchema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
                                               + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
                                               + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}}\n" +
                                               "]}");

            GenericDatumReader<object> reader = new GenericDatumReader<object>(writerSchema, readerSchema);
            Decoder decoder = new JsonDecoder(writerSchema, value);
            object o = reader.Read(null, decoder);

            Assert.AreEqual("{\"a\":{\"a1\":null,\"a2\":true}}",
                fromDatumToJson(o, readerSchema, false));
        }

        [TestCase("{\"int\":123}")]
        [TestCase("{\"string\":\"12345678-1234-5678-1234-123456789012\"}")]
        [TestCase("null")]
        public void TestJsonUnionWithLogicalTypes(String value)
        {
            Schema schema = Schema.Parse(
                "[\"null\",\n" +
                "    { \"type\": \"int\", \"logicalType\": \"date\" },\n" +
                "    { \"type\": \"string\", \"logicalType\": \"uuid\" }\n" +
                "]");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value);
            object o = reader.Read(null, decoder);

            Assert.AreEqual(value, fromDatumToJson(o, schema, true));
        }

        [TestCase("{\"int\":123}")]
        [TestCase("{\"com.myrecord\":{\"f1\":123}}")]
        [TestCase("null")]
        public void TestJsonUnionWithRecord(String value)
        {
            Schema schema = Schema.Parse(
                "[\"null\",\n" +
                "    { \"type\": \"int\", \"logicalType\": \"date\" },\n" +
                "    {\"type\":\"record\",\"name\":\"myrecord\", \"namespace\":\"com\"," +
                "        \"fields\":[{\"name\":\"f1\",\"type\": \"int\"}]}" +
                "]");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value);
            object o = reader.Read(null, decoder);

            Assert.AreEqual(value, fromDatumToJson(o, schema, true));
        }

        [TestCase("int", 1)]
        [TestCase("long", 1L)]
        [TestCase("float", 1.0F)]
        [TestCase("double", 1.0)]
        public void TestJsonDecoderNumeric(string type, object value)
        {
            string def = "{\"type\":\"record\",\"name\":\"X\",\"fields\":" + "[{\"type\":\"" + type +
                         "\",\"name\":\"n\"}]}";
            Schema schema = Schema.Parse(def);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, schema);

            string[] records = { "{\"n\":1}", "{\"n\":1.0}" };

            foreach (GenericRecord g in records.Select(r => reader.Read(null, new JsonDecoder(schema, r))))
            {
                Assert.AreEqual(value, g["n"]);
            }
        }

        [Test]
        [TestCase("float", "0", (float)0)]
        [TestCase("float", "1", (float)1)]
        [TestCase("float", "1.0", (float)1.0)]
        [TestCase("double", "0", (double)0)]
        [TestCase("double", "1", (double)1)]
        [TestCase("double", "1.0", 1.0)]
        [TestCase("float", "\"NaN\"", float.NaN)]
        [TestCase("float", "\"Infinity\"", float.PositiveInfinity)]
        [TestCase("float", "\"INF\"", float.PositiveInfinity)]
        [TestCase("float", "\"-Infinity\"", float.NegativeInfinity)]
        [TestCase("float", "\"-INF\"", float.NegativeInfinity)]
        [TestCase("double", "\"NaN\"", double.NaN)]
        [TestCase("double", "\"Infinity\"", double.PositiveInfinity)]
        [TestCase("double", "\"INF\"", double.PositiveInfinity)]
        [TestCase("double", "\"-Infinity\"", double.NegativeInfinity)]
        [TestCase("double", "\"-INF\"", double.NegativeInfinity)]
        [TestCase("float", "\"\"", null)]
        [TestCase("float", "\"unknown\"", null)]
        [TestCase("float", "\"nan\"", null)]
        [TestCase("float", "\"infinity\"", null)]
        [TestCase("float", "\"inf\"", null)]
        [TestCase("float", "\"-infinity\"", null)]
        [TestCase("float", "\"-inf\"", null)]
        [TestCase("double", "\"\"", null)]
        [TestCase("double", "\"unknown\"", null)]
        [TestCase("double", "\"nan\"", null)]
        [TestCase("double", "\"infinity\"", null)]
        [TestCase("double", "\"inf\"", null)]
        [TestCase("double", "\"-infinity\"", null)]
        [TestCase("double", "\"-inf\"", null)]
        [TestCase("double", "\"-inf\"", null)]
        public void TestJsonDecodeFloatDouble(string typeStr, string valueStr, object expected)
        {
            string def = $"{{\"type\":\"record\",\"name\":\"X\",\"fields\":[{{\"type\":\"{typeStr}\",\"name\":\"Value\"}}]}}";
            Schema schema = Schema.Parse(def);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, schema);

            string record = $"{{\"Value\":{valueStr}}}";
            Decoder decoder = new JsonDecoder(schema, record);
            try
            {
                GenericRecord r = reader.Read(null, decoder);
                Assert.AreEqual(expected, r["Value"]);
            }
            catch (AvroTypeException)
            {
                if (expected != null)
                {
                    throw;
                }
            }
        }

        [TestCase("{ \"s\": \"1900-01-01T00:00:00Z\" }", "1900-01-01T00:00:00Z")]
        [TestCase("{ \"s\": \"1900-01-01T00:00:00.0000000Z\" }", "1900-01-01T00:00:00.0000000Z")]
        [TestCase("{ \"s\": \"1900-01-01T00:00:00\" }", "1900-01-01T00:00:00")]
        public void TestJsonDecoderStringDates(string json, string expected)
        {
            string def = "{\"type\":\"record\",\"name\":\"X\",\"fields\": [{\"type\": \"string\",\"name\":\"s\"}]}";
            Schema schema = Schema.Parse(def);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, schema);

            var response = reader.Read(null, new JsonDecoder(schema, json));

            Assert.AreEqual(expected, response["s"]);
        }

        // Ensure that even if the order of fields in JSON is different from the order in schema, it works.
        [Test]
        public void TestJsonDecoderReorderFields()
        {
            String w = "{\"type\":\"record\",\"name\":\"R\",\"fields\":" + "[{\"type\":\"long\",\"name\":\"l\"},"
                                                                         + "{\"type\":{\"type\":\"array\",\"items\":\"int\"},\"name\":\"a\"}" +
                                                                         "]}";
            Schema ws = Schema.Parse(w);
            String data = "{\"a\":[1,2],\"l\":100}";
            JsonDecoder decoder = new JsonDecoder(ws, data);
            Assert.AreEqual(100, decoder.ReadLong());
            decoder.SkipArray();
            data = "{\"l\": 200, \"a\":[1,2]}";
            decoder = new JsonDecoder(ws, data);
            Assert.AreEqual(200, decoder.ReadLong());
            decoder.SkipArray();
        }

        [Test]
        public void TestJsonDecoderSpecificDatumWriterWithArrayAndMap()
        {
            Root data = new Root();
            Item item = new Item { id = 123456 };
            data.myarray = new List<Item> { item };
            data.mymap = new Dictionary<string, int> { { "1", 1 }, { "2", 2 }, { "3", 3 }, { "4", 4 } };

            DatumWriter<Root> writer = new SpecificDatumWriter<Root>(data.Schema);

            ByteBufferOutputStream bbos = new ByteBufferOutputStream();

            Encoder encoder = new JsonEncoder(data.Schema, bbos);
            writer.Write(data, encoder);
            encoder.Flush();

            List<MemoryStream> listStreams = bbos.GetBufferList();

            using (StreamReader reader = new StreamReader(listStreams[0]))
            {
                String output = reader.ReadToEnd();
                Assert.AreEqual("{\"myarray\":[{\"id\":123456}],\"mymap\":{\"map\":{\"1\":1,\"2\":2,\"3\":3,\"4\":4}}}", output);
            }
        }

        [Test]
        public void TestJsonDecoderSpecificDefaultWriterWithArrayAndMap()
        {
            Root data = new Root();
            Item item = new Item { id = 123456 };
            data.myarray = new List<Item> { item };
            data.mymap = new Dictionary<string, int> { { "1", 1 }, { "2", 2 }, { "3", 3 }, { "4", 4 } };

            SpecificDefaultWriter writer = new SpecificDefaultWriter(data.Schema);

            ByteBufferOutputStream bbos = new ByteBufferOutputStream();

            Encoder encoder = new JsonEncoder(data.Schema, bbos);
            writer.Write(data, encoder);
            encoder.Flush();

            List<MemoryStream> listStreams = bbos.GetBufferList();

            using (StreamReader reader = new StreamReader(listStreams[0]))
            {
                String output = reader.ReadToEnd();
                Assert.AreEqual("{\"myarray\":[{\"id\":123456}],\"mymap\":{\"map\":{\"1\":1,\"2\":2,\"3\":3,\"4\":4}}}", output);
            }
        }

        private byte[] fromJsonToAvro(string json, Schema schema)
        {
            DatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            GenericDatumWriter<object> writer = new GenericDatumWriter<object>(schema);
            MemoryStream output = new MemoryStream();

            Decoder decoder = new JsonDecoder(schema, json);
            Encoder encoder = new BinaryEncoder(output);

            object datum = reader.Read(null, decoder);

            writer.Write(datum, encoder);
            encoder.Flush();
            output.Flush();

            return output.ToArray();
        }

        private string fromAvroToJson(byte[] avroBytes, Schema schema, bool includeNamespace)
        {
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);

            Decoder decoder = new BinaryDecoder(new MemoryStream(avroBytes));
            object datum = reader.Read(null, decoder);
            return fromDatumToJson(datum, schema, includeNamespace);
        }

        private string fromDatumToJson(object datum, Schema schema, bool includeNamespace)
        {
            DatumWriter<object> writer = new GenericDatumWriter<object>(schema);
            MemoryStream output = new MemoryStream();

            JsonEncoder encoder = new JsonEncoder(schema, output);
            encoder.IncludeNamespace = includeNamespace;
            writer.Write(datum, encoder);
            encoder.Flush();
            output.Flush();

            return Encoding.UTF8.GetString(output.ToArray());
        }
    }

    public partial class Root : global::Avro.Specific.ISpecificRecord
    {
        public static global::Avro.Schema _SCHEMA = global::Avro.Schema.Parse(
            "{\"type\":\"record\",\"name\":\"Root\",\"namespace\":\"Avro.Test\",\"fields\":[{\"name\":\"myarray" +
            "\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Item\",\"namespace\":\"Avr" +
            "o.Test\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}}},{\"name\":\"mymap\",\"default\":null," +
            "\"type\":[\"null\",{\"type\":\"map\",\"values\":\"int\"}]}]}");
        private IList<Avro.Test.Item> _myarray;
        private IDictionary<string, System.Int32> _mymap;

        public virtual global::Avro.Schema Schema
        {
            get { return Root._SCHEMA; }
        }

        public IList<Avro.Test.Item> myarray
        {
            get { return this._myarray; }
            set { this._myarray = value; }
        }

        public IDictionary<string, System.Int32> mymap
        {
            get { return this._mymap; }
            set { this._mymap = value; }
        }

        public virtual object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return this.myarray;
                case 1: return this.mymap;
                default: throw new global::Avro.AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            }
        }

        public virtual void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    this.myarray = (IList<Avro.Test.Item>)fieldValue;
                    break;
                case 1:
                    this.mymap = (IDictionary<string, System.Int32>)fieldValue;
                    break;
                default: throw new global::Avro.AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            }
        }
    }

    public partial class Item : global::Avro.Specific.ISpecificRecord
    {
        public static global::Avro.Schema _SCHEMA = global::Avro.Schema.Parse(
            "{\"type\":\"record\",\"name\":\"Item\",\"namespace\":\"Avro.Test\",\"fields\":[{\"name\":\"id\",\"ty" +
            "pe\":\"long\"}]}");

        private long _id;

        public virtual global::Avro.Schema Schema
        {
            get { return Item._SCHEMA; }
        }

        public long id
        {
            get { return this._id; }
            set { this._id = value; }
        }

        public virtual object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return this.id;
                default: throw new global::Avro.AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            }
        }

        public virtual void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    this.id = (System.Int64)fieldValue;
                    break;
                default: throw new global::Avro.AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            }
        }
    }
}
