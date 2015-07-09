# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_help'

class TestLogicalTypes < Test::Unit::TestCase
  def test_decimal
    schema = Avro::Schema.parse <<-SCHEMA
      {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 4,
        "scale": 2
      }
    SCHEMA

    datum = 42

    buffer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(buffer)
    writer = Avro::IO::DatumWriter.new(schema)
    writer.write(datum, encoder)

    buffer.rewind
    decoder = Avro::IO::BinaryDecoder.new(buffer)
    reader = Avro::IO::DatumReader.new(schema)
    result = reader.read(decoder)

    assert_equal BigDecimal.new(42, 4), result
  end
end
