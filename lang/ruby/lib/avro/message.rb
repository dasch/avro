module Avro
  module Message
    META_SCHEMA = Schema.parse('{"type": "map", "values": "bytes"}')

    class Writer
      def initialize(writers_schema)
        @datum_writer = IO::DatumWriter.new(writers_schema)
        @meta = {}

        @meta['avro.schema'] = writers_schema.to_s
      end

      def write(datum)
        stream = StringIO.new('', 'w')
        stream.set_encoding('BINARY') if stream.respond_to?(:set_encoding)

        write_to_stream(datum, stream)

        stream.string
      end

      def write_to_stream(datum, stream)
        encoder = IO::BinaryEncoder.new(stream)

        @datum_writer.write_data(META_SCHEMA, @meta, encoder)
        @datum_writer.write(datum, encoder)
      end
    end

    class Reader
      attr_reader :readers_schema

      def initialize(readers_schema=nil)
        @readers_schema = readers_schema
      end

      def read(string)
        reader = StringIO.new(string)
        read_stream(reader)
      end

      def read_stream(reader)
        decoder = IO::BinaryDecoder.new(reader)
        schema_reader = IO::DatumReader.new(META_SCHEMA)

        meta = schema_reader.read(decoder)
        writers_schema = Schema.parse(meta.fetch("avro.schema"))

        datum_reader = IO::DatumReader.new(writers_schema, readers_schema)
        datum_reader.read(decoder)
      end
    end
  end
end
