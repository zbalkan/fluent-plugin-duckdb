require "fluent/plugin/output"
require "duckdb"
require "yajl"
require "json"
require "stringio"

module Fluent::Plugin
  class DuckdbOutput < Fluent::Plugin::Output
    Fluent::Plugin.register_output("duckdb", self)

    helpers :compat_parameters

    DEFAULT_BUFFER_TYPE = "memory"

    desc "The DuckDB database file path (use ':memory:' for in-memory)"
    config_param :database, :string, default: ":memory:"
    desc "The table name to insert records"
    config_param :table, :string, default: "fluentd_events"
    desc "The column name for the time"
    config_param :time_col, :string, default: "time"
    desc "The column name for the tag"
    config_param :tag_col, :string, default: "tag"
    desc "The column name for the record"
    config_param :record_col, :string, default: "record"
    desc "Format for timestamp conversion"
    config_param :time_format, :string, default: "%F %T.%N %z"

    config_section :buffer do
      config_set_default :@type, DEFAULT_BUFFER_TYPE
      config_set_default :chunk_keys, ["tag"]
    end

    def configure(conf)
      compat_parameters_convert(conf, :buffer)
      super
      unless @chunk_key_tag
        raise Fluent::ConfigError, "'tag' in chunk_keys is required."
      end
    end

    def start
      super
      init_connection
      @appender = @con.appender(@table)
    end

    def shutdown
      if @appender
        @appender.flush
        @appender.close
      end
      @con.close if @con
      @db.close if @db
      super
    end

    def write(chunk)
      tag = chunk.metadata.tag

      chunk.each do |time, record|
        @appender.begin_row
        @appender.append(tag)
        @appender.append(Time.at(time).strftime(@time_format))
        @appender.append(Yajl.dump(record))
        @appender.end_row
      end

      @appender.flush
    end

    private
    def init_connection
      return if @con
      @db = DuckDB::Database.open(@database)
      @con = @db.connect

      # Create table if missing
      @con.query <<~SQL
        CREATE TABLE IF NOT EXISTS #{@table} (
          #{@tag_col}    VARCHAR,
          #{@time_col}   TIMESTAMP,
          #{@record_col} JSON
        )
      SQL
    end
  end
end
