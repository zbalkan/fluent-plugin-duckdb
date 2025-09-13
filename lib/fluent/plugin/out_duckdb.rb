require "fluent/plugin/output"
require "duckdb"
require "yajl"
require "json"
require "stringio"
require "thread"

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

    desc "Maximum number of records before flush"
    config_param :flush_size, :integer, default: 1000
    desc "Maximum flush interval in seconds"
    config_param :flush_interval, :time, default: 5

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
      @buffer = []
      @mutex = Mutex.new
      @last_flush_time = Fluent::Engine.now

      @flush_thread = thread_create(:duckdb_flush_thread) do
        loop do
          sleep 1
          now = Fluent::Engine.now
          flush_if_necessary(now)
        end
      end
    end

    def shutdown
      @flush_thread&.kill
      flush_all
      @con.close if @con
      @db.close if @db
      super
    end

    def write(chunk)
      tag = chunk.metadata.tag
      now = Fluent::Engine.now

      chunk.each do |time, record|
        entry = [
          tag,
          Time.at(time).strftime(@time_format),
          Yajl.dump(record)
        ]
        @mutex.synchronize { @buffer << entry }
      end

      flush_if_necessary(now)
    end

    private

    def init_connection
      return if @con
      @db = DuckDB::Database.open(@database)
      @con = @db.connect

      @con.query <<~SQL
        CREATE TABLE IF NOT EXISTS #{@table} (
          #{@tag_col}    VARCHAR,
          #{@time_col}   TIMESTAMP,
          #{@record_col} JSON
        )
      SQL
    end

    def flush_if_necessary(now)
      should_flush = false
      batch = nil

      @mutex.synchronize do
        if @buffer.size >= @flush_size || (now - @last_flush_time) >= @flush_interval
          batch = @buffer.dup
          @buffer.clear
          @last_flush_time = now
          should_flush = true
        end
      end

      flush_batch(batch) if should_flush && batch
    end

    def flush_all
      batch = nil
      @mutex.synchronize do
        batch = @buffer.dup unless @buffer.empty?
        @buffer.clear
      end
      flush_batch(batch) if batch
    end

    def flush_batch(batch)
      return if batch.empty?

      appender = @con.appender(@table)
      batch.each do |tag, time_str, json|
        appender.begin_row
        appender.append(tag)
        appender.append(time_str)
        appender.append(json)
        appender.end_row
      end
      appender.flush
      appender.close
    end
  end
end
