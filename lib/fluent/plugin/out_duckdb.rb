# lib/fluent/plugin/out_duckdb.rb
require 'fluent/plugin/output'
require 'duckdb'
require 'yajl'
require 'digest'
require 'fluent/error'

module Fluent::Plugin
  class DuckdbOutput < Fluent::Plugin::Output
    Fluent::Plugin.register_output("duckdb", self)

    helpers :compat_parameters

    # ---- Configuration ----
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
    config_param :dedupe, :bool, default: true

    # Delegate buffering/retries to Fluentd
    config_section :buffer do
      # sensible default; users may override in fluent.conf
      config_set_default :chunk_keys, ['time','tag']
    end

    # DuckDB prefers a single writer
    def multi_workers_ready?; false; end

    # ---- Lifecycle ----
    def start
      super
      connect_database
      ensure_schema
      prepare_statements
    end

    def shutdown
      finalize_statements
      close_database
      super
    end

    # ---- Main write path (one transaction per chunk) ----
    def write(chunk)
      tag  = chunk.metadata.tag
      rows = build_rows(chunk, tag)
      with_transaction { insert_rows(rows) }
    rescue => e
      rollback_if_needed
      raise  # let Fluentd buffer handle retries/backoff
    end

    private

    # ===== Connection lifecycle =====
    def connect_database
      return if @con
      @db = DuckDB::Database.open(@database)
      @con = @db.connect
    end

    def close_database
      @con&.close
      @db&.close
    end

    # ===== Schema & prepared statements =====
    def ensure_schema
      ddl = <<~SQL
        CREATE TABLE IF NOT EXISTS #{@table} (
          #{@tag_col} VARCHAR,
          #{@time_col} TIMESTAMP,   -- stored as UTC with up to 9-digit fractional seconds
          #{@record_col} JSON
          #{ dedupe? ? ", record_hash VARCHAR, UNIQUE(#{@tag_col}, #{@time_col}, record_hash)" : "" }
        );
      SQL
      @con.query(ddl)
    end

    def prepare_statements
      @insert_sql  = build_insert_sql
      @insert_stmt = @con.prepare(@insert_sql)
    end

    def finalize_statements
      @insert_stmt&.destroy
      @insert_stmt = nil
    end

    def build_insert_sql
      if dedupe?
        "INSERT INTO #{@table}(#{@tag_col}, #{@time_col}, #{@record_col}, record_hash)
         VALUES (?, CAST(? AS TIMESTAMP), CAST(? AS JSON), ?)"
      else
        "INSERT INTO #{@table}(#{@tag_col}, #{@time_col}, #{@record_col})
         VALUES (?, CAST(? AS TIMESTAMP), CAST(? AS JSON))"
      end
    end

    def schema_fatal?(e)
      msg = e.message.downcase
      msg.include?('type mismatch') || msg.include?('invalid json')
    end

    # ===== Chunk → rows =====
    def build_rows(chunk, tag)
      rows = []
      chunk.each do |time, record|
        ts_str = encode_timestamp(time)      # UTC ISO-like string with 9-digit ns
        json   = encode_record_json(record)  # JSON text
        if dedupe?
          rh = compute_record_hash(tag, ts_str, json)
          rows << [tag, ts_str, json, rh]
        else
          rows << [tag, ts_str, json]
        end
      end
      rows
    end

    def encode_timestamp(time)
      # Preserve nanoseconds by formatting explicitly.
      sec  = time.to_i
      nsec = time.nsec
      base = Time.at(sec, 0).utc.strftime("%Y-%m-%d %H:%M:%S")
      "#{base}.#{sprintf('%09d', nsec)}Z"
    end

    def encode_record_json(record)
      Yajl::Encoder.encode(record)
    end

    def compute_record_hash(tag, ts, json)
      Digest::SHA256.hexdigest("#{tag}|#{ts}|#{json}")
    end

    def dedupe?
      @dedupe
    end

    # ===== Transactions & inserts =====
    def with_transaction
      begin_transaction
      yield
      commit_transaction
    end

    def begin_transaction
      @con.query('BEGIN')
      @in_tx = true
    end

    def commit_transaction
      @con.query('COMMIT')
      @in_tx = false
    end

    def rollback_if_needed
      return unless @in_tx
      @con.query('ROLLBACK') rescue nil
      @in_tx = false
    end

    def insert_rows(rows)
      if dedupe?
        rows.each do |tag, ts, rec, rh|
          @insert_stmt.bind(1, tag)
          @insert_stmt.bind(2, ts)
          @insert_stmt.bind(3, rec)
          @insert_stmt.bind(4, rh)
          @insert_stmt.execute
        end
      else
        rows.each do |tag, ts, rec|
          @insert_stmt.bind(1, tag)
          @insert_stmt.bind(2, ts)
          @insert_stmt.bind(3, rec)
          @insert_stmt.execute
        end
      end
    end
  end
end
