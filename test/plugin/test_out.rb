require "test/unit"
require "fluent/test"
require "fluent/test/driver/output"
require "fluent/test/helpers"
require "fluent/plugin/out_duckdb"

class DuckdbOutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  CONFIG = %[
    @type duckdb
    database :memory:
    table test_events
    time_col time
    tag_col tag
    record_col record
  ]

  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::DuckdbOutput).configure(conf)
  end

  def test_write_and_query
    d = create_driver
    tag = "test"
    time = event_time("2025-01-01 00:00:00 UTC")
    record = { "user" => { "id" => 123 } }

    d.run(default_tag: tag) { d.feed(time, record) }

    # Access connection to query the DuckDB directly
    con = d.instance.instance_variable_get(:@con)
    result = con.query("SELECT record->>'id' AS id FROM test_events")

    rows = result.map(&:first)
    assert_equal ["123"], rows
  end
end
