# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-duckdb"
  spec.version       = "0.1.0"
  spec.authors       = ["Zafer Balkan"]
  spec.email         = ["zafer@zaferbalkan.com"]

  spec.summary       = %q{Fluentd output plugin for DuckDB}
  spec.description   = %q{
    A Fluentd output plugin that writes events into a DuckDB table.
    Records are stored in a JSON column, enabling rich queries with
    DuckDB’s native JSON functions and operators.
  }
  spec.homepage      = "https://github.com/zbalkan/fluent-plugin-duckdb"
  spec.license       = "Apache-2.0"

  spec.files         = Dir["lib/**/*.rb", "README.md", "LICENSE*"]
  spec.require_paths = ["lib"]

  spec.add_dependency "fluentd", ">= 1.0"
  spec.add_dependency "duckdb", ">= 1.3.1.0"
  spec.add_dependency "yajl-ruby", ">= 1.3"

  spec.add_development_dependency "rake", ">= 12"
  spec.add_development_dependency "test-unit", ">= 3.3"
end
