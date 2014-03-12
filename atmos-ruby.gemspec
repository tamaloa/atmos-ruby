# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |s|
  s.platform = Gem::Platform::RUBY
  s.summary = "Ruby API wrapper for EMC Atmos"
  s.name = 'atmos-ruby'
  s.version = "1.4.0.7"
  s.homepage = "http://code.google.com/p/atmos-ruby"
  s.authors = ['jasoncwik']
  s.requirements << 'none'
  s.description = <<EOF
  Atmos-java is a API wrapper that provides access to the EMC Atmos REST API.
EOF

  s.files         = `git ls-files -z`.split("\x0")
  s.test_files    = s.files.grep(%r{^(test|spec|features)/})
  s.require_paths = ["lib"]

  s.add_development_dependency "bundler", "~> 1.5"
  s.add_development_dependency "rake"
  s.add_development_dependency "test-unit"

  s.add_runtime_dependency 'ruby-hmac', '>=0.4.0'
  s.add_runtime_dependency 'nokogiri', '>1.4'
end