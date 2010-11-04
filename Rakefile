require 'rubygems'
require 'rake/gempackagetask'

task :default => [:gem]

spec = Gem::Specification.new do |s|
  s.platform = Gem::Platform::RUBY
  s.summary = "Ruby API wrapper for EMC Atmos"
  s.name = 'atmos-ruby'
  s.version = "1.4.0.7"
  s.homepage = "http://code.google.com/p/atmos-ruby"
  s.requirements << 'none'
  s.require_path = 'lib'
  s.files = ["lib/EsuApi.rb"]
  s.description = <<EOF
  Atmos-java is a API wrapper that provides access to the EMC Atmos REST API.
EOF
  s.add_runtime_dependency 'ruby-hmac', '>=0.4.0'
  s.add_runtime_dependency 'nokogiri', '>1.4'
end
  
Rake::GemPackageTask.new(spec) do |pkg|
    pkg.need_zip = true
    pkg.need_tar = true
end
 