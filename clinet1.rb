#!/usr/bin/ruby
require 'rinda/tuplespace'

# Start drb and connect to server
DRb.start_service
ts=DRbObject.new(nil, ARGV.shift)

# Add some tuples
ts.write([:page, 'foo'])
puts "Written"

observer = ts.notify('write', [:link,nil])

observer.each do |event, tuple|
  puts("OBJ: #{event.inspect} #{tuple.inspect}")
  ts.write([:page,'moooore'])
end


