#!/usr/bin/ruby
require 'rinda/tuplespace'

# Start drb and connect to server
DRb.start_service
ts=DRbObject.new(nil, ARGV.shift)

observer = ts.notify('write', [:page,nil])

i=0
observer.each do |event, tuple|
  puts("OBJ: #{event.inspect} #{tuple.inspect}")
  10.times { ts.write([:link,i]); i=i+1 }
end


