#!/usr/bin/ruby
require 'rinda/tuplespace'

# Start drb and connect to server
DRb.start_service
ts=DRbObject.new(nil, ARGV.shift)

# Add some tuples
ts.write([:page, 'foo'])
puts "Written"

# notification types: 'write', 'take', & 'delete'.  Event type 'close'
# if entry expires..  Second arg is wild card for entries we want to
# watch.
observer = ts.notify('write', [:link,nil])

# Most people fire off a thread to do the watching, but that is all we
# are gonna do in this program...
observer.each do |event, tuple|
  puts("OBJ: #{event.inspect} #{tuple.inspect}")
  ts.write([:page,'moooore'])
end


