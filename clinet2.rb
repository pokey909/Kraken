#!/usr/bin/ruby
require 'rinda/tuplespace'

# Start drb and connect to server
DRb.start_service
ts=DRbObject.new(nil, ARGV.shift)

# notification types: 'write', 'take', & 'delete'.  Event type 'close'
# if entry expires..  Second arg is wild card for entries we want to
# watch.
observer = ts.notify('write', [:page,nil])

# Most people fire off a thread to do the watching, but that is all we
# are gonna do in this program...
i=0
observer.each do |event, tuple|
  puts("OBJ: #{event.inspect} #{tuple.inspect}")
  10.times { ts.write([:link,i]); i=i+1 }
end


