#!/usr/bin/ruby

require 'rinda/tuplespace'

# Construct our tuple space 
ts = Rinda::TupleSpace.new

# Start a drb server attached to the tuple space A URI string may be
# given: "druby://foobar.dal.design.ti.com:1234" instead of nil.
DRb.start_service("druby://localhost:12345", ts) 

puts("Server Running: #{DRb.uri}")

# Join with drb thread to avoid exit..
DRb.thread.join

puts("Server Dead")
