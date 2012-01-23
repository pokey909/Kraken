#!/usr/bin/ruby

require 'rinda/tuplespace'

# Construct our tuple space 
ts = Rinda::TupleSpace.new

DRb.start_service("druby://localhost:12345", ts) 

puts("Server Running: #{DRb.uri}")

# Join with drb thread to avoid exit..
DRb.thread.join

puts("Server Dead")
