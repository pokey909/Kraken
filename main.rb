#!/usr/bin/ruby -w
require 'rubygems'
require 'mongo'
require 'open-uri'
require 'yaml'
require 'amqp'

class PageProcessor

	def initialize()
    @procs=[]
    1000.times { @links.insert( {:url=>"http://www.ruby-doc.org/core-1.9.3/Process.html", :processing=>false, :done=>false, :pid=>0 } )}
	end

	def get(url)
    return open(url) { |f| f.read }
	end

  def clearLinks
    @links.remove
  end

  def clearPages
    @pages.remove
  end

  
  def process
    pid = Process.pid
    t = Thread.new { EventMachine.run }
    sleep(0.5)
    connection = AMQP.connect
    channel    = AMQP::Channel.new(connection, :auto_recovery => true)
    channelOut = AMQP::Channel.new(connection, :auto_recovery => true)
    channel.prefetch(1)
    #exchange   = channel.fanout("kraken.", :durable => true, :auto_delete => false)

    channel.queue("kraken.links.pending", :durable => true, :auto_delete => false).subscribe(:ack => true) do |metadata, payload|
      case metadata.type
      when "isome command"
        data = YAML.load(payload)
        puts "[gems.install] Received a 'gems.install' request with #{data.inspect}"

        # just to demonstrate a realistic example
        shellout = "gem install #{data[:gem]} --version '#{data[:version]}'"
        puts "[gems.install] Executing #{shellout}"; system(shellout)

        puts
        puts "[gems.install] Done"
        puts
      else
        puts "[commands] Unknown command: #{metadata.type}"
        body = get("some url")
        payload = { :url => 'some url', :body => body, :on_page => @blocks[:on_page].call(body) }.to_yaml
        channel.default_exchange.publish(payload,
                                         :type        => "page post",
                                         :routing_key => "kraken.pages.pending")
      end

      # message is processed, acknowledge it so that broker discards it
      metadata.ack
    end
    puts "[PageProcessor] ##{pid} booted."
    Signal.trap("INT") { connection.close { EventMachine.stop } }
    t.join
  end
  
  def on_page(&block)
    @blocks[:on_page] = block
  end

  def spawn(num)
    num.times { @procs << fork { self.process } }
  end

  def stop
    @procs.each { |x| Process.kill("HUP", x); Process.wait } 
  end

  def goTest
    self.spawn(10)
    puts @procs
    sleep 5
    self.stop
  end

  def showDoneLinks
    @links.find({:done=>true}).each { |x| puts x.inspect }
  end
  def showPages
    @pages.find().each { |x| puts x.inspect }
  end
  def countPages
    puts "#{@pages.count} pages!"
  end
end

p = PageProcessor.new
p.goTest
#p.clearLinks
#p.clearPages
p.showDoneLinks
