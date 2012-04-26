#!/usr/bin/ruby -w
require 'rubygems'
require 'open-uri'
require 'amqp'
require "amqp/extensions/rabbitmq"
require 'yaml'
require 'http.rb'
require 'page.rb'

module Kraken
    
    VERSION = '0.0.1';


    def Kraken.crawl(opts = {}, &block)
      PageProcessor.start(opts, &block)
    end
 
########## PAGE PROCESSOR ################

    class PageProcessor

        def initialize(opts)
          @http = Kraken::HTTP.new(opts)
         
          @links=[]
          @pages=[]
          @procs=[]
          @blocks={}          
          @blocks[:on_every_page] = []
          @skip_link_patterns = []
          yield self if block_given?
        end

        def get(url)
           # return open(url) { |f| f.read }
           return 
        end
        
        def self.start(opts = {})
          self.new(opts) do |processor|
            yield processor if block_given?
            processor.replicate(1) # spawn 20 worker processes
          end
        end

        #
        # Add one ore more Regex patterns for URLs which should not be
        # followed
        #
        def skip_links_like(*patterns)
          @skip_link_patterns.concat [patterns].flatten.compact
          self
        end
        
        #
        # Add a block to be executed on every Page as they are encountered
        # during the crawl
        #
        def on_every_page(&block)
          @blocks[:on_every_page] << block
          self
        end
    
        #
        # Returns +true+ if *link* should not be visited because
        # its URL matches a skip_link pattern.
        #
        def skip_link?(link)
          @skip_link_patterns.any? { |pattern| link.path =~ pattern }
        end
    
      def clearLinks
      end

      def clearPages
      end

      def process
        pid = Process.pid
        puts "Starting worker #{pid}..."
        Signal.trap("INT") { puts "Exiting..."; @connection.close { EventMachine.stop }; exit }
        EventMachine.run do
          @connection = AMQP.connect(:host => '127.0.0.1')
          puts "Connected to AMQP broker. Running #{AMQP::VERSION} version of the gem..."
          @channel = AMQP::Channel.new(@connection, :auto_recovery => true)
          @channel.prefetch(1)
          @exchange   = @channel.direct("")
          @queue = @channel.queue("kraken.links", :durable => true, :auto_delete => false)
          
          100.times { 
            #puts "[#{pid}]Posting 100 links..."
            @exchange.publish(  "http://www.ruby-doc.org/core-1.9.3/Process.html", 
                                :routing_key => "kraken.links", 
                                :type => "new_link",
                                :headers => {
                                  :referrer => "http://www.google.com"
                                }
                              )
          }# if opts[:dbg_pre_populate_pages]
          @queue.subscribe(:ack => true) do |metadata, payload|
            case metadata.type
            when "new_link"
              #data = YAML.load(payload)
              #puts "[Worker #{pid}] Received a new_link request with #{payload}"
              page = @http.fetch_page(payload, metadata.headers.nil? ? nil : metadata.headers["referrer"])
              src = page.body
              @blocks[:on_every_page].each { |blk| blk.call(page) }
              metadata.ack
            else
              puts "[commands] Unknown command: #{metadata.type}"
            end
          end
        end
      end
      
      def spawn(num)
        num.times { @procs << fork { self.process } }
      end

      def stop
        @procs.each { |x| Process.kill("INT", x); Process.wait } 
      end

      def replicate(num)
        self.spawn(num)
        puts @procs
        puts ">> Press key to stop <<"
        STDIN.getc 
        self.stop
      end

      def showDoneLinks
      end

      def showPages
      end

      def countPages
      end
    end
end