#!/usr/bin/ruby -w
require 'rubygems'
require 'open-uri'
require 'amqp'
require "amqp/extensions/rabbitmq"
require 'yaml'

module Kraken
    
    VERSION = '0.0.1';


    def Kraken.crawl(opts = {}, &block)
      PageProcessor.start(opts, &block)
    end

########## PAGE PROCESSOR ################

    class PageProcessor

        def initialize(opts)
          @links=[]
          @pages=[]
          @procs=[]
          @blocks={}
          @blocks[:on_every_page] = []
          @skip_link_patterns = []
          yield self if block_given?
        end

        def get(url)
            return open(url) { |f| f.read }
        end
        
        def self.start(opts = {})
          self.new(opts) do |processor|
            yield processor if block_given?
            processor.goTest
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
          @channel.prefetch(10)
          @exchange   = @channel.direct("")
          @queue = @channel.queue("kraken.links", :durable => true, :auto_delete => true)
          
          100.times { 
            #puts "[#{pid}]Posting 100 links..."
            @exchange.publish "http://www.ruby-doc.org/core-1.9.3/Process.html", :routing_key => "kraken.links", :type => "new_link"
          }# if opts[:dbg_pre_populate_pages]
          @queue.subscribe(:ack => true) do |metadata, payload|
            case metadata.type
            when "new_link"
              #data = YAML.load(payload)
              #puts "[Worker #{pid}] Received a new_link request with #{payload}"
              src = get(payload) 
              @blocks[:on_every_page].each { |blk| blk.call(src) }
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

      def goTest
        self.spawn(10)
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

################ ENGINE ##########################

#    class Engine
#
#        attr_reader :opts
#
#        DEF_OPTS = {
#            :pool_size => 4,
#            :verbose => true,
#            :keep_page_body => true,
#            :user_agent => "Kraken/#{Kraken::VERSION}",
#            :obey_robots_txt => false,
#            :read_timeout => 5,
#            :dbg_pre_populate_pages => true
#        }
#
#        # Create setter methods for all options to be called from the crawl block
#        DEF_OPTS.keys.each do |key|
#            define_method "#{key}=" do |value|
#                @opts[key.to_sym] = value
#            end
#        end
#
#        #
#        # Initialize the crawl with starting *urls* (single URL or Array of URLs)
#        # and optional *block*
#        #
#        def initialize(urls, opts = {})
#            @urls = [urls].flatten.map{ |url| url.is_a?(URI) ? url : URI(url) }
#            @urls.each{ |url| url.path = '/' if url.path.empty? }
#
#            @tentacles = []
#            @on_every_page_blocks = []
#            @on_pages_like_blocks = Hash.new { |hash,key| hash[key] = [] }
#            @skip_link_patterns = []
#            @after_crawl_blocks = []
#            @opts = opts
#
#            @page_proc = PageProcessor.new(@opts)
#            @blocks = {}
#
#            yield self if block_given?
#        end
#
#
#        #
#        # Start a new crawl
#        #
#        def self.crawl(urls, opts = {})
#            self.new(urls, opts) do |engine|
#                yield engine if block_given?
#                engine.run
#            end
#        end
#
#        def on_page(&block)
#            @blocks[:on_page] << block
#        end
#
#        #
#        # Perform the crawl
#        #
#        def run
#            process_options
#
#            @urls.delete_if { |url| !visit_link?(url) }
#            return if @urls.empty?
#
#            link_queue = Queue.new
#            page_queue = Queue.new
#
#            @page_proc.spawn(@opts[:pool_size])
#
#            @urls.each{ |url| link_queue.enq(url) }
#
#            loop do
#                page = page_queue.deq
#                @pages.touch_key page.url
#                puts "#{page.url} Queue: #{link_queue.size}" if @opts[:verbose]
#                do_page_blocks page
#                page.discard_doc! if @opts[:discard_page_bodies]
#
#                links = links_to_follow page
#                links.each do |link|
#                    link_queue << [link, page.url.dup, page.depth + 1]
#                end
#                @pages.touch_keys links
#
#                @pages[page.url] = page
#
#                # if we are done with the crawl, tell the threads to end
#                if link_queue.empty? and page_queue.empty?
#                    until link_queue.num_waiting == @tentacles.size
#                      Thread.pass
#                    end
#                    if page_queue.empty?
#                      @tentacles.size.times { link_queue << :END }
#                      break
#                    end
#                end
#            end
#
#            @tentacles.each { |thread| thread.join }
#            do_after_crawl_blocks
#            self
#        end
end