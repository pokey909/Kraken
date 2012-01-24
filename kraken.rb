#!/usr/bin/ruby -w
require 'rubygems'
require 'mongo'
require 'open-uri'

module Kraken
    
    VERSION = '0.0.1';


    def Kraken.crawl(urls, opts = {}, &block)
    end

########## PAGE PROCESSOR ################

    class PageProcessor

        def initialize(blocks, opts)
            @db = Mongo::Connection.new.db("mydb")
            @db = Mongo::Connection.new("localhost").db("kraken")
            @connection = Mongo::Connection.new # (optional host/port args)
            @connection.database_names.each { |name| puts name }
            @connection.database_info.each { |info| puts info.inspect}
            @links=@db["links"]
            @pages=@db["pages"]
            @procs=[]
            @blocks = blocks
            self.clearLinks
            1000.times { @links.insert( {:url=>"http://www.ruby-doc.org/core-1.9.3/Process.html", :processing=>false, :done=>false, :pid=>0 } )} if opts[:dbg_pre_populate_pages]
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
        Signal.trap("HUP") { puts "PageProcessor #{pid} exiting..."; exit; }
        loop do
          while (res = @links.find_and_modify( { :query=>{:processing=>false, :done=>false}, :update=>{"$set"=>{:processing=>true, :pid=>pid}}, :new=>true  } )) do
            ## download page
            src = get(res["url"]) 
            @pages.insert({ :body=>src, :url=>res["url"], :pid=>res["pid"]}) if @opts[:keep_page_body]

            @blocks[:on_page].each { |blk| blk.call(src) }

            @links.update({"_id"=>res["_id"]}, {"$set"=>{:processing=>false, :done=>true}})
          end
          sleep 0.5
        end
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

################ ENGINE ##########################

    class Engine

        attr_reader :opts

        DEF_OPTS = {
            :pool_size => 4,
            :verbose => true,
            :keep_page_body => true,
            :user_agent => "Kraken/#{Kraken::VERSION}",
            :obey_robots_txt => false,
            :read_timeout => 5,
            :dbg_pre_populate_pages => true
        }

        # Create setter methods for all options to be called from the crawl block
        DEF_OPTS.keys.each do |key|
            define_method "#{key}=" do |value|
                @opts[key.to_sym] = value
            end
        end

        #
        # Initialize the crawl with starting *urls* (single URL or Array of URLs)
        # and optional *block*
        #
        def initialize(urls, opts = {})
            @urls = [urls].flatten.map{ |url| url.is_a?(URI) ? url : URI(url) }
            @urls.each{ |url| url.path = '/' if url.path.empty? }

            @tentacles = []
            @on_every_page_blocks = []
            @on_pages_like_blocks = Hash.new { |hash,key| hash[key] = [] }
            @skip_link_patterns = []
            @after_crawl_blocks = []
            @opts = opts

            @page_proc = PageProcessor.new(@opts)

            yield self if block_given?
        end


        #
        # Start a new crawl
        #
        def self.crawl(urls, opts = {})
            self.new(urls, opts) do |engine|
                yield engine if block_given?
                engine.run
            end
        end

        #
        # Perform the crawl
        #
        def run
            process_options

            @urls.delete_if { |url| !visit_link?(url) }
            return if @urls.empty?

            link_queue = Queue.new
            page_queue = Queue.new

            @page_proc.spawn(@opts[:pool_size])

            @urls.each{ |url| link_queue.enq(url) }

            loop do
                page = page_queue.deq
                @pages.touch_key page.url
                puts "#{page.url} Queue: #{link_queue.size}" if @opts[:verbose]
                do_page_blocks page
                page.discard_doc! if @opts[:discard_page_bodies]

                links = links_to_follow page
                links.each do |link|
                    link_queue << [link, page.url.dup, page.depth + 1]
                end
                @pages.touch_keys links

                @pages[page.url] = page

                # if we are done with the crawl, tell the threads to end
                if link_queue.empty? and page_queue.empty?
                    until link_queue.num_waiting == @tentacles.size
                      Thread.pass
                    end
                    if page_queue.empty?
                      @tentacles.size.times { link_queue << :END }
                      break
                    end
                end
            end

            @tentacles.each { |thread| thread.join }
            do_after_crawl_blocks
            self
        end
    end
