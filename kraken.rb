require 'rubygems'
require 'open-uri'
require 'amqp'
require "amqp/extensions/rabbitmq"
require 'yaml'
require 'http.rb'
require 'page.rb'
require 'snappy'
require 'digest/md5'

module Kraken
    
  VERSION = '0.0.1';


  def Kraken.crawl(url, opts = {}, &block)
    PageProcessor.start(url, opts, &block)
  end
 
########## PAGE PROCESSOR ################

  class PageProcessor
    # Hash of options for the crawl
    attr_reader :opts

    DEFAULT_OPTS = {
      # run 4 Tentacle threads to fetch pages
      :threads => 4,
      # disable verbose output
      :verbose => false,
      # don't throw away the page response body after scanning it for links
      :discard_page_bodies => false,
      # identify self as Anemone/VERSION
      :user_agent => "Kraken/#{Kraken::VERSION}",
      # no delay between requests
      :delay => 0,
      # don't obey the robots exclusion protocol
      :obey_robots_txt => false,
      # by default, don't limit the depth of the crawl
      :depth_limit => false,
      # number of times HTTP redirects will be followed
      :redirect_limit => 5,
      # storage engine defaults to Hash in +process_options+ if none specified
      :storage => nil,
      # Hash of cookie name => value to send with HTTP requests
      :cookies => nil,
      # accept cookies from the server and send them back?
      :accept_cookies => false,
      # skip any link with a query string? e.g. http://foo.com/?u=user
      :skip_query_strings => false,
      # proxy server hostname 
      :proxy_host => nil,
      # proxy server port number
      :proxy_port => false,
      # HTTP read timeout in seconds
      :read_timeout => nil
    }

    # Create setter methods for all options to be called from the crawl block
    DEFAULT_OPTS.keys.each do |key|
      define_method "#{key}=" do |value|
        @opts[key.to_sym] = value
      end
    end
    
    def initialize(url, opts)
      @http = Kraken::HTTP.new(opts)
     
      @links=url
      @pages=[]
      @procs=[]
      @blocks={}          
      @blocks[:on_every_page] = []
      @skip_link_patterns = []
      @opts = opts
      @link_hash = Hash.new
      
      yield self if block_given?
    end

    def self.start(url, opts = {})
      self.new(url, opts) do |processor|
        yield processor if block_given?
        processor.replicate(5) # spawn 20 worker processes
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
  
    def process
      pid = Process.pid
      puts "Starting worker #{pid}..."
      Signal.trap("HUP") { puts "Exiting..."; @connection.close { EventMachine.stop }; exit }
      EventMachine.run do
        @connection = AMQP.connect(:host => '127.0.0.1')
        puts "Connected to AMQP broker. Running #{AMQP::VERSION} version of the gem..."
        @channel = AMQP::Channel.new(@connection, :auto_recovery => true)
        @channel.prefetch(1)
        @exchange   = @channel.direct("")
        @queue = @channel.queue("kraken.links", :durable => true, :auto_delete => false)
        
        @queue.subscribe(:ack => true) do |metadata, payload|
          case metadata.type
          when "new_link"
            data = YAML.load(Snappy.inflate(payload))
#            puts "[Worker #{pid}] Received a new_link request with #{data.inspect}"
#            puts "        Fetching #{data[:url]}..."
            page = @http.fetch_page(data[:url], metadata.headers.nil? ? nil : metadata.headers["referrer"])
#            puts "        OK!"
            src = page.body
            @blocks[:on_every_page].each { |blk| blk.call(page) }
            
            page.links.each do |link|     
              md5 = Digest::MD5.new << link.to_s
              compressed = Snappy.deflate( {:url => link.to_s, :referer => data[:url], :md5 => md5.to_s}.to_yaml )
              @exchange.publish( 
                      compressed,
                      :routing_key => "kraken.hash_check", 
                      :type => "Snappy/MD5"
                      )
            end

            metadata.ack
#            @queue.status { |num_msg, num_consumers|
#              if (num_msg==0) 
#                puts "Exiting..."; @connection.close { EventMachine.stop }; 
#              else
#                puts "Msg: #{num_msg}  /  Consumers: #{num_consumers}"
#              end
#            }
          else
            puts "Unknown metatype: #{metadata.type}"
          end
        end
      end
      puts "Process #{pid} done!"
    end
    
    def spawn(num)
      num.times { @procs << fork { self.process } }
    end

    def stop
      @procs.each { |x| Process.kill("HUP", x); Process.wait } 
    end

    def replicate(num)
      self.spawn(num)
      link_hash_checker
 #     self.stop
    end

    def link_hash_checker
      puts "Starting link hash checker..."
      Signal.trap("INT") { puts "Main Exiting..."; @connection.close { EventMachine.stop }; self.stop }
      EventMachine.run do
        @connection = AMQP.connect(:host => '127.0.0.1')
        puts "Connected to AMQP broker. Running #{AMQP::VERSION} version of the gem..."
        @channel = AMQP::Channel.new(@connection, :auto_recovery => true)
        @channel.prefetch(1)
        @exchange   = @channel.direct("")
        @queue = @channel.queue("kraken.hash_check", :durable => true, :auto_delete => false)

        # post a start link to work on
        @links.each do |link|
          payload = { :url => link,
                      :referer => "http://www.google.com",
                      :some_attr => { :a_hash => ["is a list", "of strings"] }
                    }
          @exchange.publish(  Snappy.deflate(payload.to_yaml), 
                              :routing_key => "kraken.links", 
                              :type => "new_link"
                            )
        end
        
        @queue.subscribe(:ack => true) do |metadata, payload|
          case metadata.type
          when "Snappy/MD5"
            data = YAML.load(Snappy.inflate(payload))
#            puts "[Worker #{pid}] Received a new_link request with #{data.inspect}"
#            puts "        Fetching #{data[:url]}..."

            if !@link_hash.has_key?(data[:url])
              @link_hash[data[:url]] = Time.now
              @exchange.publish( 
                      payload,
                      :routing_key => "kraken.links", 
                      :type => "new_link"
                      )
            else
             # puts "Page #{data[:url]} last crawled on #{@link_hash[data[:url]]}"
            end
            metadata.ack

            @queue.status { |num_msg, num_consumers|
              if (num_msg==0) 
                #puts "Exiting..."; @connection.close { EventMachine.stop }; 
              else
                puts "Msg: #{num_msg}  /  Consumers: #{num_consumers}"
              end
            }
          else
            puts "Unknown metatype: #{metadata.type}"
          end
        end
      end
      puts "Main process done!"
    end
  
    def showDoneLinks
    end

    def showPages
    end

    def countPages
    end
  end
end