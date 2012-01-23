#!/usr/bin/ruby -w
require 'rubygems'
require 'mongo'
require 'open-uri'

class PageProcessor

	def initialize()
		@db = Mongo::Connection.new.db("mydb")
		@db = Mongo::Connection.new("localhost").db("kraken")
		@connection = Mongo::Connection.new # (optional host/port args)
		@connection.database_names.each { |name| puts name }
		@connection.database_info.each { |info| puts info.inspect}
	  @links=@db["links"]
	  @pages=@db["pages"]
    @procs=[]
    self.clearLinks
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
    Signal.trap("HUP") { puts "PageProcessor #{pid} exiting..."; exit; }
    loop do
      while (res = @links.find_and_modify( { :query=>{:processing=>false, :done=>false}, :update=>{"$set"=>{:processing=>true, :pid=>pid}}, :new=>true  } )) do
        ## download page
        @pages.insert({ :body=>get(res["url"]), :url=>res["url"], :pid=>res["pid"]})
        @links.update({"_id"=>res["_id"]}, {"$set"=>{:processing=>false, :done=>true}})
      end
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

p = PageProcessor.new
p.goTest
#p.clearLinks
#p.clearPages
p.showDoneLinks
