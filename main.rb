#!/usr/bin/ruby
# encoding: utf-8

require "rubygems"
require 'kraken.rb'

Kraken.crawl(["http://www.ruby-lang.org/de/","http://www.cnet.com"]) do |kraken|
  
  kraken.on_every_page do |page|
    #puts page.body[0..540] if page.html?
  end
  
end
