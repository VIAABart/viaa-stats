require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)

require_relative 'helpers/numeric'

### The data is base ###

conf = YAML.load_file('db.yml')

DataMapper::Logger.new(STDOUT, :debug)

DataMapper.setup(:default, { 
      adapter: conf["db1"]["adapter"],
      database: conf["db1"]["db"],
      username: conf["db1"]["u"],
      password: conf["db1"]["p"],
      host: conf["db1"]["h"],
      port: conf["db1"]["port"]})

DataMapper.setup(:monitoring, {
      adapter: conf["db2"]["adapter"],
      database: conf["db2"]["db"],
      username: conf["db2"]["u"],
      password: conf["db2"]["p"],
      host: conf["db2"]["h"],
      port: conf["db2"]["port"]})

class Carrier
    include DataMapper::Resource
    is :read_only
    
    storage_names[:default] = 'carrier'
    
    property :id,               Serial
    property :carrier_type_id,  Integer
    property :status_id,        Integer 
    property :is_digitized,     Integer
    property :created_on ,      DateTime
    
    has n, :events, :model => 'Carrierevent'
end

class Carrierevent
  include DataMapper::Resource
  is :read_only
  
  storage_names[:default] = 'events'
  
  property :id,               Serial
  property :carrier_id,       Integer
  property :event_lookup_id,  Integer
  property :event_date,       DateTime
  
  belongs_to :carrier, :model => 'Carrier'

end

class Pid
    include DataMapper::Resource
    is :read_only
    
    def self.default_repository_name
        :monitoring
    end
    
    storage_names[:monitoring] = 'pids'
    
    property :id,               Serial
    property :carrier_size,     Integer
    property :status,           String
    property :date,             DateTime
    
end

class Event
  include DataMapper::Resource
  is :read_only
  
  def self.default_repository_name
      :monitoring
  end
  
  storage_names[:monitoring] = 'events'
  
  property :event_id,         Serial
  property :pid,              String
  property :status,           String, :field => 'key'
  property :date,             DateTime

end

DataMapper.finalize

### The Method Man ###

@@since = "2013-01-01"
@@until = DateTime.now

class Stats
  
  #@carrier = {:digitized => {:audio => Carrier.count(:carrier_type_id => 1 , :is_digitized => 1)}}
  
  def initialize
    digitized_audio = Carrier.count(:events => {:event_lookup_id => 5, :event_date.gte => @@since, :event_date.lte => @@until}, :carrier_type_id => 1)
    digitized_video = Carrier.count(:events => {:event_lookup_id => 5, :event_date.gte => @@since, :event_date.lte => @@until}, :carrier_type_id => 2)
    digitized_paper = Carrier.count(:events => {:event_lookup_id => 5, :event_date.gte => @@since, :event_date.lte => @@until}, :carrier_type_id => 3)
    digitized_all = digitized_audio + digitized_video + digitized_paper
    registered_audio = Carrier.count(:carrier_type_id => 1, :created_on.gte => @@since, :created_on.lte => @@until)
    registered_video = Carrier.count(:carrier_type_id => 2, :created_on.gte => @@since, :created_on.lte => @@until)
    registered_paper = Carrier.count(:carrier_type_id => 3, :created_on.gte => @@since, :created_on.lte => @@until)
    registered_all = registered_audio + registered_video + registered_paper
    archived_all = Event.count(:status => 'ARCHIVED_ON_TAPE', :date.gte => @@since, :date.lte => @@until)
    archived_bytes = Pid.sum(:carrier_size, :date.gte => @@since, :date.lte => @@until, :status => 'OK')
    archived_terabytes = archived_bytes.to(:tb,2)
    ingested_all = Pid.count(:date.gte => @@since, :date.lte => @@until)
    @stats = {:digitised => {:audio => digitized_audio, :video => digitized_video, :paper => digitized_paper, :all => digitized_all},
              :registered => {:audio => registered_audio, :video => registered_video, :paper => registered_paper, :all => registered_all},
              :archived => {:all => archived_all, :bytes => archived_bytes, :terabytes => archived_terabytes},
              :ingested => {:all => ingested_all}}
  end
  
  def give(status,type)
    status.to_s == "all" ? @stats : @stats[status.to_sym][type.to_sym]
  end
  
end

### Extra's ###

class Numeric
  def to(unit, places=1)
    units = { :b => 1,
              :kb => 1024**1,
              :mb => 1024**2,
              :gb => 1024**3,
              :tb => 1024**4,
              :pb => 1024**5,
              :eb => 1024**6}
    unitval = units[unit.to_s.downcase.to_sym]
    "#{sprintf("%.#{places}f", self / unitval)}" # "#{unit.to_s.upcase}"
  end # to
end

### The Sinatra Part ###

class V1 < Sinatra::Base
  
  class MethodError < StandardError; end
  class SyntaxError < StandardError; end
  
  register Sinatra::MultiRoute
 
  configure :development do
    register Sinatra::Reloader
    enable :raise_errors
    enable :show_exceptions
    set :dump_errors, true
  end
  
  set :show_exceptions, false
  set :raise_errors, false
  set :dump_errors, false
  
  set :default_type, :all
  set :default_count, :items

  before do
    content_type :json
  end

  get '/', '/api', '/api/' ,'/api/v1/' do
    redirect to('/api/v1'), 303
  end  
  
  get '/api/v1' do
      status 200
      body "GET https://status.viaa.be/api/v1/<status>/<type>/<count>          
 
/* where:
    - status:   all, registered, digitised, archived, ingested, published (required, no default)
    - type:     audio, video, paper, all (default: all)
    - count:    items (default: count expressed in number of items) 
*/"
  end
  
  
  get '/api/v1/:status/?:type?/?:count?' do
    raise MethodError unless params[:status].match /archived|ingested|registered|published|digitised|all/
    
    if params[:type]
      raise SyntaxError unless params[:type].match /video|audio|paper|all/  
    end
    
    if params[:count]
      raise SyntaxError unless params[:count].match /size|time|items/  
    end
    
    if params[:since]
      @@since = params[:since]
    end
    
    if params[:until]
      @@until = params[:until]
    end
    
    res,req = {},{}
    req[:url] = request.url.to_s
    req[:timestamp] = Time.now
    req[:type] = settings.default_type
    req[:count] = settings.default_count
        
    params.each do |k,v|
      req[k.to_sym] = v unless k == "splat" || k == "captures"
    end
    
    stats = Stats.new
    res[:data] = stats.give(req[:status],req[:type])

    jason = {:request => req, :response => res}
    return jason.to_json
  end
  
  get '/api/v1/:status/?:type?/?:count?/*' do
    raise SyntaxError
  end
  
  error MethodError do
    status 404
    {:error => "404 for that one, sorry"}.to_json
  end
  
  error SyntaxError do
    status 404
    {:error => "Missing arguments or syntax error."}.to_json
  end
  
  run! if __FILE__ == $0

end