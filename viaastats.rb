require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)

### The data is base ###

conf = YAML.load_file('config.yaml')

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
    property :is_digitised,     Integer
    property :created_on ,      DateTime
    
    has n, :events, :model => 'Carrierevent'
    has n, :paper_event, :model => 'Paperevent'
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

class Paperevent
  include DataMapper::Resource
  is :read_only
  
  storage_names[:default] = 'paper_event'
  
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
    
    property :pid,              Serial
    property :carrier_size,     Integer
    property :status,           String
    property :date,             DateTime
    property :content_provider, String
    
    has n, :events, 'Event',
      :parent_key => [:pid],
      :child_key => [:pid]
    
end

class Event
  include DataMapper::Resource
  is :read_only
  
  def self.default_repository_name
      :monitoring
  end
  
  storage_names[:monitoring] = 'events'
  
  property :event_id,         Serial, :field => 'key'
  property :pid,              String
  property :status,           String
  property :date,             DateTime
  
  belongs_to :pid, 'Pid',
    :parent_key => [:pid],
    :child_key => [:pid]

end

DataMapper.finalize

### The Method Man ###

require_relative 'lib/stats'

### Extra's ###

require_relative 'lib/numeric'

### The Sinatra Part ###

class V1 < Sinatra::Base
  
  class MethodError < StandardError; end
  
  register Sinatra::MultiRoute
 
  configure :development do
    register Sinatra::Reloader
    enable :raise_errors
    enable :show_exceptions
    set :dump_errors, true
  end
  
  configure :production do
    set :raise_errors, false
    set :show_exceptions, false
    set :dump_errors, false
  end

  helpers do
    
    def validate(*date)
      date.each do |date|
        begin
          Date.parse(date)
          puts "date ok"
        rescue ArgumentError
          raise MethodError, "Invalid Date"
        end
      end
    end
    
    def datebefore(date1,date2)
      raise MethodError if Date.parse(date1) > Date.parse(date2)
    end
    
    def past(*date)
      date.each do |date|
        raise MethodError if Date.parse(date) > DateTime.now
      end
    end
    
  end

  before do
    content_type :json
  end

  get '/' , '/api', '/api/', '/api/v1/' do
    redirect to('/api/v1'), 303
  end  
  
  get '/api/v1' do
      status 200
      body "GET https://status.viaa.be/api/v1/<status>?<since=YYYY-MM-DD>&<until=YYYY-MM-DD>        
 
/* Where:
    - status:       all, registered, digitised, archived, ingested, published (required, no default)
    - since-until:  optional query string; when specifying until, since has to be provided (defaults to all time if non is given)
    
    Default count is expressed in number of items accept for 'archived' where (t)bytesize is also returned if status is 'all'. 
*/"
  end
  
  
  get '/api/:version/:status' do
    raise MethodError unless params[:version].match /v1/
    raise MethodError unless params[:status].match /archived|ingested|registered|published|digitised|all/
    raise MethodError if params[:until] and not(params[:since])
    
    if params[:since] and not params[:until]
      validate(params[:since])
      past(params[:since])
      stats = Stats.new(params)
    elsif params[:since] and params[:until]
      validate(params[:since],params[:until])
      past(params[:since],params[:until])
      datebefore(params[:since],params[:until])
      stats = Stats.new(params)
    else
      stats = Stats.new(params)
    end
    
    res,req = {},{}
    req[:url] = request.url.to_s
    req[:timestamp] = Time.now
        
    params.each do |k,v|
      req[k.to_sym] = v unless k == "splat" || k == "captures"
    end
    
    res[:data] = stats.give(req[:status])

    payload = {:request => req, :response => res}
    return payload.to_json
  end
  
  get '/api/v1/:status/*' do
    raise MethodError
  end
  
  error 500..510 do
    'Boom'
  end
  
  error MethodError do
    status 400
    { :timestamp => Time.now,
      :error => env['sinatra.error'],
      :message => env['sinatra.error'].message,
      :doc => "Specify one of these /api/v1/<archived>|<ingested>|<registered>|<published>|<digitised>|<all>"
    }.to_json
  end
  
  error Sinatra::NotFound do
    halt 404
  end
  
  run! if __FILE__ == $0

end
