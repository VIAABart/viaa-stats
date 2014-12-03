require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)

### The data is base ###

db = YAML.load_file('db.yml')
env = "development"

DataMapper::Logger.new($stdout, :debug)

DataMapper.setup(:default, "mysql://#{db[env]["u"]}:#{db[env]["p"]}@#{db[env]["h"]}/#{db[env]["db"]}")

class Carrier
    include DataMapper::Resource
    is :read_only
    
    storage_names[:default] = 'carrier'
    
    property :id,               Serial
    property :carrier_type_id,  Integer
    property :status_id,        Integer 
    property :is_digitized,     Integer
    
    has 1, :carrier_type
end

class CarrierType
    include DataMapper::Resource
    is :read_only
    
    storage_names[:default] = 'carrier_type'
    
    property :id,         Serial
    property :name,       String
    
    belongs_to :carrier
    
end

DataMapper.finalize

### The Method Man ###

class Stats
  
  #@carrier = {:digitized => {:audio => Carrier.count(:carrier_type_id => 1 , :is_digitized => 1)}}
  
  def initialize
    digitized_audio = Carrier.count(:carrier_type_id => 1 , :is_digitized => 1)
    digitized_video = Carrier.count(:carrier_type_id => 2 , :is_digitized => 1)
    digitized_paper = Carrier.count(:carrier_type_id => 3 , :is_digitized => 1)
    digitized_all = Carrier.count(:is_digitized => 1)
    registered_audio = Carrier.count(:carrier_type_id => 1)
    registered_video = Carrier.count(:carrier_type_id => 2)
    registered_paper = Carrier.count(:carrier_type_id => 3)
    registered_all = Carrier.count
    @stats = {:digitised => {:audio => digitized_audio, :video => digitized_video, :paper => digitized_paper, :all => digitized_all},
              :registered => {:audio => registered_audio, :video => registered_video, :paper => registered_paper, :all => registered_all}}
  end
  
  def give(status,type)
    status.to_s == "all" ? @stats : @stats[status.to_sym][type.to_sym]
  end
  
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
  set :default_unit, :items

  before do
    content_type :json
  end

  get '/', '/api', '/api/' ,'/api/v1/' do
    redirect to('/api/v1'), 303
  end  
  
  get '/api/v1' do
      status 200
      body "GET https://status.viaa.be/api/v1/<status>/<type>/<unit>          
 
/* where:
    - status:   all, registered, digitized, archived, published (required, no default)
    - type:     audio, video, paper, all (default: all)
    - unit:     items, hours, size (default: size expressed in items) 
*/"
  end
  
  
  get '/api/v1/:status/?:type?/?:unit?' do
    raise MethodError unless params[:status].match /archived|registered|published|digitised|all/
    
    if params[:type]
      raise SyntaxError unless params[:type].match /video|audio|paper|all/  
    end
    
    if params[:unit]
      raise SyntaxError unless params[:unit].match /size|time|items/  
    end
    
    res,req = {},{}
    req[:url] = request.url.to_s
    req[:timestamp] = Time.now
    req[:type] = settings.default_type
    req[:unit] = settings.default_unit
        
    params.each do |k,v|
      req[k.to_sym] = v unless k == "splat" || k == "captures"
    end
    
    stats = Stats.new
    res[:data] = stats.give(req[:status],req[:type])

    jason = {:request => req, :response => res}
    return jason.to_json
  end
  
  get '/api/v1/:status/?:type?/?:unit?/*' do
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