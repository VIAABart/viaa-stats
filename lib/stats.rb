require 'time'

class Stats
  
  attr_reader :since, :until  
  
  def initialize(s="2001-01-01",u=DateTime.now)
    @since = s
    @until = u
    @stats = Hash.new
  end
  
  def digitised_audio
    @da = repository(:default).adapter.select("select count(distinct pid) from carrier where id in (select distinct carrier_id from events where event_lookup_id = 5 and event_date >= '#{@since}' AND event_date <= '#{@until}') and carrier_type_id = 1;")[0]
    return @da
    # Carrier.count(:events => {:event_lookup_id => 6, :event_date.gte => @since, :event_date.lte => @until}, :carrier_type_id => 1)
  end 
  
  def digitised_video
    @dv = repository(:default).adapter.select("select count(distinct pid) from carrier where id in (select distinct carrier_id from events where event_lookup_id = 5 and event_date >= '#{@since}' AND event_date <= '#{@until}' OR MONTH(event_date) = 10 AND DAY(event_date) = 14) and carrier_type_id = 2;")[0] 
    return @dv
    # Carrier.count(:events => {:event_lookup_id => 6, :event_date.gte => @since, :event_date.lte => @until}, :carrier_type_id => 2)
  end
  
  def digitised_paper
    @dp = repository(:default).adapter.select("select count(distinct carrier_id) from paper_event where event_lookup_id = 5 and event_date >= '#{@since}' AND event_date <= '#{@until}';")[0] 
    return @dp
    #Carrier.count(:paper_event => {:event_lookup_id => 5, :event_date.gte => @since, :event_date.lte => @until}, :carrier_type_id => 3)
  end
  
  def digitised_all
    return @da+@dv+@dp
    #Carrier.count(:is_digitised => 1)
  end
  
  def registered_audio
    return Carrier.count(:carrier_type_id => 1, :created_on.gte => @since, :created_on.lte => @until)
  end
  
  def registered_video
    return Carrier.count(:carrier_type_id => 2, :created_on.gte => @since, :created_on.lte => @until)
  end
  
  def registered_paper
    return Carrier.count(:carrier_type_id => 3, :created_on.gte => @since, :created_on.lte => @until)
  end
  
  def registered_all
    self.registered_audio + self.registered_video + self.registered_paper
  end
  
  def archived_all
    Event.count(:status => 'ARCHIVED_ON_VAULT', :date.gte => @since, :date.lte => @until)
  end
  
  def archived_bytes
    Pid.sum(:carrier_size, :date.gte => @since, :date.lte => @until, :status => 'OK')
  end
  
  def archived_terabytes
    self.archived_bytes.to(:tb,2)
  end
  
  def ingested_all
    Pid.count(:date.gte => @since, :date.lte => @until)
  end
    
  def give(status)
    case status
    when "all"
      @stats = {:digitised => {:audio => self.digitised_audio, :video => self.digitised_video, :paper => self.digitised_paper, :all => self.digitised_all},
                  :registered => {:audio => self.registered_audio, :video => self.registered_video, :paper => self.registered_paper, :all => self.registered_all},
                  :archived => {:all => self.archived_all, :bytes => self.archived_bytes, :terabytes => self.archived_terabytes},
                  :ingested => {:all => self.ingested_all}}
    when "archived"
      @stats = {:archived => {:all => self.archived_all, :bytes => self.archived_bytes, :terabytes => self.archived_terabytes}}
    when "digitised"
      @stats = {:digitised => {:audio => self.digitised_audio, :video => self.digitised_video, :paper => self.digitised_paper, :all => self.digitised_all}}
    when "registered"
      @stats = {:registered => {:audio => self.registered_audio, :video => self.registered_video, :paper => self.registered_paper, :all => self.registered_all}}
    when "ingested"
      @stats = {:ingested => {:all => self.ingested_all}}
    else
      @stats = {:data => "0"}
    end
    return @stats
  end
  
end