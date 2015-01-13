class Stats
  
  attr_reader :since, :until  
  
  def initialize(s="2013-01-01",u=DateTime.now)
    @since = s
    @until = u
    @stats = Hash.new
  end
  
  def digitized_audio
    return Carrier.count(:events => {:event_lookup_id => 5, :event_date.gte => @since, :event_date.lte => @until}, :carrier_type_id => 1)
  end 
  
  def digitized_video
    return Carrier.count(:events => {:event_lookup_id => 5, :event_date.gte => @since, :event_date.lte => @until}, :carrier_type_id => 2)
  end
  
  def digitized_paper
    return Carrier.count(:events => {:event_lookup_id => 5, :event_date.gte => @since, :event_date.lte => @until}, :carrier_type_id => 3)
  end
  
  def digitized_all
    self.digitized_audio + self.digitized_video + self.digitized_paper
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
      @stats = {:digitised => {:audio => self.digitized_audio, :video => self.digitized_video, :paper => self.digitized_paper, :all => self.digitized_all},
                  :registered => {:audio => self.registered_audio, :video => self.registered_video, :paper => self.registered_paper, :all => self.registered_all},
                  :archived => {:all => self.archived_all, :bytes => self.archived_bytes, :terabytes => self.archived_terabytes},
                  :ingested => {:all => self.ingested_all}}
    when "archived"
      @stats = {:archived => {:all => self.archived_all, :bytes => self.archived_bytes, :terabytes => self.archived_terabytes}}
    when "digitised"
      @stats = {:digitised => {:audio => self.digitized_audio, :video => self.digitized_video, :paper => self.digitized_paper, :all => self.digitized_all}}
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