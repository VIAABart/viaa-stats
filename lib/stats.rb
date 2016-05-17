require 'time'

class Stats
  
  attr_reader :since, :until  
  
  def initialize(opts={})
    @options = opts
    @since = opts[:since] ? DateTime.parse(opts[:since]).iso8601 : "1901-01-01T00:00:00Z"
    @until = opts[:until] ? DateTime.parse(opts[:until]).iso8601 : Time.now.iso8601
    @options[:tenant] ? @tenant = @options[:tenant] : @tenant = nil
    @stats = Hash.new
  end
  
  def digitised_audio
    @da = repository(:default).adapter.select("select count(distinct pid) from carrier where id in (select distinct carrier_id from events where event_lookup_id = 5 and event_outcome = 1 and event_date >= '#{@since}' AND event_date <= '#{@until}') and carrier_type_id = 1;")[0]
    return @da
    # Carrier.count(:events => {:event_lookup_id => 5, :event_date.gte => @since, :event_date.lte => @until}, :carrier_type_id => 1)
  end 
  
  def digitised_video
    @dv = repository(:default).adapter.select("select count(distinct pid) from carrier where id in (select distinct carrier_id from events where event_lookup_id = 5 and event_outcome = 1 and event_date >= '#{@since}' AND event_date <= '#{@until}' OR MONTH(event_date) = 10 AND DAY(event_date) = 14) and carrier_type_id = 2;")[0] # shiver and shrug
    return @dv
    # Carrier.count(:events => {:event_lookup_id => 5, :event_date.gte => @since, :event_date.lte => @until}, :carrier_type_id => 2)
  end
  
  def digitised_paper
    @dp = repository(:default).adapter.select("select count(distinct carrier_id) from paper_event where event_lookup_id = 5 and event_outcome = 1 and event_date >= '#{@since}' AND event_date <= '#{@until}';")[0] 
    return @dp
    #Carrier.count(:paper_event => {:event_lookup_id => 5, :event_date.gte => @since, :event_date.lte => @until}, :carrier_type_id => 3)
  end
  
  def digitised_all
    return @da+@dv+@dp
    #Carrier.count(:is_digitised => 1)
  end
  
  def registered_audio
    @ra = Carrier.count(:carrier_type_id => 1, :created_on.gte => @since, :created_on.lte => @until)
    return @ra
  end
  
  def registered_video
    @rv = Carrier.count(:carrier_type_id => 2, :created_on.gte => @since, :created_on.lte => @until)
    return @rv
  end
  
  def registered_paper
    @rp = Carrier.count(:carrier_type_id => 3, :created_on.gte => @since, :created_on.lte => @until)
    return @rp
  end
  
  def registered_film
    @rf = Carrier.count(:carrier_type_id => 4, :created_on.gte => @since, :created_on.lte => @until)
    return @rf
  end
  
  def registered_all
    return @ra+@rv+@rp+@rf
  end
  
  def archived(tenant=nil)
    unless tenant == nil
      @at=repository(:monitoring).adapter.select("SELECT COUNT(*) FROM (SELECT events.pid,events.key,events.date,pids.pid,pids.content_provider FROM events LEFT JOIN pids USING (pid)) AS temp WHERE key = 'ARCHIVED_ON_VAULT' AND content_provider = '#{tenant}' AND date >= '#{@since}' AND date <= '#{@until}';")[0]
    else
      @at=repository(:monitoring).adapter.select("SELECT COUNT(*) FROM (SELECT events.pid,events.key,events.date,pids.pid,pids.content_provider FROM events LEFT JOIN pids USING (pid)) AS temp WHERE key = 'ARCHIVED_ON_VAULT' AND date >= '#{@since}' AND date <= '#{@until}';")[0]
    end
    return @at
  end

  def archived_bytes(tenant=nil)
    unless tenant == nil
      @bytes=repository(:monitoring).adapter.select("SELECT SUM(carrier_size) FROM (SELECT events.pid,events.key,events.date,pids.pid,pids.content_provider,pids.carrier_size FROM events LEFT JOIN pids USING (pid)) AS temp WHERE key = 'ARCHIVED_ON_VAULT' AND content_provider = '#{tenant}' AND date >= '#{@since}' AND date <= '#{@until}';")[0].to_f
      #@bytes=Pid.sum(:carrier_size, :date.gte => @since, :date.lte => @until, :status => 'OK', :content_provider => tenant)
    else 
      @bytes=repository(:monitoring).adapter.select("SELECT SUM(carrier_size) FROM (SELECT events.pid,events.key,events.date,pids.pid,pids.content_provider,pids.carrier_size FROM events LEFT JOIN pids USING (pid)) AS temp WHERE key = 'ARCHIVED_ON_VAULT' AND date >= '#{@since}' AND date <= '#{@until}';")[0].to_f
      #@bytes=Pid.sum(:carrier_size, :date.gte => @since, :date.lte => @until, :status => 'OK')
      p @bytes
    end
    return @bytes
  end
  
  def ingested_all
    Pid.count(:date.gte => @since, :date.lte => @until)
  end
    
  def give(status)
    case status
    when "all"
      @stats = {:digitised => {:audio => self.digitised_audio, :video => self.digitised_video, :paper => self.digitised_paper, :all => self.digitised_all},
                  :registered => {:audio => self.registered_audio, :video => self.registered_video, :paper => self.registered_paper, :film => self.registered_film, :all => self.registered_all},
                  :archived => {:all => self.archived(@tenant), :bytes => self.archived_bytes(@tenant), :gigabytes => @bytes.to(:gb), :terabytes => @bytes.to(:tb), :petabytes => @bytes.to(:pb)},
                  :ingested => {:all => self.ingested_all}}
    when "archived"
      @stats = {:archived => {:all => self.archived(@tenant), :bytes => self.archived_bytes(@tenant), :gigabytes => @bytes.to(:gb), :terabytes => @bytes.to(:tb), :petabytes => @bytes.to(:pb)}}
    when "digitised"
      @stats = {:digitised => {:audio => self.digitised_audio, :video => self.digitised_video, :paper => self.digitised_paper, :all => self.digitised_all}}
    when "registered"
      @stats = {:registered => {:audio => self.registered_audio, :video => self.registered_video, :paper => self.registered_paper, :film => self.registered_film, :all => self.registered_all}}
    when "ingested"
      @stats = {:ingested => {:all => self.ingested_all}}
    else
      @stats = {:data => "0"}
    end
    return @stats
  end
  
end