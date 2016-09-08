# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"

class LogStash::Inputs::Rackspace < LogStash::Inputs::Base

  config_name "rackspace"

  # Rackspace Cloud Username
  config :username, :validate => :string, :required => true
  
  # Rackspace Cloud API Key
  config :api_key, :validate => :string, :required => true
   
  # Rackspace region
  # `ord, dfw, lon, syd,` etc
  config :region, :validate => :string, :default => 'dfw'

  # Rackspace Queue Name
  config :queue,  :validate => :string, :default => 'logstash'

  # number of messages to claim
  # Min: 1, Max: 10
  config :claim, :validate => :number, :default => 1
  
  # length of time to hold claim
  # Min: 60
  config :ttl, :validate => :number, :default => 60
  
  # Rackspace Service URL. Defaults to Region Public is not specifed
  config :queue_url, :validate => :string, :required => false

  public
  def register
    require "fog"
	
	if @queue_url
		@service = Fog::Rackspace::Queues.new(
		:rackspace_username  => @username,   # Your Rackspace Username
		:rackspace_api_key   => @api_key,         # Your Rackspace API key
		:rackspace_region    => @region.to_sym,   # Your desired region
		:rackspace_queues_url => @queue_url,      #Queue URL Override
		:connection_options  => {}                #Optional connection options
    )
	else
		@service = Fog::Rackspace::Queues.new(
		:rackspace_username  => @username,   # Your Rackspace Username
		:rackspace_api_key   => @api_key,         # Your Rackspace API key
		:rackspace_region    => @region.to_sym,                  # Your desired region
		:connection_options  => {}                     # Optional connection options
    )
	end
	
    begin
      @rackspace_queue = @service.queues.create :name => @queue
    rescue Fog::Rackspace::Queues::ServiceError => e
      if e.status_code == 204
        @logger.warn("Queue #{@queue} already exists")
      else
        @logger.warn("something bad happened!")
      end # rescue
    end # begin
    @service.queues.each_with_index do |queue, index|
      if queue.name == @queue
        @rackspace_queue = @service.queues[index]
        break
      end
    end
    @logger.info("Opened connection to rackspace cloud queues")
  end # def register

  private
  def queue_event(msg, output_queue)
    begin
      @codec.decode(msg.body.to_s) do |event|
        decorate(event)
        output_queue << event
      end
      msg.destroy
    rescue => e # parse or event creation error
      @logger.error("Failed to create event", :message => msg, :exception => e,
                    :backtrace => e.backtrace);
    end
  end

  public
  def run(output_queue)
    while !stop?
      claim = @rackspace_queue.claims.create :ttl => @ttl, :grace => 100, :limit => @claim
      if claim
        claim.messages.each do |message|
          queue_event message, output_queue
        end
      end # unless
    end # while !stop
  end # def run

  public
  def close
    @service = nil
  end # def close

end # class LogStash::Inputs::Rackspace
