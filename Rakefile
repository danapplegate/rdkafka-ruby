# frozen_string_literal: true

# Rakefile

require 'bundler/gem_tasks'
require "./lib/rdkafka"

desc 'Generate some message traffic'
task :produce_messages, [:topic_name] do |t, args|
  config = {:"bootstrap.servers" => "localhost:9092"}
  if ENV["DEBUG"]
    config[:debug] = "broker,topic,msg"
  end
  producer = Rdkafka::Config.new(config).producer

  args.with_defaults(topic_name: 'rake_test_topic')

  delivery_handles = []
  100.times do |i|
    puts "Producing message #{i}"
    delivery_handles << producer.produce(
        topic:   args.topic_name,
        payload: "Payload #{i} from Rake",
        key:     "Key #{i} from Rake"
    )
  end
  puts 'Waiting for delivery'
  delivery_handles.each(&:wait)
  puts 'Done'
end

desc 'Consume some messages'
task :consume_messages do
  config = {
    :"bootstrap.servers" => "localhost:9092",
    :"group.id" => "rake_test",
    :"enable.partition.eof" => false,
    :"auto.offset.reset" => "earliest",
    :"statistics.interval.ms" => 10_000
  }
  if ENV["DEBUG"]
    config[:debug] = "cgrp,topic,fetch"
  end
  Rdkafka::Config.statistics_callback = lambda do |stats|
    puts stats
  end
  consumer = Rdkafka::Config.new(config).consumer
  consumer = Rdkafka::Config.new(config).consumer
  consumer.subscribe("rake_test_topic")
  consumer.each do |message|
    puts "Message received: #{message}"
  end
end

desc 'Long consumption of messages'
task :long_consume_messages do
  config = {
    :"bootstrap.servers" => "localhost:9092",
    :"group.id" => "commit_test",
    :"enable.partition.eof" => false,
    :"auto.offset.reset" => "earliest",
    :"statistics.interval.ms" => 5_000,

    # Try consuming messages either with enable.auto.offset.store true or false,
    # to show that auto commit uses the stored offset, which is updated _before_
    # the message is passed to the handler and processed.
    :"enable.auto.offset.store" => false,
    :"enable.auto.commit" => true,
    :"auto.commit.interval.ms" => 1_000
  }

  # Create a thread-safe way to track the consumer for stats thread
  require 'monitor'
  consumer_lock = Monitor.new
  consumer = nil

  # Start a separate thread to query and print offset statistics
  stats_thread = Thread.new do
    loop do
      consumer_lock.synchronize do
        if consumer
          begin
            watermarks = consumer.query_watermark_offsets("commit_test_topic", 0, 5000)
            tpl = Rdkafka::Consumer::TopicPartitionList.new
            tpl.add_topic("commit_test_topic", [0])
            committed = consumer.committed(tpl)
            position = consumer.position

            puts "[OFFSET CHECK] Low watermark: #{watermarks.first}, High watermark: #{watermarks.last}"
            if committed
              puts "[OFFSET CHECK] Committed offsets: #{committed}"
            end
            if position
              puts "[OFFSET CHECK] Current positions: #{position}"
            end
          rescue => e
            puts "[OFFSET CHECK] Error querying offsets: #{e.message}"
          end
        end
      end
      sleep 2  # Check every 2 seconds
    end
  end
  stats_thread.abort_on_exception = true

  # Keep the statistics callback for additional info
  # Rdkafka::Config.statistics_callback = lambda do |stats|
    # partition_stats = stats['topics']['commit_test_topic']['partitions']['0']
    # puts "[STATS] Committed offset: #{partition_stats['committed_offset']}, stored offset: #{partition_stats['stored_offset']}"
  # end

  # Initialize the consumer inside the lock
  consumer_lock.synchronize do
    consumer = Rdkafka::Config.new(config).consumer
    consumer.subscribe('commit_test_topic')
  end

  begin
    consumer.each do |message|
      puts "Processing message: #{message}"
      # Simulate long processing time
      sleep 12
      puts "Finished processing"
    end
  ensure
    stats_thread.kill  # Clean up the stats thread when we're done
    consumer.close
  end
end

desc 'Hammer down'
task :load_test do
  puts "Starting load test"

  config = Rdkafka::Config.new(
    :"bootstrap.servers" => "localhost:9092",
    :"group.id" => "load-test",
    :"enable.partition.eof" => false
  )

  # Create a producer in a thread
  Thread.new do
    producer = config.producer
    loop do
      handles = []
      1000.times do |i|
        handles.push(producer.produce(
          topic:   "load_test_topic",
          payload: "Payload #{i}",
          key:     "Key #{i}"
        ))
      end
      handles.each(&:wait)
      puts "Produced 1000 messages"
    end
  end.abort_on_exception = true

  # Create three consumers in threads
  3.times do |i|
    Thread.new do
      count = 0
      consumer = config.consumer
      consumer.subscribe("load_test_topic")
      consumer.each do |message|
        count += 1
        if count % 1000 == 0
          puts "Received 1000 messages in thread #{i}"
        end
      end
    end.abort_on_exception = true
  end

  loop do
    sleep 1
  end
end
