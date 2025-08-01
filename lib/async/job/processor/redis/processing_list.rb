# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2024-2025, by Samuel Williams.

module Async
	module Job
		module Processor
			module Redis
				# Manages jobs currently being processed and handles abandoned job recovery.
				# Maintains heartbeats for active workers and automatically requeues
				# jobs from workers that have stopped responding.
				class ProcessingList
					REQUEUE = <<~LUA
						local cursor = "0"
						local count = 0
						
						repeat
							-- Scan through all known server id -> job id mappings and requeue any jobs that have been abandoned:
							local result = redis.call('SCAN', cursor, 'MATCH', KEYS[1]..':*:pending')
							cursor = result[1]
							for _, pending_key in pairs(result[2]) do
								-- Check if the server is still active:
								local server_key = KEYS[1]..":"..pending_key:match("([^:]+):pending")
								local state = redis.call('GET', server_key)
								if state == false then
									while true do
										-- Requeue any pending jobs:
										local result = redis.call('RPOPLPUSH', pending_key, KEYS[2])
										
										if result == false then
											-- Delete the pending list:
											redis.call('DEL', pending_key)
											break
										end
										
										count = count + 1
									end
								end
							end
						until cursor == "0"
						
						return count
					LUA
					
					RETRY = <<~LUA
						redis.call('LREM', KEYS[1], 1, ARGV[1])
						redis.call('LPUSH', KEYS[2], ARGV[1])
					LUA
					
					COMPLETE = <<~LUA
						redis.call('LREM', KEYS[1], 1, ARGV[1])
						redis.call('HDEL', KEYS[2], ARGV[1])
					LUA
					
					# Initialize a new processing list manager.
					# @parameter client [Async::Redis::Client] The Redis client instance.
					# @parameter key [String] The base Redis key for processing data.
					# @parameter id [String] The unique server/worker ID.
					# @parameter ready_list [ReadyList] The ready job queue.
					# @parameter job_store [JobStore] The job data store.
					def initialize(client, key, id, ready_list, job_store)
						@client = client
						@key = key
						@id = id
						
						@ready_list = ready_list
						@job_store = job_store
						
						@pending_key = "#{@key}:#{@id}:pending"
						@heartbeat_key = "#{@key}:#{@id}"
						
						@requeue = @client.script(:load, REQUEUE)
						@retry = @client.script(:load, RETRY)
						@complete = @client.script(:load, COMPLETE)
						
						@complete_count = 0
					end
					
					# @attribute [String] The base Redis key for this processing list.
					attr :key
					
					# @attribute [String] The Redis key for this worker's heartbeat.
					attr :heartbeat_key
					
					# @attribute [Integer] The total count of all jobs completed by this worker.
					attr :complete_count
					
					# @returns [Integer] The number of jobs currently being processed by this worker.
					def size
						@client.llen(@pending_key)
					end
					
					# Fetch the next job from the ready queue, moving it to this worker's pending list.
					# This is a blocking operation that waits until a job is available.
					# @returns [String, nil] The job ID, or nil if no job is available.
					def fetch
						@client.brpoplpush(@ready_list.key, @pending_key, 0)
					end
					
					# Mark a job as completed, removing it from the pending list and job store.
					# @parameter id [String] The job ID to complete.
					def complete(id)
						@complete_count += 1
						
						@client.evalsha(@complete, 2, @pending_key, @job_store.key, id)
					end
					
					# Retry a failed job by moving it back to the ready queue.
					# @parameter id [String] The job ID to retry.
					def retry(id)
						Console.warn(self, "Retrying job: #{id}")
						@client.evalsha(@retry, 2, @pending_key, @ready_list.key, id)
					end
					
					# Update heartbeat and requeue any abandoned jobs from inactive workers.
					# @parameter start_time [Float] The start time for calculating uptime.
					# @parameter delay [Numeric] The heartbeat update interval.
					# @parameter factor [Numeric] The heartbeat expiration factor.
					# @returns [Integer] The number of jobs requeued from abandoned workers.
					def requeue(start_time, delay, factor)
						uptime = (Time.now.to_f - start_time).round(2)
						expiry = (delay*factor).ceil
						@client.set(@heartbeat_key, JSON.dump(uptime: uptime), seconds: expiry)
						
						# Requeue any jobs that have been abandoned:
						count = @client.evalsha(@requeue, 2, @key, @ready_list.key)
						
						return count
					end
					
					# Start the background heartbeat and abandoned job recovery task.
					# @parameter delay [Integer] The heartbeat update interval in seconds.
					# @parameter factor [Integer] The heartbeat expiration factor.
					# @parameter parent [Async::Task] The parent task to run the background loop in.
					# @returns [Async::Task] The background processing task.
					def start(delay: 5, factor: 2, parent: Async::Task.current)
						start_time = Time.now.to_f
						
						parent.async do |task|
							while true
								task.defer_stop do
									count = self.requeue(start_time, delay, factor)
									
									if count > 0
										Console.warn(self, "Requeued #{count} abandoned jobs.")
									end
								end
								
								sleep(delay)
							end
						end
					end
				end
			end
		end
	end
end
