# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2025, by Samuel Williams.

require "async"
require "async/redis"

require "sus/fixtures/async/reactor_context"
require "sus/fixtures/console"

require "async/job/processor/redis/delayed_jobs"
require "async/job/processor/redis/ready_list"
require "async/job/processor/redis/job_store"

describe Async::Job::Processor::Redis::DelayedJobs do
	include Sus::Fixtures::Async::ReactorContext
	include Sus::Fixtures::Console::CapturedLogger
	
	let(:client) {Async::Redis::Client.new}
	let(:prefix) {"test-delayed-#{SecureRandom.hex(8)}"}
	let(:delayed_jobs) {subject.new(client, "#{prefix}:delayed")}
	let(:job_store) {Async::Job::Processor::Redis::JobStore.new(client, "#{prefix}:jobs")}
	let(:ready_list) {Async::Job::Processor::Redis::ReadyList.new(client, "#{prefix}:ready")}
	
	let(:test_job) {JSON.dump({"data" => "test delayed job"})}
	
	with "#add" do
		it "can add a job with a timestamp" do
			future_time = Time.now + 60  # 1 minute from now
			
			job_id = delayed_jobs.add(test_job, future_time, job_store)
			
			expect(job_id).to be_a(String)
			expect(job_id).not.to be(:empty?)
			
			# Verify the job was stored in the job store
			stored_job = client.hget(job_store.key, job_id)
			expect(stored_job).to be == test_job
			
			# Verify the job was added to the delayed queue with correct timestamp
			score = client.zscore(delayed_jobs.key, job_id)
			expect(score.to_f).to be == future_time.to_f
		end
	end
	
	with "#move" do
		it "can move ready jobs from delayed queue to ready list" do
			# Add a job that's ready to be processed (past timestamp)
			past_time = Time.now - 60  # 1 minute ago
			job_id = delayed_jobs.add(test_job, past_time, job_store)
			
			# Move jobs that are ready
			count = delayed_jobs.move(destination: ready_list.key)
			
			expect(count).to be == 1
			
			# Verify the job was moved to the ready list
			ready_job_id = client.lpop(ready_list.key)
			expect(ready_job_id).to be == job_id
			
			# Verify the job was removed from the delayed queue
			remaining_score = client.zscore(delayed_jobs.key, job_id)
			expect(remaining_score).to be_nil
		end
		
		it "does not move jobs that aren't ready yet" do
			# Add a job scheduled for the future
			future_time = Time.now + 60  # 1 minute from now
			job_id = delayed_jobs.add(test_job, future_time, job_store)
			
			# Try to move jobs
			count = delayed_jobs.move(destination: ready_list.key)
			
			expect(count).to be == 0
			
			# Verify the job is still in the delayed queue
			score = client.zscore(delayed_jobs.key, job_id)
			expect(score.to_f).to be == future_time.to_f
			
			# Verify no jobs were added to the ready list
			ready_job_id = client.lpop(ready_list.key)
			expect(ready_job_id).to be_nil
		end
		
		it "can move multiple ready jobs at once" do
			past_time = Time.now - 60
			
			# Add multiple jobs that are ready
			job_id_1 = delayed_jobs.add(JSON.dump({"data" => "job 1"}), past_time, job_store)
			job_id_2 = delayed_jobs.add(JSON.dump({"data" => "job 2"}), past_time, job_store)
			job_id_3 = delayed_jobs.add(JSON.dump({"data" => "job 3"}), past_time, job_store)
			
			count = delayed_jobs.move(destination: ready_list.key)
			
			expect(count).to be == 3
			
			# Verify all jobs were moved to ready list
			ready_jobs = []
			3.times do
				job_id = client.lpop(ready_list.key)
				ready_jobs << job_id if job_id
			end
			
			expect(ready_jobs).to have_attributes(size: be == 3)
			expect(ready_jobs).to be(:include?, job_id_1)
			expect(ready_jobs).to be(:include?, job_id_2)
			expect(ready_jobs).to be(:include?, job_id_3)
		end
	end
	
	with "#start" do
		it "can start the background processing task" do
			# Add a job that will become ready during the test
			near_future = Time.now + 0.5  # Half second from now
			job_id = delayed_jobs.add(test_job, near_future, job_store)
			
			# Start the delayed job processor with high resolution for fast testing
			task = delayed_jobs.start(ready_list, resolution: 0.1)
			
			# Wait for the job to become ready and be processed
			Async::Task.current.with_timeout(2.0) do
				loop do
					ready_job_id = client.lpop(ready_list.key)
					if ready_job_id == job_id
						break
					elsif ready_job_id
						# Put it back if it's a different job
						client.lpush(ready_list.key, ready_job_id)
					end
					Async::Task.current.sleep(0.05)
				end
			end
			
			# Stop the background task
			task.stop
			
			# Verify the job was moved (it should no longer be in delayed queue)
			remaining_score = client.zscore(delayed_jobs.key, job_id)
			expect(remaining_score).to be_nil
		end
		
		it "logs debug messages when moving jobs" do
			# Add a ready job
			past_time = Time.now - 60
			job_id = delayed_jobs.add(test_job, past_time, job_store)
			
			# Start the processor briefly to capture console output
			task = delayed_jobs.start(ready_list, resolution: 0.1)
			
			# Wait for one processing cycle
			Async::Task.current.sleep(0.2)
			
			# Stop the task
			task.stop
			
			# Check for debug log message
			expect_console.to have_logged(severity: be(:==, :debug), message: be(:include?, "Moved 1 delayed jobs to ready list"))
		end
	end
end
