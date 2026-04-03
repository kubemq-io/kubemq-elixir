unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Work queue pattern — distribute tasks across competing workers using queues

channel = "patterns.work-queue"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-work-queue")

# Enqueue work items
IO.puts("Enqueueing 10 tasks...")

for i <- 1..10 do
  {:ok, _} = KubeMQ.Client.send_queue_message(client,
    KubeMQ.QueueMessage.new(
      channel: channel,
      body: ~s({"task_id": #{i}, "type": "process_image", "file": "img_#{i}.jpg"}),
      tags: %{"priority" => if(rem(i, 3) == 0, do: "high", else: "normal")}
    ))
end

IO.puts("All tasks enqueued")

# Simulate 3 workers pulling from the same queue
workers =
  for worker_id <- 1..3 do
    Task.async(fn ->
      processed = 0

      case KubeMQ.Client.poll_queue(client,
             channel: channel,
             max_items: 4,
             wait_timeout: 3_000
           ) do
        {:ok, poll} when length(poll.messages) > 0 ->
          Enum.each(poll.messages, fn msg ->
            IO.puts("[Worker-#{worker_id}] Processing: #{msg.body}")
            Process.sleep(100)
          end)

          {:ok, _} = KubeMQ.PollResponse.ack_all(poll)
          length(poll.messages)

        {:ok, _} ->
          0

        {:error, _} ->
          0
      end
    end)
  end

# Wait for all workers
results = Task.await_many(workers, 15_000)
total = Enum.sum(results)
IO.puts("\nTotal processed: #{total} tasks across #{length(workers)} workers")

KubeMQ.Client.close(client)
