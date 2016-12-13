#ifndef REPLICATOR_QUEUE_H
#define REPLICATOR_QUEUE_H

#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

template<typename T> class Queue
{
	public:
		Queue(const unsigned limit_) : limit(limit_) {}

		T pop()
		{
			std::unique_lock<std::mutex> lock(mutex);

			if (queue.empty()) {
				cv1.wait(lock, [this] { return !queue.empty(); });
			}

			T item = queue.front();
			queue.pop_front();
			lock.unlock();
			cv2.notify_all();

			return item;
		}

		void fetch(const std::function<bool (T&)>& cb, const std::chrono::milliseconds timeout, const unsigned limit_)
		{
			std::unique_lock<std::mutex> lock(mutex);

			if (!queue.empty() || cv1.wait_for(lock, timeout, [this] { return !queue.empty(); })) {
				for (unsigned cnt = queue.size() < limit_ ? queue.size() : limit_; cnt; cnt--) {
					cb(queue.front());
					queue.pop_front();
				}
			}

			lock.unlock();
			cv2.notify_all();
		}

		void push(const T& item)
		{
			std::unique_lock<std::mutex> lock(mutex);

			if (queue.size() >= limit) {
				cv2.wait(lock, [this] { return queue.size() < limit; });
			}

			queue.push_back(item);
			lock.unlock();
			cv1.notify_one();
		}

	private:
		std::deque<T> queue;
		std::mutex mutex;
		std::condition_variable cv1;
		std::condition_variable cv2;
		const unsigned limit;
};

#endif // REPLICATOR_QUEUE_H
