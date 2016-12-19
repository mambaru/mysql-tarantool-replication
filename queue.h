#ifndef REPLICATOR_QUEUE_H
#define REPLICATOR_QUEUE_H

#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

template<typename T> class Queue
{
	private:
		template<typename M> class ulock : public std::unique_lock<M> {
			std::condition_variable& cv;
			public:
			ulock(M& m, std::condition_variable& cv_) : std::unique_lock<M>(m), cv(cv_) {}
			void unlock() { std::unique_lock<M>::unlock(); cv.notify_all(); }
		};

	public:
		Queue(const unsigned limit_) : limit(limit_) {}

		inline T pop()
		{
			// std::unique_lock<std::mutex> lock_(mutex);
			ulock<std::mutex> lock_(mutex, cv2);

			if (queue.empty()) {
				cv1.wait(lock_, [this] { return !queue.empty(); });
			}

			T item = queue.front();
			queue.pop_front();
			// lock_.unlock();
			// cv2.notify_all();

			return item;
		}

		inline void try_fetch(const std::function<void (T&)>& cb, const std::chrono::milliseconds timeout)
		{
			// std::unique_lock<std::mutex> lock_(mutex);
			ulock<std::mutex> lock_(mutex, cv2);

			if (queue.empty() && !cv1.wait_for(lock_, timeout, [this] { return !queue.empty(); })) {
				return;
			}

			unsigned cnt = queue.size();
			do {
				T item = queue.front();
				queue.pop_front();
				lock_.unlock();
				//cv2.notify_all();

				cb(item);

				if (--cnt) {
					lock_.lock();
					continue;
				}
			} while (false);
		}

		inline void push(const T& item)
		{
			// std::unique_lock<std::mutex> lock_(mutex);
			ulock<std::mutex> lock_(mutex, cv1);

			if (queue.size() >= limit) {
				cv2.wait(lock_, [this] { return queue.size() < limit; });
			}

			queue.push_back(item);
			// lock_.unlock();
			// cv1.notify_one();
		}

		inline unsigned size() const {
			// std::lock_guard<std::mutex> lock_(mutex);
			return queue.size();
		}

	private:
		std::deque<T> queue;
		mutable std::mutex mutex;
		std::condition_variable cv1;
		std::condition_variable cv2;
		const unsigned limit;
};

#endif // REPLICATOR_QUEUE_H
