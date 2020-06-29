// Worker thread pool:
// 1. Each job has one stage that can run in parallel, followed by a serial stage to run
//    exclusively of other jobs' serial stages in the enqueued order. Put another way, the data
//    flows through a parallel scatter and serial gather.
// 2. The jobs have no return values, relying instead on side-effects. (Exception: for each job,
//    the parallel stage result passes a result to its serialized stage.)
// 3. The Enqueue operation blocks if the queue is "full"

#pragma once

#include <atomic>
#include <climits>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>

namespace SQLiteNested {

class ThreadPool {
    struct Job {
        // sequence number used to ensure serial stages run in the correct order
        unsigned long long seqno = ULLONG_MAX;
        void *x = nullptr;
        std::function<void *(void *) noexcept> par;
        std::function<void(void *) noexcept> ser;
    };

    // main thread state
    size_t max_threads_, max_jobs_;
    std::vector<std::thread> threads_;
    unsigned long long seqno_next_ = 0; // jobs enqueued so far / next seqno to be enqueued

    // shared state
    std::atomic<unsigned long long> seqno_done_; // highest seqno done (serial stage completed)
    std::mutex state_mutex_;
    std::condition_variable cv_enqueue_, cv_done_;
    std::queue<Job> par_queue_;
    bool shutdown_ = false;

    // serial-stage state: priority queue to order jobs by seqno
    std::mutex ser_mutex_;
    std::function<bool(const Job &, const Job &)> job_greater_ =
        [](const Job &lhs, const Job &rhs) { return lhs.seqno > rhs.seqno; };
    std::priority_queue<Job, std::vector<Job>, decltype(job_greater_)> ser_queue_;

    // worker thread
    void Worker() {
        std::unique_lock<std::mutex> lock(state_mutex_);
        while (true) {
            // wait for / dequeue a job
            while (!shutdown_ && par_queue_.empty()) {
                cv_enqueue_.wait(lock);
            }
            if (shutdown_) {
                break;
            }
            Job job = par_queue_.front();
            par_queue_.pop();
            lock.unlock();
            // run parallel stage
            if (job.par) {
                job.x = job.par(job.x);
            }
            {
                // enqueue serial stage
                std::lock_guard<std::mutex> ser_lock(ser_mutex_);
                ser_queue_.push(job);
                // run the enqueued serial stage(s) so long as the next one is in seqno order.
                // this may or may not include the one just enqueued.
                while (!ser_queue_.empty()) {
                    job = ser_queue_.top();
                    if (job.seqno != seqno_done_) {
                        break;
                    }
                    ser_queue_.pop();
                    if (job.ser) {
                        job.ser(job.x);
                    }
                    ++seqno_done_;
                }
            }
            lock.lock();
            cv_done_.notify_all();
        }
    }

  public:
    ThreadPool(size_t max_threads, size_t max_jobs)
        : max_threads_(max_threads), max_jobs_(max_jobs), ser_queue_(job_greater_) {
        seqno_done_ = 0;
    }
    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            shutdown_ = true;
        }
        cv_enqueue_.notify_all();
        for (auto &thread : threads_) {
            thread.join();
        }
    }

    // Enqueue ser(par(x)) for background processing as described. The functions must not raise.
    void Enqueue(void *x, std::function<void *(void *) noexcept> par,
                 std::function<void(void *) noexcept> ser) {
        std::unique_lock<std::mutex> lock(state_mutex_);
        while (seqno_next_ - seqno_done_ >= max_jobs_) {
            cv_done_.wait(lock);
        }
        Job job;
        job.seqno = seqno_next_++;
        job.x = x;
        job.par = par;
        job.ser = ser;
        par_queue_.push(job);
        if (threads_.size() < max_threads_ && threads_.size() < par_queue_.size()) {
            threads_.emplace_back([this]() { this->Worker(); });
        }
        lock.unlock();
        cv_enqueue_.notify_one();
    }

    // Await completion of all previously enqueued jobs
    void Barrier() {
        if (seqno_next_) {
            std::unique_lock<std::mutex> lock(state_mutex_);
            while (seqno_done_ < seqno_next_) {
                cv_done_.wait(lock);
            }
        }
    }
};

} // namespace SQLiteNested
