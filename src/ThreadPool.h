// Worker thread pool:
// 1. Each job has one stage that can run in parallel, followed by a serial stage to be run in the
//    enqueued order, exclusively of other jobs' serial stages. Put another way, the data flow
//    through a parallel scatter, then a serial gather.
// 2. The jobs have no return values, relying instead on side-effects. (Exception: for each job,
//    the parallel stage passes a void* to its serialized stage.)
// 3. The Enqueue operation blocks if the queue is "full"

#pragma once

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
    std::function<bool(const Job &, const Job &)> job_greater_ =
        [](const Job &lhs, const Job &rhs) { return lhs.seqno > rhs.seqno; };

    // main thread state
    size_t max_threads_, max_jobs_;
    std::vector<std::thread> threads_;
    unsigned long long seqno_next_ = 0; // jobs enqueued so far / next seqno to be enqueued

    // shared state
    std::mutex mutex_;
    std::condition_variable cv_enqueue_, cv_done_;
    std::queue<Job> par_queue_;
    std::priority_queue<Job, std::vector<Job>, decltype(job_greater_)> ser_queue_;
    unsigned long long seqno_done_ = 0; // highest seqno done (serial stage completed)
    bool shutdown_ = false;

    // worker thread
    void Worker() {
        std::unique_lock<std::mutex> lock(mutex_);
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
            // run parallel stage
            if (job.par) {
                lock.unlock();
                job.x = job.par(job.x);
                lock.lock();
            }
            // enqueue serial stage
            ser_queue_.push(job);
            // run the enqueued serial stage(s) so long as the next one is in seqno order.
            // this may or may not include the one just enqueued.
            while (!ser_queue_.empty() && ser_queue_.top().seqno == seqno_done_) {
                job = ser_queue_.top();
                ser_queue_.pop();
                if (job.ser) {
                    // run the next serial stage; we can release lock for this because the seqno
                    // check ensures only one can be dequeued at a time.
                    lock.unlock();
                    job.ser(job.x);
                    lock.lock();
                }
                ++seqno_done_;
            }
            cv_done_.notify_all();
        }
    }

  public:
    ThreadPool(size_t max_threads, size_t max_jobs)
        : max_threads_(max_threads), max_jobs_(max_jobs), ser_queue_(job_greater_) {}
    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_ = true;
            cv_enqueue_.notify_all();
        }
        for (auto &thread : threads_) {
            thread.join();
        }
    }

    // Enqueue ser(par(x)) for background processing as described. The functions must not throw.
    void Enqueue(void *x, std::function<void *(void *) noexcept> par,
                 std::function<void(void *) noexcept> ser) {
        if (seqno_next_ == ULLONG_MAX) { // pedantic
            Barrier();
            seqno_next_ = 0;
        }

        Job job;
        job.x = x;
        job.par = par;
        job.ser = ser;

        std::unique_lock<std::mutex> lock(mutex_);
        while (seqno_next_ - seqno_done_ >= max_jobs_) {
            cv_done_.wait(lock);
        }
        job.seqno = seqno_next_++;
        par_queue_.push(job);
        lock.unlock();
        cv_enqueue_.notify_one();
        if (threads_.size() < max_threads_ && threads_.size() < par_queue_.size()) {
            threads_.emplace_back([this]() { this->Worker(); });
        }
    }

    // Await completion of all previously enqueued jobs
    void Barrier() {
        if (seqno_next_) {
            std::unique_lock<std::mutex> lock(mutex_);
            while (seqno_done_ < seqno_next_) {
                cv_done_.wait(lock);
            }
        }
    }
};

} // namespace SQLiteNested
