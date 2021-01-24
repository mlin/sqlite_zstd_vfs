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
#include <readerwriterqueue.h>
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
            auto seqno0 = seqno_done_;
            for (; !ser_queue_.empty() && ser_queue_.top().seqno == seqno_done_; ++seqno_done_) {
                job = ser_queue_.top();
                ser_queue_.pop();
                if (job.ser) {
                    // run the next serial stage; we can release lock for this because the seqno
                    // check ensures only one can be dequeued at a time.
                    lock.unlock();
                    if (seqno_done_ > seqno0) {
                        cv_done_.notify_all();
                    }
                    job.ser(job.x);
                    lock.lock();
                }
            }
            if (seqno_done_ > seqno0) {
                cv_done_.notify_all();
            }
        }
    }

  public:
    ThreadPool(size_t max_threads, size_t max_jobs)
        : max_threads_(max_threads), max_jobs_(max_jobs), ser_queue_(job_greater_) {}
    virtual ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_ = true;
            cv_enqueue_.notify_all();
        }
        for (auto &thread : threads_) {
            thread.join();
        }
    }

    size_t MaxThreads() const noexcept { return max_threads_; }

    size_t MaxJobs() const noexcept { return max_jobs_; }

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

// Adds lock-free EnqueueFast() for use on critical paths.
// - EnqueueFast() never blocks (!)
// - Only one thread should ever use it
class ThreadPoolWithEnqueueFast : public ThreadPool {
    // concept: foreground thread adds job onto a lock-free queue, which a single background thread
    // consumes to Enqueue()

    struct EnqueueFastJob {
        bool shutdown = false;
        void *x = nullptr;
        std::function<void *(void *) noexcept> par;
        std::function<void(void *) noexcept> ser;
    };

    moodycamel::BlockingReaderWriterQueue<EnqueueFastJob> fast_queue_;
    std::unique_ptr<std::thread> worker_thread_;

    void EnqueueFastWorker() {
        EnqueueFastJob job;
        while (true) {
            fast_queue_.wait_dequeue(job);
            if (job.shutdown) {
                break;
            }
            this->Enqueue(job.x, job.par, job.ser);
        }
    }

  public:
    ThreadPoolWithEnqueueFast(size_t max_threads, size_t max_jobs)
        : ThreadPool(max_threads, max_jobs), fast_queue_(max_jobs) {}

    ~ThreadPoolWithEnqueueFast() {
        if (worker_thread_) {
            EnqueueFastJob job;
            job.shutdown = true;
            fast_queue_.enqueue(job);
            worker_thread_->join();
        }
    }

    void EnqueueFast(void *x, std::function<void *(void *) noexcept> par,
                     std::function<void(void *) noexcept> ser) {
        EnqueueFastJob job;
        job.x = x;
        job.par = par;
        job.ser = ser;
        fast_queue_.enqueue(job);
        if (!worker_thread_) {
            worker_thread_.reset(new std::thread([this]() { this->EnqueueFastWorker(); }));
        }
    }
};

} // namespace SQLiteNested
