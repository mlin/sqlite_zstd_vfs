// Worker thread pool:
//  (i) Each job has one parallel and one serialized stage. The serialized stages run on a worker
//      thread in the order of the jobs' original queueing.
//  (ii) no return values, only side-effects (except: result of parallel stage is passed to serial
//       stage)
//  (iii) enqueue operation blocks if queue is 'full'

#include <atomic>
#include <climits>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>

class ThreadPool {
    struct Job {
        unsigned long long seqno = ULLONG_MAX;
        void *x = nullptr;
        std::function<void *(void *)> par;
        std::function<void(void *)> ser;
    };

    size_t max_threads_, max_jobs_;
    std::vector<std::thread> threads_;
    unsigned long long seqno_next_ = 0;
    std::atomic<unsigned long long> seqno_done_;

    std::mutex state_mutex_;
    std::condition_variable cv_enqueue_, cv_done_;
    std::queue<Job> par_queue_;
    bool shutdown_ = false;

    std::mutex ser_mutex_;
    std::function<bool(const Job &, const Job &)> job_greater = [](const Job &lhs, const Job &rhs) {
        return lhs.seqno > rhs.seqno;
    };
    std::priority_queue<Job, std::vector<Job>, decltype(job_greater)> ser_queue_;

    void Worker() {
        std::unique_lock<std::mutex> lock(state_mutex_);
        while (true) {
            // wait for job
            while (!shutdown_ && par_queue_.empty()) {
                cv_enqueue_.wait(lock);
            }
            if (shutdown_) {
                break;
            }
            Job job = par_queue_.front();
            par_queue_.pop();
            lock.release();
            // run parallel stage
            if (job.par) {
                job.x = job.par(job.x);
            }
            {
                std::lock_guard<std::mutex> ser_lock(ser_mutex_);
                // enqueue serial stage
                ser_queue_.push(job);
                // run the available, contiguous sequence of serial stage(s), if any; possibly but
                // not necessarily including that of the job whose parallel stage we just ran
                while (!ser_queue_.empty()) {
                    job = ser_queue_.top();
                    if (job.seqno != seqno_done_) {
                        break;
                    }
                    if (job.ser) {
                        job.ser(job.x);
                    }
                    ser_queue_.pop();
                    ++seqno_done_;
                }
            }
            lock.lock();
            cv_done_.notify_all();
        }
    }

  public:
    ThreadPool(size_t max_threads, size_t max_jobs)
        : max_threads_(max_threads), max_jobs_(max_jobs) {
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

    void Enqueue(void *x, std::function<void *(void *)> par, std::function<void(void *)> ser) {
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

    void Drain() {
        if (seqno_next_) {
            std::unique_lock<std::mutex> lock(state_mutex_);
            while (seqno_done_ < seqno_next_) {
                cv_done_.wait(lock);
            }
        }
    }
};
