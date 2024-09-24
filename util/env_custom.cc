
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <vector>
#include <iostream>

#include "leveldb/env.h"
#include "leveldb/custom_env.h"
#include "port/port.h"
#include "util/custom_logger.h"

namespace leveldb {

namespace {

constexpr const size_t kWritableFileBufferSize = 65536;

#define __CUSTOM_ERROR__(ctx, error_code) (std::cout << "Error at line: " << __LINE__ << "\n", CustomError(ctx, error_code))

Status CustomError(const std::string& context, int error_code) {
  if (error_code == 7)
    return Status::NotFound(context, "FILE NOT FOUND");
  else
    return Status::IOError(context, "FAILED WITH CODE: " + std::to_string(error_code));
}

class CustomSequentialFile : public SequentialFile {
 public:
  CustomSequentialFile(std::string filename, file_ref handle)
      : handle_(std::move(handle)), filename_(std::move(filename)) {}
  ~CustomSequentialFile() override { closeFile(handle_); }

  Status Read(size_t n, Slice* result, char* scratch) override {
    size_t bytes_read;
    int err;
    if (err = readFile(handle_, scratch, n, &bytes_read)) {
      return __CUSTOM_ERROR__(filename_, err);
    }

    *result = Slice(scratch, bytes_read);
    return Status::OK();
  }

  Status Skip(uint64_t n) override {
    int err;
    if (err = setFilePointer(handle_, n, NULL, CURRENT)) {
      return __CUSTOM_ERROR__(filename_, err);
    }
    return Status::OK();
  }

 private:
  const file_ref handle_;
  const std::string filename_;
};

class CustomRandomAccessFile : public RandomAccessFile {
 public:
  CustomRandomAccessFile(std::string filename, file_ref handle)
      : handle_(std::move(handle)), filename_(std::move(filename)) {}

  ~CustomRandomAccessFile() override { closeFile(handle_); };

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    size_t bytes_read;
    int err;
    if (err = setFilePointer(handle_, offset, NULL, START)) {
      return __CUSTOM_ERROR__(filename_, err);
    }
    if (err = readFile(handle_, scratch, n, &bytes_read)) {
      return __CUSTOM_ERROR__(filename_, err);
    }

    *result = Slice(scratch, bytes_read);
    return Status::OK();
  }

 private:
  const file_ref handle_;
  const std::string filename_;
};

class CustomWritableFile : public WritableFile {
 public:
  CustomWritableFile(std::string filename, file_ref handle)
      : pos_(0), handle_(std::move(handle)), filename_(std::move(filename)) {}

  ~CustomWritableFile() override { closeFile(handle_); }

  Status Append(const Slice& data) override {
    size_t write_size = data.size();
    const char* write_data = data.data();

    // Fit as much as possible into buffer.
    size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
    std::memcpy(buf_ + pos_, write_data, copy_size);
    write_data += copy_size;
    write_size -= copy_size;
    pos_ += copy_size;
    if (write_size == 0) {
      return Status::OK();
    }

    // Can't fit in buffer, so need to do at least one write.
    Status status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    // Small writes go to buffer, large writes are written directly.
    if (write_size < kWritableFileBufferSize) {
      std::memcpy(buf_, write_data, write_size);
      pos_ = write_size;
      return Status::OK();
    }
    return WriteUnbuffered(write_data, write_size);
  }

  Status Close() override {
    Status status = FlushBuffer();
    int err;
    if ((err = closeFile(handle_)) && status.ok()) {
      status = __CUSTOM_ERROR__(filename_, err);
    }
    return status;
  }

  Status Flush() override { return FlushBuffer(); }

  Status Sync() override {
    return FlushBuffer();
  }

 private:
  Status FlushBuffer() {
    Status status = WriteUnbuffered(buf_, pos_);
    pos_ = 0;
    return status;
  }

  Status WriteUnbuffered(const char* data, size_t size) {
    size_t bytes_written;
    int err;
    if (err = writeFile(handle_, data, size, &bytes_written)) {
      return __CUSTOM_ERROR__(filename_, err);
    }
    return Status::OK();
  }

  // buf_[0, pos_-1] contains data to be written to handle_.
  char buf_[kWritableFileBufferSize];
  size_t pos_;

  file_ref handle_;
  const std::string filename_;
};

class CustomFileLock : public FileLock {
 public:
  CustomFileLock(file_ref handle, std::string filename)
      : handle_(std::move(handle)), filename_(std::move(filename)) {}

  const file_ref& handle() const { return handle_; }
  const std::string& filename() const { return filename_; }

 private:
  const file_ref handle_;
  const std::string filename_;
};

class CustomEnv : public Env {
 public:
  CustomEnv();
  ~CustomEnv() override {
    static const char msg[] =
        "CustomEnv singleton destroyed. Unsupported behavior!\n";
    std::fwrite(msg, 1, sizeof(msg), stderr);
    std::abort();
  }

  Status NewSequentialFile(const std::string& filename,
                           SequentialFile** result) override {
    *result = nullptr;
    file_ref handle = 0;
    int error = openFile(filename, READ, &handle);
    if (error) {
      return __CUSTOM_ERROR__(filename, error);
    }

    *result = new CustomSequentialFile(filename, handle);
    return Status::OK();
  }

  Status NewRandomAccessFile(const std::string& filename,
                             RandomAccessFile** result) override {
    *result = nullptr;
    file_ref handle = 0;
    int error = openFile(filename, READ, &handle);
    if (error) {
      return __CUSTOM_ERROR__(filename, error);
    }

    *result = new CustomRandomAccessFile(filename, handle);
    return Status::OK();
  }

  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    *result = nullptr;
    file_ref handle = 0;
    int error = openFile(filename, TRUNC | WRITE | CREATE, &handle);
    if (error) {
      return __CUSTOM_ERROR__(filename, error);
    }

    *result = new CustomWritableFile(filename, handle);
    return Status::OK();
  }

  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    *result = nullptr;
    file_ref handle = 0;
    int error = openFile(filename, APPEND | WRITE | CREATE, &handle);
    if (error) {
      return __CUSTOM_ERROR__(filename, error);
    }

    *result = new CustomWritableFile(filename, handle);
    return Status::OK();
  }

  bool FileExists(const std::string& filename) override {
    return fileExists(filename);
  }

  Status GetChildren(const std::string& directory_path,
                     std::vector<std::string>* result) override {
    int err;
    if (err = listDirChildren(directory_path, result)) {
      return __CUSTOM_ERROR__(directory_path, err);
    }
    return Status::OK();
  }

  Status RemoveFile(const std::string& filename) override {
    int err;
    if (err = deleteFile(filename)) {
      return __CUSTOM_ERROR__(filename, err);
    }
    return Status::OK();
  }

  Status CreateDir(const std::string& dirname) override {
    int err;
    if (err = createDir(dirname)) {
      return __CUSTOM_ERROR__(dirname, err);
    }
    return Status::OK();
  }

  Status RemoveDir(const std::string& dirname) override {
    int err;
    if (err = deleteDir(dirname)) {
      return __CUSTOM_ERROR__(dirname, err);
    }
    return Status::OK();
  }

  Status GetFileSize(const std::string& filename, uint64_t* size) override {
    int err;
    if (err = getFileSize(filename, size)) {
      return __CUSTOM_ERROR__(filename, err);
    }
    return Status::OK();
  }

  Status RenameFile(const std::string& from, const std::string& to) override {
    int err;
    if (err = renameFile(from, to)) {
      return __CUSTOM_ERROR__(from, err);
    }
    return Status::OK();
  }

  Status LockFile(const std::string& filename, FileLock** lock) override {
    *lock = nullptr;
    Status result;
    file_ref handle;
    int err = openFile(filename, READ_WRITE | CREATE, &handle);
    if (handle == NULL) {
      result = __CUSTOM_ERROR__(filename, err);
    } else {
      *lock = new CustomFileLock(std::move(handle), filename);
    }
    return result;
  }

  Status UnlockFile(FileLock* lock) override {
    CustomFileLock* windows_file_lock =
        reinterpret_cast<CustomFileLock*>(lock);
    if (int err = closeFile(windows_file_lock->handle())) {
      return __CUSTOM_ERROR__("unlock " + windows_file_lock->filename(), err);
    }
    delete windows_file_lock;
    return Status::OK();
  }

  void Schedule(void (*background_work_function)(void* background_work_arg),
                void* background_work_arg) override;

  void StartThread(void (*thread_main)(void* thread_main_arg),
                   void* thread_main_arg) override {
    std::thread new_thread(thread_main, thread_main_arg);
    new_thread.detach();
  }

  Status GetTestDirectory(std::string* result) override {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
      return Status::OK();
    }

    std::string tmp_path;
    if (int err = getTempDir(&tmp_path)) {
      return __CUSTOM_ERROR__("GetTempPath", err);
    }
    std::stringstream ss;
    ss << tmp_path << "leveldbtest-" << std::this_thread::get_id();
    *result = ss.str();

    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  Status NewLogger(const std::string& filename, Logger** result) override {
    file_ref handler;
    if (int err = openFile(filename, WRITE | CREATE | TRUNC, &handler)) {
      *result = nullptr;
      return CustomError(filename, err);
    } else {
      *result = new CustomLogger(handler);
      return Status::OK();
    }
  }

  uint64_t NowMicros() override {
    return getTimeMillis();
  }

  void SleepForMicroseconds(int micros) override {
    std::this_thread::sleep_for(std::chrono::microseconds(micros));
  }

 private:
  void BackgroundThreadMain();

  static void BackgroundThreadEntryPoint(CustomEnv* env) {
    env->BackgroundThreadMain();
  }

  // Stores the work item data in a Schedule() call.
  //
  // Instances are constructed on the thread calling Schedule() and used on the
  // background thread.
  //
  // This structure is thread-safe beacuse it is immutable.
  struct BackgroundWorkItem {
    explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
        : function(function), arg(arg) {}

    void (*const function)(void*);
    void* const arg;
  };

  port::Mutex background_work_mutex_;
  port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);
  bool started_background_thread_ GUARDED_BY(background_work_mutex_);

  std::queue<BackgroundWorkItem> background_work_queue_
      GUARDED_BY(background_work_mutex_);

  // Limiter mmap_limiter_;  // Thread-safe.
};

CustomEnv::CustomEnv()
    : background_work_cv_(&background_work_mutex_),
      started_background_thread_(false) {}
    //  mmap_limiter_(MaxMmaps()) {}

void CustomEnv::Schedule(
    void (*background_work_function)(void* background_work_arg),
    void* background_work_arg) {
  background_work_mutex_.Lock();

  // Start the background thread, if we haven't done so already.
  if (!started_background_thread_) {
    started_background_thread_ = true;
    std::thread background_thread(CustomEnv::BackgroundThreadEntryPoint, this);
    background_thread.detach();
  }

  // If the queue is empty, the background thread may be waiting for work.
  if (background_work_queue_.empty()) {
    background_work_cv_.Signal();
  }

  background_work_queue_.emplace(background_work_function, background_work_arg);
  background_work_mutex_.Unlock();
}

void CustomEnv::BackgroundThreadMain() {
  while (true) {
    background_work_mutex_.Lock();

    // Wait until there is work to be done.
    while (background_work_queue_.empty()) {
      background_work_cv_.Wait();
    }

    assert(!background_work_queue_.empty());
    auto background_work_function = background_work_queue_.front().function;
    void* background_work_arg = background_work_queue_.front().arg;
    background_work_queue_.pop();

    background_work_mutex_.Unlock();
    background_work_function(background_work_arg);
  }
}

template <typename EnvType>
class SingletonEnv {
 public:
  SingletonEnv() {
#if !defined(NDEBUG)
    env_initialized_.store(true, std::memory_order::memory_order_relaxed);
#endif  // !defined(NDEBUG)
    static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                  "env_storage_ will not fit the Env");
    static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                  "env_storage_ does not meet the Env's alignment needs");
    new (&env_storage_) EnvType();
  }
  ~SingletonEnv() = default;

  SingletonEnv(const SingletonEnv&) = delete;
  SingletonEnv& operator=(const SingletonEnv&) = delete;

  Env* env() { return reinterpret_cast<Env*>(&env_storage_); }

  static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
    assert(!env_initialized_.load(std::memory_order::memory_order_relaxed));
#endif  // !defined(NDEBUG)
  }

 private:
  typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type
      env_storage_;
#if !defined(NDEBUG)
  static std::atomic<bool> env_initialized_;
#endif  // !defined(NDEBUG)
};

#if !defined(NDEBUG)
template <typename EnvType>
std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif  // !defined(NDEBUG)

using CustomDefaultEnv = SingletonEnv<CustomEnv>;
}  // namespace

Env* Env::Default() {
  static CustomDefaultEnv env_container;
  return env_container.env();
}

}  // namespace leveldb