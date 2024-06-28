#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <memory_resource>
#include <numeric>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

constexpr inline static uint32_t config_thread_count = 64;

class FileToMemory {
public:
  FileToMemory(const std::string &filename) : filename_(filename), data_(nullptr), size_(0) {}

  FileToMemory(const FileToMemory &) = delete;
  FileToMemory(FileToMemory &&other) noexcept
      : filename_(std::move(other.filename_)), data_(std::move(other.data_)), size_(other.size_) {
    other.data_ = nullptr;
    other.size_ = 0;
  }

  ~FileToMemory() {
    if (map_fd_ >= 0) {
      munmap(reinterpret_cast<void *>(const_cast<char *>(std::get<const char *>(data_))), size_);
      close(map_fd_);
      map_fd_ = -1;
    }
  }

public:
  FileToMemory &load_ram() {
    load_size_();

    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(size_);
    if (!buffer) {
      throw std::runtime_error("alloc memory failed");
    }

    std::ifstream ifs(filename_, std::ios::binary);
    if (!ifs) {
      throw std::runtime_error("open file failed");
    }
    if (!ifs.read(buffer.get(), size_)) {
      throw std::runtime_error("read file failed");
    }
    data_ = std::move(buffer);

    return *this;
  }

  FileToMemory &load_map() {
    load_size_();

    map_fd_ = open(filename_.c_str(), O_RDONLY);
    if (map_fd_ < 0) {
      throw std::runtime_error("open file failed");
    }

    const void *buffer = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, map_fd_, 0);
    if (buffer == MAP_FAILED) {
      throw std::runtime_error("mmap file failed");
    }
    data_ = static_cast<const char *>(buffer);

    return *this;
  }

  uint64_t size() const { return size_; }
  const char *data() const {
    return (std::holds_alternative<std::unique_ptr<char[]>>(data_)) ? std::get<std::unique_ptr<char[]>>(data_).get()
                                                                    : std::get<const char *>(data_);
  }

private:
  void load_size_() {
    std::ifstream ifs(filename_, std::ios::binary | std::ios::ate);
    if (!ifs) {
      throw std::runtime_error("open file failed");
    }
    size_ = ifs.tellg();
  }

private:
  std::string filename_{""};
  std::variant<std::unique_ptr<char[]>, const char *> data_;
  int32_t map_fd_{-1};
  uint64_t size_{0};
};

class OneBRC {
private:
  struct Item {
    int32_t min{std::numeric_limits<int32_t>::max()};
    int32_t max{std::numeric_limits<int32_t>::min()};
    int64_t sum{0};
    uint64_t count{0};
  };

  using ItemMap = std::unordered_map<std::string_view, Item>;
  using ItemMapOrder = std::map<ItemMap::key_type, ItemMap::mapped_type>;

private:
  static ItemMap run_thread_(const std::string_view &buffer) {
    ItemMap items(10000);

    int32_t value = 0;
    bool sign = false, access_number = false;
    for (auto start = buffer.begin(), prev = start, split = start, end = buffer.end(); start != end; start++) {
      if (const char c = *start; access_number && c != '\n') {
        if (c == '-') {
          sign = true;
        } else if (c != '.') {
          value = value * 10 + (c - '0');
        }
      } else if (c == ';') {
        split = start;
        value = 0;
        sign = false, access_number = true;
      } else if (c == '\n') {
        value = sign ? -value : value;
        const std::string_view name(prev, split - prev);
        auto &[min, max, sum, count] = items[name];
        min = std::min(min, value);
        max = std::max(max, value);
        sum += value;
        count++;
        prev = start + 1;
        access_number = false;
      }
    }

    return items;
  }

  static std::vector<std::string_view> buffer_split_(const std::string_view &buffer, uint32_t count) {
    const size_t step = buffer.size() / count;
    size_t start = 0, end = step;
    std::vector<std::string_view> buffers;
    for (uint32_t index = 0; index < count; index++, start = end + 1, end = std::min(start + step, buffer.size())) {
      while (end < buffer.size() && buffer[end] != '\n') {
        (index & 0x1) ? end-- : end++;
      }
      end = std::min(end, buffer.size() - 1);
      buffers.emplace_back(buffer.substr(start, end - start + 1));
    }
    return buffers;
  }

public:
  static ItemMapOrder run(const FileToMemory &io, uint32_t threads_count) {
    const uint32_t real_thread_count = (io.size() < 1024 * 1024) ? 1 : threads_count;
    const auto sub_bufs = buffer_split_(std::string_view(io.data(), io.size()), real_thread_count);
    assert(sub_bufs.size() == real_thread_count);

    std::vector<std::thread> threads;
    std::vector<ItemMap> threads_results(real_thread_count);
    std::transform(sub_bufs.begin(), sub_bufs.end(), threads_results.begin(), std::back_inserter(threads),
                   [](const auto &buf, auto &result) { return std::thread([&]() { result = run_thread_(buf); }); });
    std::for_each(threads.begin(), threads.end(), [](auto &thread) { thread.join(); });

    return std::accumulate(threads_results.begin(), threads_results.end(), ItemMapOrder(),
                           [&](auto &global_items, const auto &items) {
                             for (const auto &item : items) {
                               const auto &[name, value] = item;
                               using key_t = typename ItemMapOrder::key_type;
                               auto &[min, max, sum, count] = global_items[key_t(name)];
                               min = std::min(min, value.min);
                               max = std::max(max, value.max);
                               sum += value.sum;
                               count += value.count;
                             }
                             return global_items;
                           });
  }

  static void dump(const ItemMapOrder &items, const std::string &filename) {
    constexpr auto buffer_size = 10240 * 256; // enough for the case
    auto file_buffer = std::make_unique<char[]>(buffer_size);
    char *buffer_ptr = file_buffer.get();
    buffer_ptr += snprintf(buffer_ptr, buffer_size - (buffer_ptr - file_buffer.get()), "{");
    for (const auto &item : items) {
      const auto &[name, value] = item;
      const int32_t mean = std::round(static_cast<double>(value.sum) / value.count);
      buffer_ptr += snprintf(buffer_ptr, buffer_size - (buffer_ptr - file_buffer.get()), "%s=%.1f/%.1f/%.1f, ",
                             std::string(name).c_str(), value.min / 10.0, mean / 10.0, value.max / 10.0);
    }
    buffer_ptr -= 2; // remove last ', '
    buffer_ptr += snprintf(buffer_ptr, buffer_size - (buffer_ptr - file_buffer.get()), "}");
    *buffer_ptr = '\n'; // end of string

    // if exist output.txt, will be overwrite
    std::ofstream ofs(filename, std::ios::binary);
    if (!ofs) {
      throw std::runtime_error("open file failed");
    }
    ofs.write(file_buffer.get(), buffer_ptr - file_buffer.get() + 1);
  }
};

int32_t main(int32_t argc, char *argv[]) {
  const std::string_view filename = argc > 1 ? argv[1] : "/home/bing/Downloads/measurements.txt";
  auto start = std::chrono::high_resolution_clock::now();
  FileToMemory io = std::move(FileToMemory(std::string(filename)).load_map());
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> diff = end - start;
  std::cout << "Load time: " << diff.count() << " s\n";

  std::chrono::duration<double> diff_sum = std::chrono::duration<double>::zero(),
                                diff_max = std::chrono::duration<double>::min(),
                                diff_min = std::chrono::duration<double>::max();
  uint32_t diff_count = 0;
  const int32_t loop_max = 10;
  for (int32_t loop = 0; loop < loop_max; loop++) {
    start = std::chrono::high_resolution_clock::now();
    OneBRC::dump(OneBRC::run(io, std::min(std::thread::hardware_concurrency(), config_thread_count)), "output.txt");
    end = std::chrono::high_resolution_clock::now();
    diff = end - start;
    diff_sum += diff;
    diff_max = std::max(diff_max, diff);
    diff_min = std::min(diff_min, diff);
    diff_count++;
    std::cout << "Run time[" << loop << "]: " << diff.count() << " " << diff_min.count() << "/"
              << diff_sum.count() / diff_count << "/" << diff_max.count() << "s\n";
  }

  return 0;
}
