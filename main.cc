#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <memory_resource>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

constexpr inline static uint32_t config_thread_count = 64;
constexpr inline static uint32_t config_switch_idx = 2;

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

    auto buffer = std::make_unique<char[]>(size_);
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

  template <typename Key, typename Value> class MapHelper {
    struct Node {
      Key key;
      Value value;
    };

  public:
    void reserve(size_t size) { nodes_.reserve(size); }

    auto find(const Key &key) {
      auto it = std::lower_bound(nodes_.begin(), nodes_.end(), key,
                                 [](const Node &lhs, const Key &rhs) { return lhs.key < rhs; });
      return (it != nodes_.end() && it->key == key) ? it : nodes_.end();
    }

    Value &operator[](const Key &key) {
      if (auto it = find(key); it != nodes_.end()) {
        assert(it->key == key);
        return it->value;
      } else {
        nodes_.push_back(Node{key, Value{}});
        std::sort(nodes_.begin(), nodes_.end(), [](const Node &lhs, const Node &rhs) { return lhs.key < rhs.key; });
        it = find(key);
        assert(it->key == key);
        return it->value;
      }
    }

    auto end() { return nodes_.end(); }
    auto begin() { return nodes_.begin(); }

  private:
    std::vector<Node> nodes_;
  };

  template <uint32_t idx, typename T, typename... Ts> struct Switch {
    using type = typename Switch<idx - 1, Ts...>::type;
  };
  template <typename T, typename... Ts> struct Switch<0, T, Ts...> { using type = T; };
  template <uint32_t idx, typename T, typename... Ts> using Switch_t = typename Switch<idx, T, Ts...>::type;

  // search is faster than hash ?
  // pmr stack buffer is faster than heaps
  constexpr inline static uint32_t switch_idx = config_switch_idx;
  using ItemMap = Switch_t<switch_idx,                                      // switch_idx
                           MapHelper<std::string_view, Item>,               // 0
                           std::unordered_map<std::string_view, Item>,      // 1
                           std::pmr::unordered_map<std::string_view, Item>, // 2
                           std::pmr::map<std::string_view, Item>            // 3
                           >;
  using ItemMapOrder = std::map<std::string_view, Item>;

public:
  OneBRC(const FileToMemory &io) : io_(io) {
    threads_count_ = std::min(std::thread::hardware_concurrency(), config_thread_count);
  }

public:
  template <uint32_t switch_idx_cp> void run_thread(size_t index, const char *buffer, size_t length) {
    auto &items = thread_items_[index];
    if constexpr (switch_idx_cp == 2 ||
                  switch_idx_cp == 3) { // constexpr if works as compile switcher only in template function
      std::array<std::byte, 10000 * 256> stack_buffer;
      std::pmr::monotonic_buffer_resource resource(stack_buffer.data(), stack_buffer.size());
      items = ItemMap(&resource);
    }
    items.reserve(10000);

    const char *prev_ptr = buffer, *split_ptr = nullptr, *end = buffer + length;
    int32_t value = 0;
    bool sign = false, access_number = false;
    for (const char *start = buffer; start < end; start++) {
      if (const char c = *(start); access_number && c != '\n') {
        if (c == '-') {
          sign = true;
        } else if (c != '.') {
          value = value * 10 + (c - '0');
        }
      } else if (c == ';') {
        split_ptr = (start);
        value = 0;
        sign = false, access_number = true;
      } else if (c == '\n') {
        value = sign ? -value : value;
        const std::string_view name(prev_ptr, split_ptr - prev_ptr);
        auto &[min, max, sum, count] = items[name];
        min = std::min(min, value);
        max = std::max(max, value);
        sum += value;
        count++;
        prev_ptr = (start) + 1;
        access_number = false;
      }
    }
  }

  size_t run() {
    const auto size = io_.size();
    const char *buffer = io_.data();
    const char *end = buffer + size;
    const uint32_t real_thread_count = (size < 1024 * 1024) ? 1 : threads_count_;

    threads_.reserve(real_thread_count);
    thread_items_.resize(real_thread_count);

    size_t step = size / real_thread_count;

    for (size_t i = 0; i < real_thread_count; ++i) {
      const char *next_buffer = std::min(buffer + step, end - 1);
      while (next_buffer < end && *next_buffer != '\n') {
        (i & 0x1) ? next_buffer-- : next_buffer++;
      }
      step = next_buffer - buffer;
      threads_.emplace_back(&OneBRC::run_thread<switch_idx>, this, i, buffer, step + 1);
      buffer = next_buffer + 1;
    }
    for (auto &t : threads_) {
      t.join();
    }

    for (auto &items : thread_items_) {
      for (const auto &item : items) {
        auto &[name, value] = item;
        auto &[min, max, sum, count] = items_[name];
        min = std::min(min, value.min);
        max = std::max(max, value.max);
        sum += value.sum;
        count += value.count;
      }
    }

    constexpr auto buffer_size = 10240 * 256;
    auto file_buffer = std::make_unique<char[]>(buffer_size); // enough
    char *buffer_ptr = file_buffer.get();
    buffer_ptr += snprintf(buffer_ptr, buffer_size - (buffer_ptr - file_buffer.get()), "{");
    for (const auto &item : items_) {
      const auto &[name, value] = item;
      const int32_t mean = std::round(static_cast<double>(value.sum) / value.count);
      buffer_ptr += snprintf(buffer_ptr, buffer_size - (buffer_ptr - file_buffer.get()), "%s=%.1f/%.1f/%.1f, ",
                             std::string(name).c_str(), value.min / 10.0, mean / 10.0, value.max / 10.0);
    }
    buffer_ptr -= 2; // remove last ', '
    buffer_ptr += snprintf(buffer_ptr, buffer_size - (buffer_ptr - file_buffer.get()), "}");
    *buffer_ptr = '\n'; // end of string

    // if exist output.txt, will be overwrite
    std::ofstream ofs("output.txt", std::ios::binary);
    if (!ofs) {
      throw std::runtime_error("open file failed");
    }
    ofs.write(file_buffer.get(), buffer_ptr - file_buffer.get() + 1);

    return items_.size();
  }

private:
  const FileToMemory &io_;
  ItemMapOrder items_;

  uint32_t threads_count_{0};
  std::vector<std::thread> threads_;
  std::vector<ItemMap> thread_items_;
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
  const int32_t loop_max = 1;
  for (int32_t loop = 0; loop < loop_max; loop++) {
    start = std::chrono::high_resolution_clock::now();
    const auto count = OneBRC(io).run();
    end = std::chrono::high_resolution_clock::now();
    diff = end - start;
    diff_sum += diff;
    diff_max = std::max(diff_max, diff);
    diff_min = std::min(diff_min, diff);
    diff_count++;
    std::cout << "Run time[" << loop << "]: " << diff.count() << " " << diff_min.count() << "/"
              << diff_sum.count() / diff_count << "/" << diff_max.count() << "s " << count << " items\n";
  }

  return 0;
}
