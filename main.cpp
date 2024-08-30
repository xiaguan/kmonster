#include <cassert>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <optional>
#include <seastar/core/file-types.hh>
#include <unordered_map>

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_intent.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/log.hh>
#include <seastar/util/tmp_file.hh>
#include <sys/types.h>

#include "crc32.hpp"

using namespace seastar;
logger ksea_logger("ksea");

constexpr int64_t PAGE_SIZE = 4096;
constexpr int64_t PAGE_HEAD_SIZE = 64;
constexpr uint32_t PAGE_HEADER_MAGIC = 0x12345678;

constexpr int64_t PAGE_ENTRY_SIZE = (PAGE_SIZE - PAGE_HEAD_SIZE) / 4;
constexpr int64_t PAGE_ENTRY_HEADER_SIZE = 10;
constexpr int64_t PAGE_ENTRY_VALUE_SIZE =
    PAGE_ENTRY_SIZE - PAGE_ENTRY_HEADER_SIZE - 4;

class PageHeader {
public:
  PageHeader(uint32_t page_number)
      : magic_number(PAGE_HEADER_MAGIC), page_number(page_number) {
    std::memset(reserved, 0, sizeof(reserved));
    crc = crc32_fast(this, sizeof(PageHeader) - sizeof(crc));
  }

  uint32_t get_page_number() const { return page_number; }
  void set_page_number(uint32_t number) {
    page_number = number;
    update_crc();
  }

  bool validate_crc() const {
    uint32_t calculated_crc =
        crc32_fast(this, sizeof(PageHeader) - sizeof(crc));
    return calculated_crc == crc;
  }

private:
  void update_crc() {
    crc = crc32_fast(this, sizeof(PageHeader) - sizeof(crc));
  }

  uint32_t magic_number;
  uint32_t page_number;
  char reserved[50];
  uint32_t crc;
};

class PageEntry {
public:
  PageEntry() : key_size(0), value_size(0), crc(0) {
    std::memset(data, 0, sizeof(data));
    update_crc();
  }

  PageEntry(const std::string &key, const std::string &val)
      : key_size(key.size()), value_size(val.size()) {
    std::memset(data, 0, sizeof(data));
    std::memcpy(data, key.data(), key_size);
    std::memcpy(data + key_size, val.data(), value_size);
    update_crc();
  }

  bool validate_crc() const {
    return crc == crc32_fast(this, sizeof(PageEntry) - sizeof(crc));
  }

  std::string get_value() const {
    return std::string(data + key_size, value_size);
  }

  std::string get_key() const { return std::string(data, key_size); }

  void print_info() const {
    ksea_logger.info("Entry key:{} value:{}", get_key(), get_value());
  }

private:
  void update_crc() {
    this->crc = crc32_fast(this, sizeof(PageEntry) - sizeof(crc));
  }

  uint32_t key_size;
  uint32_t value_size;
  char data[PAGE_ENTRY_VALUE_SIZE];
  uint32_t crc;
};

class Page {
public:
  Page(uint32_t page_number) : header(page_number) {}

  PageHeader &get_header() { return header; }
  const PageHeader &get_header() const { return header; }

  bool add_entry(const std::string &key, const std::string &val, size_t index) {
    if (index >= 4)
      return false; // Ensure index is within bounds
    entries[index] = PageEntry(key, val);
    return true;
  }

  PageEntry &get_entry(size_t index) { return entries[index]; }
  const PageEntry &get_entry(size_t index) const { return entries[index]; }

  bool validate_crc() const {
    if (!header.validate_crc()) {
      ksea_logger.error("header crc validate failed");
      return false;
    }
    for (const auto &entry : entries) {
      if (!entry.validate_crc()) {
        ksea_logger.error("entry crc validate failed");
        return false;
      }
    }
    return true;
  }

  void print_info() const {
    for (const auto &entry : entries) {
      entry.print_info();
    }
  }

  uint32_t get_page_number() const { return this->header.get_page_number(); }

private:
  PageHeader header;
  PageEntry entries[4];
};
class Worker {
public:
  Worker() {
    ksea_logger.info("Worker created");
    ksea_logger.info("Page size:{}", sizeof(Page));
  }

  // 完成拷贝构造函数
  Worker(const Worker &other) {
    ksea_logger.info("Worker copy constructor");
    _file = other._file;
    _key_to_page_index = other._key_to_page_index;
    _page_number = other._page_number;
    _entry_index = other._entry_index;
  }
  // =default表示使用默认的拷贝构造函数
  Worker &operator=(const Worker &other) = default;

  future<> init() { _file = co_await _open_file(); }
  future<> close() { co_await _close_file(); }

  seastar::future<std::optional<std::string>> get(const std::string &key) {
    if (auto it = _key_to_page_index.find(key);
        it != _key_to_page_index.end()) {
      auto [page_number, entry_index] = it->second;
      Page *page = co_await _read_page_from_file(page_number);

      // if (!page->validate_crc()) {
      //   ksea_logger.error("Page validate CRC failed for page number");
      //   co_return std::nullopt;
      // }

      const PageEntry &entry = page->get_entry(entry_index);
      assert(key == entry.get_key());
      std::string return_value = entry.get_value();
      delete page;
      co_return return_value;
    } else {
      co_return std::nullopt;
    }
  }

  seastar::future<bool> add(const std::string &key, const std::string &value) {
    int64_t allocated_entry_index = co_await _allocate_new_page();
    if (allocated_entry_index == -1) {
      co_return false;
    }

    Page *page = co_await _read_page_from_file(_page_number);
    if (!page->add_entry(key, value, allocated_entry_index)) {
      co_return false;
    }

    co_await _write_page_to_file(page);

    ksea_logger.debug("Put key {} to page {} index {}", key, _page_number,
                      allocated_entry_index);
    _key_to_page_index[key] = {_page_number, allocated_entry_index};
    delete page;
    co_return true;
  }

private:
  future<file> _open_file() {
    co_return co_await open_file_dma("page_data.bin",
                                     open_flags::rw | open_flags::create);
  }

  future<> _close_file() { co_await _file.close(); }

  future<Page *> _read_page_from_file(int64_t page_number) {
    Page *page = new Page(page_number);
    ksea_logger.debug("pre read");
    auto bytes_read = co_await _file.dma_read(
        page_number * PAGE_SIZE, reinterpret_cast<char *>(page), PAGE_SIZE);
    if (bytes_read != sizeof(Page)) {
      ksea_logger.error("Error reading page from file");
      delete page;       // Clean up if reading fails
      co_return nullptr; // Return nullptr on error
    }
    co_return page;
  }

  future<> _write_page_to_file(const Page *page) {
    int64_t pos = page->get_page_number() * PAGE_SIZE;
    const char *buffer = reinterpret_cast<const char *>(page);
    size_t bytes_written = co_await _file.dma_write(pos, buffer, PAGE_SIZE);
    if (bytes_written != PAGE_SIZE) {
      ksea_logger.error("Error writing page to file");
    }
    ksea_logger.debug("Write pos:{} done", pos);
  }

  future<int64_t> _allocate_new_page() {
    if (_entry_index < 4) {
      auto result = _entry_index;
      _entry_index++;
      co_return result;
    } else {
      _page_number++;
      _entry_index = 1;

      Page page(_page_number);
      co_await _write_page_to_file(&page);

      co_return 0;
    }
  }

  seastar::file _file;
  std::unordered_map<std::string, std::pair<int64_t, int64_t>>
      _key_to_page_index;
  int64_t _page_number = -1;
  int64_t _entry_index = 4;
};

static_assert(sizeof(PageHeader) == 64, "Page Header size must be 100");
static_assert(sizeof(PageEntry) == PAGE_ENTRY_SIZE, "Page entry must");
static_assert(sizeof(Page) <= 4096, "Page size must be 4KB");

class BenchService {
public:
  BenchService(Worker &worker) : _worker(worker) {}

  seastar::future<int64_t> read_workload() {
    // pirnt thread info
    std::cout << "thread id: " << std::this_thread::get_id() << std::endl;
    int64_t query_count = 0; // Initialize query_count
    auto start_time = std::chrono::high_resolution_clock::now();
    // 生成一个vector<string>，里面存放1000个key，从0开始，到999
    std::vector<std::string> keys;
    for (int i = 0; i < 100000; ++i) {
      keys.push_back("key" + std::to_string(i));
    }
    // 创建vector<int>, 大小为1000
    std::vector<int> coroutines(100);
    while (true) {
      co_await coroutine::parallel_for_each(coroutines, [&](int i) -> future<> {
        // 从keys中随机选择一个key
        std::string key = keys[rand() % keys.size()];
        auto result = co_await _worker.get(key);
        assert(result.has_value());
        query_count++;
      });
      query_count++;

      auto current_time = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                          current_time - start_time)
                          .count();
      if (duration >= 20) {
        break;
      }
    }
    // 打印所用时间
    std::cout << "pass seconds: "
              << std::chrono::duration_cast<std::chrono::seconds>(
                     std::chrono::high_resolution_clock::now() - start_time)
                     .count()
              << std::endl;
    co_return query_count;
  }

private:
  Worker &_worker;
};

int main(int argc, char **argv) {
  seastar::app_template app;
  app.run(argc, argv, []() -> seastar::future<> {
    Worker worker;
    co_await worker.init();
    std::string value = "hello worrld";

    // Add 1000 key-value pairs
    for (int i = 0; i < 100000; ++i) {
      std::string key = "key" + std::to_string(i);
      auto add_done = co_await worker.add(key, value);
      assert(add_done);
    }

    // int64_t query_count = 0;
    // auto start_time = std::chrono::high_resolution_clock::now();
    // co_await coroutine::parallel_for_each(coroutines, [&](int i) -> future<>
    // {
    //   auto read_qps = co_await read_workload(worker, start_time);
    //   query_count += read_qps;
    // });

    // auto current_time = std::chrono::high_resolution_clock::now();
    // auto duration = std::chrono::duration_cast<std::chrono::seconds>(
    //                     current_time - start_time)
    //                     .count();
    // std::cout << "pass seconds: " << duration << std::endl;
    // std::cout << "Queries per second (QPS): " << query_count / 20 <<
    // std::endl;
    sharded<BenchService> bench;
    co_await bench.start(std::ref(worker));
    co_await bench.invoke_on(1, [](BenchService &bench) -> future<> {
      co_await bench.read_workload().then([](int64_t query_count) {
        std::cout << "Queries per second (QPS): " << query_count / 20
                  << std::endl;
      });
    });

    co_await worker.close();
  });
}
