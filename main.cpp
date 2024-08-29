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

using namespace seastar;
logger ksea_logger("ksea");

constexpr int64_t PAGE_SIZE = 4096;
constexpr int64_t PAGE_HEAD_SIZE = 64;
constexpr uint32_t PAGE_HEADER_MAGIC = 0x12345678;

constexpr int64_t PAGE_ENTRY_SIZE = (PAGE_SIZE - PAGE_HEAD_SIZE) / 4;
constexpr int64_t PAGE_ENTRY_HEADER_SIZE = 10;
constexpr int64_t PAGE_ENTRY_VALUE_SIZE =
    PAGE_ENTRY_SIZE - PAGE_ENTRY_HEADER_SIZE - 4;

int32_t crc32_fast(const void *data, std::size_t length) {
  static const uint32_t crc_table[256] = {
      0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
      0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
      0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
      0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
      0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
      0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
      0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
      0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
      0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
      0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
      0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
      0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
      0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
      0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
      0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
      0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
      0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
      0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
      0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
      0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
      0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
      0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
      0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
      0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
      0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
      0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
      0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
      0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
      0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
      0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
      0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
      0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
      0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
      0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
      0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
      0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
      0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
      0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
      0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
      0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
      0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
      0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
      0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d};

  uint32_t crc = 0xFFFFFFFF;
  const uint8_t *current = static_cast<const uint8_t *>(data);

  while (length--) {
    crc = (crc >> 8) ^ crc_table[(crc ^ *current++) & 0xFF];
  }

  return crc ^ 0xFFFFFFFF;
}

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
  Worker() = default;
  future<> init() { _file = co_await _open_file(); }
  future<> close() { co_await _close_file(); }

  seastar::future<std::optional<std::string>> get(const std::string &key) {
    if (auto it = _key_to_page_index.find(key);
        it != _key_to_page_index.end()) {
      auto [page_number, entry_index] = it->second;
      Page page = co_await _read_page_from_file(page_number);

      if (!page.validate_crc()) {
        ksea_logger.error("Page validate CRC failed for page number");
        co_return std::nullopt;
      }

      const PageEntry &entry = page.get_entry(entry_index);
      assert(key == entry.get_key());
      co_return entry.get_value();
    } else {
      co_return std::nullopt;
    }
  }

  seastar::future<bool> add(const std::string &key, const std::string &value) {
    int64_t allocated_entry_index = co_await _allocate_new_page();
    if (allocated_entry_index == -1) {
      co_return false;
    }

    Page page = co_await _read_page_from_file(_page_number);
    if (!page.add_entry(key, value, allocated_entry_index)) {
      co_return false;
    }

    co_await _write_page_to_file(page);

    ksea_logger.debug("Put key {} to page {} index {}", key, _page_number,
                      allocated_entry_index);
    _key_to_page_index[key] = {_page_number, allocated_entry_index};
    co_return true;
  }

  seastar::future<bool> update(const std::string &key,
                               const std::string &new_value) {
    if (auto it = _key_to_page_index.find(key);
        it != _key_to_page_index.end()) {
      auto [page_number, entry_index] = it->second;
      Page page = co_await _read_page_from_file(page_number);

      if (!page.add_entry(key, new_value, entry_index)) {
        std::cerr << "Failed to update entry in page " << page_number
                  << std::endl;
        co_return false;
      }

      co_await _write_page_to_file(page);
      co_return true;
    } else {
      co_return false;
    }
  }

  seastar::future<bool> put(const std::string &key, const std::string &value) {
    co_return co_await add(key, value);
  }

private:
  future<file> _open_file() {
    co_return co_await open_file_dma("page_data.bin",
                                     open_flags::rw | open_flags::create);
  }

  future<> _close_file() { co_await _file.close(); }

  future<Page> _read_page_from_file(int64_t page_number) {
    Page page(page_number);
    ksea_logger.debug("pre read");
    auto bytes_read = co_await _file.dma_read(
        page_number * PAGE_SIZE, reinterpret_cast<char *>(&page), PAGE_SIZE);
    if (bytes_read != sizeof(Page)) {
      ksea_logger.error("Error reading page from file");
    }
    co_return page;
  }

  future<> _write_page_to_file(const Page &page) {
    int64_t pos = page.get_page_number() * PAGE_SIZE;
    const char *buffer = reinterpret_cast<const char *>(&page);
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
      co_await _write_page_to_file(page);

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

class KVService {
public:
  KVService(Worker worker) : _worker(worker) {}

  seastar::future<bool> put(const std::string &key, const std::string &value) {
    co_return co_await _worker.put(key, value);
  }

  seastar::future<std::optional<std::string>> get(const std::string &key) {
    co_return co_await _worker.get(key);
  }

private:
  Worker _worker;
};
seastar::future<int64_t> read_workload(
    Worker &worker,
    const std::chrono::high_resolution_clock::time_point &start_time) {
  int64_t query_count = 0; // Initialize query_count
  int random_key = rand() % 999 + 1;
  std::string key = "key" + std::to_string(random_key);
  while (true) {
    auto result = co_await worker.get(key);
    assert(result.has_value());
    query_count++;

    // Check time every 100 queries
    if (query_count % 100 == 0) {
      auto current_time = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                          current_time - start_time)
                          .count();
      if (duration >= 20) {
        break;
      }
    }
  }
  co_return query_count;
}

int main(int argc, char **argv) {
  seastar::app_template app;
  app.run(argc, argv, []() -> seastar::future<> {
    Worker worker;
    co_await worker.init();
    std::string value = "hello worrld";

    // Add 1000 key-value pairs
    for (int i = 0; i < 1000; ++i) {
      std::string key = "key" + std::to_string(i);
      auto add_done = co_await worker.add(key, value);
      assert(add_done);
    }

    std::vector<int> coroutines;
    for (int i = 0; i < 1000; ++i) {
      coroutines.push_back(i);
    }
    int64_t query_count = 0;
    auto start_time = std::chrono::high_resolution_clock::now();
    co_await coroutine::parallel_for_each(coroutines, [&](int i) -> future<> {
      auto read_qps = co_await read_workload(worker, start_time);
      query_count += read_qps;
    });

    auto current_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                        current_time - start_time)
                        .count();
    std::cout << "pass seconds: " << duration << std::endl;
    std::cout << "Queries per second (QPS): " << query_count / 20 << std::endl;
    co_await worker.close();
  });
}
