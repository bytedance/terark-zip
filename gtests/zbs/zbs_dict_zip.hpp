#pragma once
#include "zbs.hpp"

namespace terark {
class ZBSDictZip : ZBS {
 private:
  // for options
  DictZipBlobStore::Options dzopt;
  // random generator
  std::mt19937_64 randomGen;
  // high level api for reading
  std::unique_ptr<AbstractBlobStore> store;
  // dict zip builder
  std::unique_ptr<DictZipBlobStore::ZipBuilder> dzb;

  size_t sampleRatePercent = 3;
  // record size is identified during record adding
  size_t record_size = 0;

  /**
   * @param mmap_raw_file
   */
  void _buildDict(const MmapWholeFile &mmap_raw_file,
                  const ZBS_INPUT_FILE_TYPE file_type = LINE_BASED_RECORDS) {
    dzb.reset(DictZipBlobStore::createZipBuilder(dzopt));
    uint32_t sample_size = 0;
    auto t_start = std::chrono::high_resolution_clock::now();

    ZBS::_extractRecords(mmap_raw_file, file_type, [&](const fstring record) {
      if (randomGen() % 100 <= sampleRatePercent) {
        dzb->addSample(record);
        sample_size++;
      }
      record_size++;
    });

    dzb->finishSample();

    auto t_end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(t_end - t_start);
    std::cout << "[" << duration.count()
              << " seconds] Sample Size = " << sample_size
              << ", Record Size = " << record_size << std::endl;
  }

  void _addRecords(const MmapWholeFile &mmap_raw_file,
                   const ZBS_INPUT_FILE_TYPE file_type = LINE_BASED_RECORDS) {
    // extract all records from mmap file
    ZBS::_extractRecords(mmap_raw_file, file_type,
                         [&](const fstring record) { dzb->addRecord(record); });
  }

 public:
  /**
   * @param sampleRate 3 means 0.03
   * @param enable_entropy use entropy compression after dict zip.
   */
  explicit ZBSDictZip(uint32_t sampleRate, bool enable_entropy = false) {
    sampleRatePercent = sampleRate;
    dzopt.embeddedDict = true;
    dzopt.compressGlobalDict = true;
    if (enable_entropy) {
      dzopt.entropyAlgo = DictZipBlobStore::Options::kHuffmanO1;
    }
  }

  ~ZBSDictZip() = default;

  /**
   * Load compressed zbs file before get(idx).
   * @param fname
   */
  void load_zbs(const fstring &fname) {
    store.reset(AbstractBlobStore::load_from_mmap(fname, false));
    // if you prepare the zbs file from a non-zero position, you will also have
    // to fix the position before reading. dzb->prepare(record_cnt, fname,
    // position);
  }

  /**
   * The compressed records' length are not fixed, we can only get it by its
   * order idx.
   *
   * ZBS file has an offset table inside it and the offset table is compressed
   * by an delta-encoding-like algorithm.
   * @param idx
   */
  void get(int idx, valvec<byte_t> *record) { store->get_record(idx, record); }

  /**
   * @param raw_fname original file need to be compressed
   * @param zbs_file output compressed file
   *
   * @return record size
   */
  int compress(const fstring &raw_fname, const fstring &zbs_file,
               ZBS_INPUT_FILE_TYPE file_type = LINE_BASED_RECORDS) {
    MmapWholeFile fmmap(raw_fname);

    // 1. build dict
    _buildDict(fmmap, file_type);

    // 2. prepare output zbs file
    dzb->prepare(record_size, zbs_file);

    // 3. add records
    _addRecords(fmmap, file_type);

    // 4. finish compression, free dict in memory & write output file
    dzb->finish(DictZipBlobStore::ZipBuilder::FinishFreeDict |
                DictZipBlobStore::ZipBuilder::FinishWriteDictFile);

    return record_size;
  }

  /**
   * compress with customize input file parser.
   * @param raw_fname
   * @param zbs_file
   * @param F std::function<void(const MmapWholeFile &,
   *                             const std::function<void(const fstring)> &)>
   *
   * @return
   */
  template <class F>
  int compress(const fstring &raw_fname, const fstring &zbs_file, F &&parser) {
    MmapWholeFile fmmap(raw_fname);

    // 1. build dict
    dzb.reset(DictZipBlobStore::createZipBuilder(dzopt));
    uint32_t sample_size = 0;
    auto t_start = std::chrono::high_resolution_clock::now();

    parser(fmmap, [&](const fstring record) {
      if (randomGen() % 100 <= sampleRatePercent) {
        dzb->addSample(record);
        sample_size++;
      }
      record_size++;
    });

    dzb->finishSample();

    auto t_end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(t_end - t_start);
    std::cout << "[" << duration.count()
              << " seconds] Sample Size = " << sample_size
              << ", Record Size = " << record_size << std::endl;

    // 2. prepare output zbs file
    dzb->prepare(record_size, zbs_file);

    // 3. add records
    // extract all records from mmap file
    parser(fmmap, [&](const fstring record) { dzb->addRecord(record); });

    // 4. finish compression, free dict in memory & write output file
    dzb->finish(DictZipBlobStore::ZipBuilder::FinishFreeDict |
                DictZipBlobStore::ZipBuilder::FinishWriteDictFile);

    return record_size;
  }
};
}  // namespace terark
