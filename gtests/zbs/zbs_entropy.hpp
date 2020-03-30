#pragma once

#include "zbs.hpp"

namespace terark {

// TODO
class ZBSEntropy : ZBS {
 private:
  // for options
  DictZipBlobStore::Options dzopt;
  // high level api for reading
  std::unique_ptr<AbstractBlobStore> store;

  // record size is identified during record adding
  size_t record_size = 0;

  // if entroy compression is enabled, we have to stats records distribution on
  // first pass.
  bool entropyCompression = false;
  std::unique_ptr<freq_hist_o1> freq;
  std::unique_ptr<EntropyZipBlobStore::MyBuilder> ezbuilder;

  void _addRecords(const MmapWholeFile &mmap_raw_file,
                   const ZBS_INPUT_FILE_TYPE file_type = LINE_BASED_RECORDS) {
    ZBS::_extractRecords(mmap_raw_file, file_type, [&](const fstring record) {
      ezbuilder->addRecord(record);
    });
  }

  /**
   * Build compression dict.
   *      If you are using entropy compression, then this method only collect
   * stats information.
   * @param mmap_raw_file
   */
  void _buildDict(const MmapWholeFile &mmap_raw_file,
                  const ZBS_INPUT_FILE_TYPE file_type = LINE_BASED_RECORDS) {
    freq = std::make_unique<freq_hist_o1>();
    uint32_t sample_size = 0;
    auto t_start = std::chrono::high_resolution_clock::now();

    ZBS::_extractRecords(mmap_raw_file, file_type, [&](const fstring record) {
      // entropy needs all records for sample stats
      freq->add_record(record);
    });

    freq->finish();

    auto t_end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(t_end - t_start);
    std::cout << "[" << duration.count()
              << " seconds], Record Size = " << record_size << std::endl;
  }

 public:
  ZBSEntropy() {
    dzopt.embeddedDict = true;
    std::cout << "entropy compression is enabled " << std::endl;
    dzopt.offsetArrayBlockUnits = 128;
    dzopt.compressGlobalDict = true;
  }

  ~ZBSEntropy() {}

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
   * @param is_binary if the input data is binary, the input format shoud be :
   * [length, data, length, data...], and the length should be unit32_t.
   *
   * @return record size
   */
  int compress(const fstring &raw_fname, const fstring &zbs_file,
               const ZBS_INPUT_FILE_TYPE file_type = LINE_BASED_RECORDS) {
    MmapWholeFile fmmap(raw_fname);

    // 1. build dict
    _buildDict(fmmap, file_type);

    // 2. prepare output zbs file
    ezbuilder = std::make_unique<EntropyZipBlobStore::MyBuilder>(
        *freq, dzopt.offsetArrayBlockUnits, zbs_file);

    // 3. add records
    _addRecords(fmmap, file_type);

    // 4. finish compression, free dict in memory & write output file
    ezbuilder->finish();

    return record_size;
  }
};
}  // namespace terark