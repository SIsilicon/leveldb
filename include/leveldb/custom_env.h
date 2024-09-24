#ifndef STORAGE_LEVELDB_INCLUDE_CUSTOM_ENV_H_
#define STORAGE_LEVELDB_INCLUDE_CUSTOM_ENV_H_

#include "leveldb/env.h"

namespace leveldb {

struct SYSTEM_TIME {
  uint16_t year;
  uint16_t month;
  uint16_t dayOfWeek;
  uint16_t day;
  uint16_t hour;
  uint16_t minute;
  uint16_t second;
  uint16_t milliseconds;
};

enum FILE_SEEK_MODE { START, CURRENT, END };

enum FILE_ACCESS_FLAGS {
  READ = 0x01,        // open for reading only
  WRITE = 0x02,       // open for writing only
  READ_WRITE = 0x04,  // open for reading and writing
  APPEND = 0x08,      // writes done at eof

  CREATE = 0x10,  // create and open file
  TRUNC = 0x20,   // open and truncate
  EXCL = 0x40     // open only if file doesn't already exist
};

typedef int file_ref;

extern int (*openFile)(const std::string& filePath, int fileMode,
                       file_ref* ref);

extern int (*readFile)(const file_ref ref, char* buffer, size_t size,
                       size_t* received);

extern int (*writeFile)(const file_ref ref, const char* buffer, size_t size,
                        size_t* written);

extern int (*setFilePointer)(const file_ref ref, uint64_t offset,
                             uint64_t* new_offset, FILE_SEEK_MODE mode);

extern int (*closeFile)(const file_ref ref);

extern bool (*fileExists)(const std::string& filePath);

extern int (*getFileSize)(const std::string& filePath, uint64_t* size);

extern int (*renameFile)(const std::string& from, const std::string& to);

extern int (*deleteFile)(const std::string& filePath);

extern int (*createDir)(const std::string& dirPath);

extern int (*listDirChildren)(const std::string& dirPath,
                              std::vector<std::string>* result);

extern int (*getTempDir)(std::string* path);

extern int (*deleteDir)(const std::string& dirPath);

extern SYSTEM_TIME (*getLocalTime)();

extern uint64_t (*getTimeMillis)();

};  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_CUSTOM_ENV_H_