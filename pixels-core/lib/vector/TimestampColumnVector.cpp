//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"
#include <string>
#include <sstream>
#include <stdexcept>
#include <ctime>
#include <iomanip>

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    this->precision = precision;
    if(1) {
        posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                       len * sizeof(long));
    } else {
        this->times = nullptr;
    }
}


void TimestampColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
        if(encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::add(std::string &value) {
    // 移除字符串两端的空格
    value.erase(0, value.find_first_not_of(" \t\n\r"));
    value.erase(value.find_last_not_of(" \t\n\r") + 1);

    // 验证输入格式
    if (value.empty()) {
        throw std::invalid_argument("Timestamp string is empty.");
    }

    struct std::tm tm {};
    std::istringstream ss(value);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

    if (ss.fail()) {
        throw std::invalid_argument("Invalid timestamp format: " + value);
    }

    // 转换为 Unix 时间戳（秒）
    std::time_t ts = std::mktime(&tm);
    if (ts == -1) {
        throw std::runtime_error("Failed to convert timestamp: " + value);
    }

    // 添加到时间列中
    add(static_cast<int64_t>(ts));
}

void TimestampColumnVector::add(bool value) {
    // 将布尔值视为时间戳：false 对应 0 秒，true 对应 1 秒
    add(static_cast<int64_t>(value ? 1 : 0));
}

void TimestampColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }

    int index = writeIndex++;
    times[index] = value;

    isNull[index] = false;
}

void TimestampColumnVector::add(int value) {
    add(static_cast<int64_t>(value));
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    if (length < size) {
        long *oldTimes = times;
        posix_memalign(reinterpret_cast<void **>(&times), 64, size * sizeof(long));

        if (preserveData && oldTimes != nullptr) {
            std::copy(oldTimes, oldTimes + length, times);
        }

        if (oldTimes != nullptr) {
            free(oldTimes);
        }

        memoryUsage += (size - length) * sizeof(long);
        resize(size);
    }
}
void TimestampColumnVector::print(int rowCount)
{
    throw InvalidArgumentException("not support print longcolumnvector.");
//    for(int i = 0; i < rowCount; i++) {
//        std::cout<<longVector[i]<<std::endl;
//		std::cout<<intVector[i]<<std::endl;
//    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if(!closed) {
        TimestampColumnVector::close();
    }
}

void * TimestampColumnVector::current() {
    if(this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
}